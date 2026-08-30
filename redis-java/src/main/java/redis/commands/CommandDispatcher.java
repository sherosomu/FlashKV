package redis.commands;

import redis.core.RedisDatabase;
import redis.core.RedisValue;
import redis.core.StreamId;
import redis.protocol.RespWriter;
import redis.replication.ReplicationState;
import redis.server.ClientContext;
import redis.server.ServerContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Dispatches parsed RESP commands to their implementation. A single static
 * entry point, {@link #dispatch}, is used both for normal client connections
 * and (with fromMaster=true) for commands a replica applies from its master's
 * replication stream.
 */
public class CommandDispatcher {

    private static final Set<String> SUBSCRIBE_MODE_ALLOWED = Set.of(
            "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT", "RESET"
    );

    private static final Set<String> WRITE_COMMANDS = Set.of(
            "SET", "DEL", "RPUSH", "LPUSH", "LPOP", "RPOP", "INCR", "EXPIRE", "PERSIST", "XADD"
    );

    private static final Set<String> KNOWN_COMMANDS = Set.of(
            "PING", "ECHO", "SET", "GET", "DEL", "EXISTS", "TYPE", "KEYS", "INCR",
            "CONFIG", "INFO", "REPLCONF", "PSYNC", "WAIT", "MULTI", "EXEC", "DISCARD",
            "SUBSCRIBE", "UNSUBSCRIBE", "PUBLISH", "RPUSH", "LPUSH", "LRANGE", "LLEN",
            "LPOP", "RPOP", "BLPOP", "XADD", "XRANGE", "XREAD", "EXPIRE", "TTL", "PTTL",
            "PERSIST", "OBJECT", "COMMAND", "CLIENT", "SELECT", "FLUSHALL", "QUIT"
    );

    /**
     * Entry point. Handles MULTI-queueing and subscribe-mode restrictions,
     * then routes to {@link #executeCommand}, writing the reply to
     * ctx.writer (the client's real socket writer).
     */
    public static void dispatch(String[] args, ClientContext ctx, ServerContext srv, boolean fromMaster) throws IOException {
        if (args.length == 0) {
            return;
        }
        String cmd = args[0].toUpperCase();

        if (ctx.isSubscribedMode() && !fromMaster && !SUBSCRIBE_MODE_ALLOWED.contains(cmd)) {
            ctx.writer.writeError("ERR Can't execute '" + args[0].toLowerCase()
                    + "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context");
            return;
        }

        if (ctx.inTransaction && !fromMaster
                && !cmd.equals("EXEC") && !cmd.equals("DISCARD") && !cmd.equals("MULTI")) {
            if (!KNOWN_COMMANDS.contains(cmd)) {
                ctx.transactionDirty = true;
                ctx.writer.writeError("ERR unknown command '" + args[0] + "'");
                return;
            }
            ctx.queuedCommands.add(args);
            ctx.writer.writeSimpleString("QUEUED");
            return;
        }

        executeCommand(cmd, args, ctx, ctx.writer, srv, fromMaster);
    }

    /**
     * Executes a single command, writing its reply to {@code out}. Used
     * directly (bypassing MULTI-queueing) both for top-level dispatch and for
     * replaying queued commands during EXEC.
     */
    private static void executeCommand(String cmd, String[] args, ClientContext ctx, RespWriter out,
                                        ServerContext srv, boolean fromMaster) throws IOException {
        RedisDatabase db = srv.database;
        try {
            switch (cmd) {
                case "PING" -> {
                    if (!fromMaster) out.writeSimpleString("PONG");
                }
                case "ECHO" -> out.writeBulkString(args.length > 1 ? args[1] : "");
                case "QUIT" -> out.writeSimpleString("OK");

                case "SET" -> handleSet(args, out, db, srv, fromMaster);
                case "GET" -> {
                    String v = db.getString(args[1]);
                    if (v == null) out.writeNullBulkString(); else out.writeBulkString(v);
                }
                case "DEL" -> {
                    int count = 0;
                    for (int i = 1; i < args.length; i++) {
                        if (db.del(args[i])) count++;
                    }
                    out.writeInteger(count);
                    maybePropagate(cmd, args, srv, fromMaster);
                }
                case "EXISTS" -> {
                    int count = 0;
                    for (int i = 1; i < args.length; i++) {
                        if (db.exists(args[i])) count++;
                    }
                    out.writeInteger(count);
                }
                case "TYPE" -> {
                    RedisValue v = db.get(args[1]);
                    out.writeSimpleString(v == null ? "none" : v.typeName());
                }
                case "KEYS" -> {
                    String pattern = args.length > 1 ? args[1] : "*";
                    Pattern regex = globToRegex(pattern);
                    List<String> matches = new ArrayList<>();
                    for (String k : db.keys()) {
                        if (regex.matcher(k).matches()) matches.add(k);
                    }
                    out.writeStringArray(matches);
                }
                case "INCR" -> handleIncr(args, out, db, srv, fromMaster);

                case "CONFIG" -> handleConfig(args, out, srv);
                case "INFO" -> handleInfo(args, out, srv);

                case "REPLCONF" -> handleReplconf(args, ctx, out, fromMaster);
                case "PSYNC" -> {
                    // Handled specially by ClientHandler before reaching the dispatcher.
                    out.writeError("ERR PSYNC must be handled by the connection layer");
                }
                case "WAIT" -> {
                    int numReplicas = Integer.parseInt(args[1]);
                    long timeoutMs = Long.parseLong(args[2]);
                    int acked = srv.replicationState.waitForAcks(numReplicas, timeoutMs);
                    out.writeInteger(acked);
                }

                case "MULTI" -> {
                    if (ctx.inTransaction) {
                        out.writeError("ERR MULTI calls can not be nested");
                    } else {
                        ctx.inTransaction = true;
                        ctx.transactionDirty = false;
                        ctx.queuedCommands.clear();
                        out.writeSimpleString("OK");
                    }
                }
                case "DISCARD" -> {
                    if (!ctx.inTransaction) {
                        out.writeError("ERR DISCARD without MULTI");
                    } else {
                        ctx.inTransaction = false;
                        ctx.queuedCommands.clear();
                        out.writeSimpleString("OK");
                    }
                }
                case "EXEC" -> handleExec(ctx, out, srv, fromMaster);

                case "SUBSCRIBE" -> handleSubscribe(args, ctx, out, srv);
                case "UNSUBSCRIBE" -> handleUnsubscribe(args, ctx, out, srv);
                case "PUBLISH" -> {
                    int count = srv.pubSubManager.publish(args[1], args[2]);
                    out.writeInteger(count);
                }

                case "RPUSH", "LPUSH" -> handlePush(cmd, args, out, db, srv, fromMaster);
                case "LRANGE" -> handleLrange(args, out, db);
                case "LLEN" -> {
                    RedisValue.ListValue lv = db.getListOrNull(args[1]);
                    out.writeInteger(lv == null ? 0 : lv.items.size());
                }
                case "LPOP", "RPOP" -> handlePop(cmd, args, out, db, srv, fromMaster);
                case "BLPOP" -> handleBlpop(args, ctx, out, db, srv);

                case "XADD" -> handleXadd(args, out, db, srv, fromMaster);
                case "XRANGE" -> handleXrange(args, out, db);
                case "XREAD" -> handleXread(args, out, db);

                case "EXPIRE" -> {
                    long seconds = Long.parseLong(args[2]);
                    boolean ok = db.expire(args[1], System.currentTimeMillis() + seconds * 1000L);
                    out.writeInteger(ok ? 1 : 0);
                    if (ok) maybePropagate(cmd, args, srv, fromMaster);
                }
                case "TTL" -> {
                    long pttl = db.pttl(args[1]);
                    out.writeInteger(pttl < 0 ? pttl : (pttl + 999) / 1000);
                }
                case "PTTL" -> out.writeInteger(db.pttl(args[1]));
                case "PERSIST" -> {
                    boolean ok = db.persist(args[1]);
                    out.writeInteger(ok ? 1 : 0);
                    if (ok) maybePropagate(cmd, args, srv, fromMaster);
                }

                case "OBJECT" -> handleObject(args, out, db);
                case "COMMAND" -> out.writeEmptyArray();
                case "CLIENT" -> out.writeSimpleString("OK");
                case "SELECT" -> out.writeSimpleString("OK");
                case "FLUSHALL" -> {
                    db.keys().forEach(db::del);
                    out.writeSimpleString("OK");
                }

                default -> out.writeError("ERR unknown command '" + args[0] + "'");
            }
        } catch (RedisDatabase.WrongTypeException wte) {
            out.writeError(wte.getMessage());
        } catch (NumberFormatException nfe) {
            out.writeError("ERR value is not an integer or out of range");
        } catch (ArrayIndexOutOfBoundsException aioobe) {
            out.writeError("ERR wrong number of arguments for '" + args[0].toLowerCase() + "' command");
        }
    }

    // ---------------------------------------------------------------------
    // SET / GET / INCR
    // ---------------------------------------------------------------------

    private static void handleSet(String[] args, RespWriter out, RedisDatabase db, ServerContext srv, boolean fromMaster) throws IOException {
        String key = args[1];
        String value = args[2];
        long expireAt = -1;
        boolean nx = false, xx = false, getFlag = false;

        for (int i = 3; i < args.length; i++) {
            String opt = args[i].toUpperCase();
            switch (opt) {
                case "PX" -> expireAt = System.currentTimeMillis() + Long.parseLong(args[++i]);
                case "EX" -> expireAt = System.currentTimeMillis() + Long.parseLong(args[++i]) * 1000L;
                case "NX" -> nx = true;
                case "XX" -> xx = true;
                case "GET" -> getFlag = true;
                default -> { /* ignore unsupported option */ }
            }
        }

        String previous = db.getString(key);
        boolean exists = db.exists(key);
        if ((nx && exists) || (xx && !exists)) {
            if (getFlag) {
                if (previous == null) out.writeNullBulkString(); else out.writeBulkString(previous);
            } else {
                out.writeNullBulkString();
            }
            return;
        }

        db.setString(key, value, expireAt);
        maybePropagate("SET", args, srv, fromMaster);

        if (getFlag) {
            if (previous == null) out.writeNullBulkString(); else out.writeBulkString(previous);
        } else {
            out.writeSimpleString("OK");
        }
    }

    private static void handleIncr(String[] args, RespWriter out, RedisDatabase db, ServerContext srv, boolean fromMaster) throws IOException {
        String key = args[1];
        String current = db.getString(key);
        long value;
        if (current == null) {
            value = 1;
        } else {
            try {
                value = Long.parseLong(current) + 1;
            } catch (NumberFormatException e) {
                out.writeError("ERR value is not an integer or out of range");
                return;
            }
        }
        db.setString(key, Long.toString(value), -1);
        out.writeInteger(value);
        maybePropagate("INCR", args, srv, fromMaster);
    }

    // ---------------------------------------------------------------------
    // CONFIG / INFO
    // ---------------------------------------------------------------------

    private static void handleConfig(String[] args, RespWriter out, ServerContext srv) throws IOException {
        String sub = args[1].toUpperCase();
        if (sub.equals("GET")) {
            String param = args[2].toLowerCase();
            List<String> reply = new ArrayList<>();
            if (param.equals("dir")) {
                reply.add("dir");
                reply.add(srv.config.getDir());
            } else if (param.equals("dbfilename")) {
                reply.add("dbfilename");
                reply.add(srv.config.getDbfilename());
            }
            out.writeStringArray(reply);
        } else {
            out.writeSimpleString("OK");
        }
    }

    private static void handleInfo(String[] args, RespWriter out, ServerContext srv) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("# Replication\r\n");
        boolean isReplica = srv.config.isReplica();
        sb.append("role:").append(isReplica ? "slave" : "master").append("\r\n");
        if (isReplica) {
            sb.append("master_host:").append(srv.config.getMasterHost()).append("\r\n");
            sb.append("master_port:").append(srv.config.getMasterPort()).append("\r\n");
            sb.append("master_link_status:up\r\n");
        }
        sb.append("connected_slaves:").append(srv.replicationState.replicaCount()).append("\r\n");
        sb.append("master_replid:").append(srv.config.getReplicationId()).append("\r\n");
        sb.append("master_repl_offset:").append(srv.config.getMasterReplOffset()).append("\r\n");
        out.writeBulkString(sb.toString());
    }

    // ---------------------------------------------------------------------
    // Replication plumbing
    // ---------------------------------------------------------------------

    private static void handleReplconf(String[] args, ClientContext ctx, RespWriter out, boolean fromMaster) throws IOException {
        if (args.length < 2) {
            out.writeSimpleString("OK");
            return;
        }
        String sub = args[1].toUpperCase();
        switch (sub) {
            case "GETACK" -> {
                // We (a replica) were asked by our master to report our offset.
                // The caller (ReplicaClient) supplies the correct offset by
                // writing directly; this generic path is a fallback no-op.
            }
            case "ACK" -> {
                if (ctx != null && ctx.replicaConnection != null && args.length > 2) {
                    try {
                        ctx.replicaConnection.ackOffset = Long.parseLong(args[2]);
                    } catch (NumberFormatException ignored) {
                    }
                }
                // No reply is sent for ACK.
            }
            default -> {
                if (!fromMaster) out.writeSimpleString("OK");
            }
        }
    }

    static void maybePropagate(String cmd, String[] args, ServerContext srv, boolean fromMaster) {
        if (fromMaster) return; // never re-propagate what our own master sent us
        if (srv.config.isReplica()) return; // only a master propagates to its replicas
        if (!WRITE_COMMANDS.contains(cmd)) return;
        srv.replicationState.propagate(RespWriter.encodeCommandArray(args));
    }

    // ---------------------------------------------------------------------
    // Transactions
    // ---------------------------------------------------------------------

    private static void handleExec(ClientContext ctx, RespWriter out, ServerContext srv, boolean fromMaster) throws IOException {
        if (!ctx.inTransaction) {
            out.writeError("ERR EXEC without MULTI");
            return;
        }
        ctx.inTransaction = false;
        if (ctx.transactionDirty) {
            ctx.queuedCommands.clear();
            out.writeError("EXECABORT Transaction discarded because of previous errors.");
            return;
        }
        List<String[]> queued = new ArrayList<>(ctx.queuedCommands);
        ctx.queuedCommands.clear();

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        RespWriter capture = new RespWriter(buffer);
        for (String[] queuedArgs : queued) {
            executeCommand(queuedArgs[0].toUpperCase(), queuedArgs, ctx, capture, srv, fromMaster);
        }
        out.writeRaw(("*" + queued.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.writeRaw(buffer.toByteArray());
    }

    // ---------------------------------------------------------------------
    // Pub/Sub
    // ---------------------------------------------------------------------

    private static void handleSubscribe(String[] args, ClientContext ctx, RespWriter out, ServerContext srv) throws IOException {
        for (int i = 1; i < args.length; i++) {
            String channel = args[i];
            ctx.subscribedChannels.add(channel);
            srv.pubSubManager.subscribe(channel, ctx);
            List<String> reply = List.of("subscribe", channel, String.valueOf(ctx.subscribedChannels.size()));
            out.writeRaw(buildMixedArray(reply.get(0), reply.get(1), Long.parseLong(reply.get(2))));
        }
    }

    private static void handleUnsubscribe(String[] args, ClientContext ctx, RespWriter out, ServerContext srv) throws IOException {
        List<String> channels = new ArrayList<>();
        if (args.length > 1) {
            for (int i = 1; i < args.length; i++) channels.add(args[i]);
        } else {
            channels.addAll(ctx.subscribedChannels);
        }
        if (channels.isEmpty()) {
            out.writeRaw(buildMixedArrayNullChannel());
            return;
        }
        for (String channel : channels) {
            ctx.subscribedChannels.remove(channel);
            srv.pubSubManager.unsubscribe(channel, ctx);
            out.writeRaw(buildMixedArray("unsubscribe", channel, ctx.subscribedChannels.size()));
        }
    }

    private static byte[] buildMixedArray(String word, String channel, long count) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        writeQuiet(b, "*3\r\n");
        writeQuiet(b, RespWriter.encodeBulkString(word));
        writeQuiet(b, RespWriter.encodeBulkString(channel));
        writeQuiet(b, RespWriter.encodeInteger(count));
        return b.toByteArray();
    }

    private static byte[] buildMixedArrayNullChannel() {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        writeQuiet(b, "*3\r\n");
        writeQuiet(b, RespWriter.encodeBulkString("unsubscribe"));
        writeQuiet(b, "$-1\r\n");
        writeQuiet(b, RespWriter.encodeInteger(0));
        return b.toByteArray();
    }

    // ---------------------------------------------------------------------
    // Lists
    // ---------------------------------------------------------------------

    private static void handlePush(String cmd, String[] args, RespWriter out, RedisDatabase db, ServerContext srv, boolean fromMaster) throws IOException {
        RedisValue.ListValue list = db.getOrCreateList(args[1]);
        synchronized (db.getWaitLock()) {
            for (int i = 2; i < args.length; i++) {
                if (cmd.equals("RPUSH")) list.items.addLast(args[i]);
                else list.items.addFirst(args[i]);
            }
            db.getWaitLock().notifyAll();
        }
        out.writeInteger(list.items.size());
        maybePropagate(cmd, args, srv, fromMaster);
    }

    private static void handleLrange(String[] args, RespWriter out, RedisDatabase db) throws IOException {
        RedisValue.ListValue lv = db.getListOrNull(args[1]);
        if (lv == null) {
            out.writeEmptyArray();
            return;
        }
        int size = lv.items.size();
        int start = normalizeIndex(Integer.parseInt(args[2]), size);
        int stop = normalizeIndex(Integer.parseInt(args[3]), size);
        start = Math.max(start, 0);
        stop = Math.min(stop, size - 1);
        if (start > stop || size == 0) {
            out.writeEmptyArray();
            return;
        }
        List<String> result = new ArrayList<>();
        int idx = 0;
        for (String item : lv.items) {
            if (idx >= start && idx <= stop) result.add(item);
            idx++;
        }
        out.writeStringArray(result);
    }

    private static int normalizeIndex(int idx, int size) {
        return idx < 0 ? Math.max(size + idx, 0) : idx;
    }

    private static void handlePop(String cmd, String[] args, RespWriter out, RedisDatabase db, ServerContext srv, boolean fromMaster) throws IOException {
        RedisValue.ListValue lv = db.getListOrNull(args[1]);
        Integer count = args.length > 2 ? Integer.parseInt(args[2]) : null;

        if (lv == null || lv.items.isEmpty()) {
            if (count == null) out.writeNullBulkString(); else out.writeNullArray();
            return;
        }
        if (count == null) {
            String val = cmd.equals("LPOP") ? lv.items.removeFirst() : lv.items.removeLast();
            db.removeIfEmptyList(args[1]);
            out.writeBulkString(val);
            maybePropagate(cmd, args, srv, fromMaster);
        } else {
            List<String> result = new ArrayList<>();
            int n = Math.min(count, lv.items.size());
            for (int i = 0; i < n; i++) {
                result.add(cmd.equals("LPOP") ? lv.items.removeFirst() : lv.items.removeLast());
            }
            db.removeIfEmptyList(args[1]);
            out.writeStringArray(result);
            maybePropagate(cmd, args, srv, fromMaster);
        }
    }

    private static void handleBlpop(String[] args, ClientContext ctx, RespWriter out, RedisDatabase db, ServerContext srv) throws IOException {
        List<String> keys = new ArrayList<>();
        for (int i = 1; i < args.length - 1; i++) keys.add(args[i]);
        double timeoutSec = Double.parseDouble(args[args.length - 1]);
        long timeoutMs = (long) (timeoutSec * 1000);
        long deadline = timeoutMs > 0 ? System.currentTimeMillis() + timeoutMs : Long.MAX_VALUE;

        Object lock = db.getWaitLock();
        synchronized (lock) {
            while (true) {
                for (String key : keys) {
                    RedisValue.ListValue lv = db.getListOrNull(key);
                    if (lv != null && !lv.items.isEmpty()) {
                        String val = lv.items.removeFirst();
                        db.removeIfEmptyList(key);
                        out.writeStringArray(List.of(key, val));
                        maybePropagate("LPOP", new String[]{"LPOP", key}, srv, false);
                        return;
                    }
                }
                long remaining = deadline - System.currentTimeMillis();
                if (timeoutMs > 0 && remaining <= 0) {
                    out.writeNullArray();
                    return;
                }
                try {
                    lock.wait(timeoutMs > 0 ? remaining : 100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    out.writeNullArray();
                    return;
                }
            }
        }
    }

    // ---------------------------------------------------------------------
    // Streams
    // ---------------------------------------------------------------------

    private static void handleXadd(String[] args, RespWriter out, RedisDatabase db, ServerContext srv, boolean fromMaster) throws IOException {
        String key = args[1];
        String idSpec = args[2];
        RedisValue.StreamValue stream = db.getOrCreateStream(key);

        StreamId newId;
        synchronized (db.getWaitLock()) {
            newId = resolveStreamId(idSpec, stream);
            if (newId == null) {
                out.writeError("ERR The ID specified in XADD must be greater than 0-0");
                return;
            }
            if (newId.compareTo(stream.lastId) <= 0 && !(stream.entries.isEmpty() && newId.compareTo(StreamId.MIN) > 0)) {
                if (newId.compareTo(stream.lastId) <= 0) {
                    out.writeError("ERR The ID specified in XADD is equal or smaller than the target stream top item");
                    return;
                }
            }
            LinkedHashMap<String, String> fields = new LinkedHashMap<>();
            for (int i = 3; i + 1 < args.length; i += 2) {
                fields.put(args[i], args[i + 1]);
            }
            stream.entries.put(newId, fields);
            stream.lastId = newId;
            db.getWaitLock().notifyAll();
        }
        out.writeBulkString(newId.toString());
        maybePropagate("XADD", args, srv, fromMaster);
    }

    /** Resolves "*", "ms-*", or an explicit "ms-seq" id against the current stream state. Returns null if invalid (<=0-0). */
    private static StreamId resolveStreamId(String idSpec, RedisValue.StreamValue stream) {
        StreamId candidate;
        if (idSpec.equals("*")) {
            long ms = System.currentTimeMillis();
            long seq = (stream.lastId.ms == ms) ? stream.lastId.seq + 1 : 0;
            candidate = new StreamId(ms, seq);
        } else if (idSpec.endsWith("-*")) {
            long ms = Long.parseLong(idSpec.substring(0, idSpec.indexOf('-')));
            long seq = (stream.lastId.ms == ms) ? stream.lastId.seq + 1 : 0;
            candidate = new StreamId(ms, seq);
        } else {
            candidate = StreamId.parseExplicit(idSpec);
        }
        if (candidate.compareTo(StreamId.MIN) <= 0) {
            return null;
        }
        return candidate;
    }

    private static void handleXrange(String[] args, RespWriter out, RedisDatabase db) throws IOException {
        RedisValue.StreamValue stream = db.getStreamOrNull(args[1]);
        StreamId start = StreamId.parseRangeStart(args[2]);
        StreamId end = StreamId.parseRangeEnd(args[3]);
        Integer count = null;
        if (args.length > 5 && args[4].equalsIgnoreCase("COUNT")) {
            count = Integer.parseInt(args[5]);
        }
        List<byte[]> entries = new ArrayList<>();
        if (stream != null) {
            for (Map.Entry<StreamId, LinkedHashMap<String, String>> e : stream.entries.subMap(start, true, end, true).entrySet()) {
                entries.add(encodeStreamEntry(e.getKey(), e.getValue()));
                if (count != null && entries.size() >= count) break;
            }
        }
        out.writeRaw(buildRawArray(entries));
    }

    private static void handleXread(String[] args, RespWriter out, RedisDatabase db) throws IOException {
        Integer count = null;
        Long blockMs = null;
        int idx = 1;
        while (idx < args.length && !args[idx].equalsIgnoreCase("STREAMS")) {
            if (args[idx].equalsIgnoreCase("COUNT")) {
                count = Integer.parseInt(args[++idx]);
            } else if (args[idx].equalsIgnoreCase("BLOCK")) {
                blockMs = Long.parseLong(args[++idx]);
            }
            idx++;
        }
        idx++; // skip "STREAMS"
        int remaining = args.length - idx;
        int numStreams = remaining / 2;
        String[] keys = new String[numStreams];
        StreamId[] startIds = new StreamId[numStreams];
        for (int i = 0; i < numStreams; i++) {
            keys[i] = args[idx + i];
        }
        for (int i = 0; i < numStreams; i++) {
            String rawId = args[idx + numStreams + i];
            if (rawId.equals("$")) {
                RedisValue.StreamValue s = db.getStreamOrNull(keys[i]);
                startIds[i] = s != null ? s.lastId : StreamId.MIN;
            } else {
                startIds[i] = StreamId.parseExplicit(rawId);
            }
        }

        long deadline = (blockMs != null && blockMs > 0) ? System.currentTimeMillis() + blockMs : Long.MAX_VALUE;
        boolean blocking = blockMs != null;

        Object lock = db.getWaitLock();
        synchronized (lock) {
            while (true) {
                List<byte[]> perStreamResults = collectXreadResults(db, keys, startIds, count);
                if (!perStreamResults.isEmpty()) {
                    out.writeRaw(buildRawArray(perStreamResults));
                    return;
                }
                if (!blocking) {
                    out.writeNullArray();
                    return;
                }
                long remainingMs = deadline - System.currentTimeMillis();
                if (blockMs > 0 && remainingMs <= 0) {
                    out.writeNullArray();
                    return;
                }
                try {
                    lock.wait(blockMs > 0 ? remainingMs : 100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    out.writeNullArray();
                    return;
                }
            }
        }
    }

    private static List<byte[]> collectXreadResults(RedisDatabase db, String[] keys, StreamId[] startIds, Integer count) {
        List<byte[]> perStreamResults = new ArrayList<>();
        for (int i = 0; i < keys.length; i++) {
            RedisValue.StreamValue stream = db.getStreamOrNull(keys[i]);
            if (stream == null) continue;
            List<byte[]> entries = new ArrayList<>();
            for (Map.Entry<StreamId, LinkedHashMap<String, String>> e : stream.entries.tailMap(startIds[i], false).entrySet()) {
                entries.add(encodeStreamEntry(e.getKey(), e.getValue()));
                if (count != null && entries.size() >= count) break;
            }
            if (!entries.isEmpty()) {
                ByteArrayOutputStream b = new ByteArrayOutputStream();
                writeQuiet(b, "*2\r\n");
                writeQuiet(b, RespWriter.encodeBulkString(keys[i]));
                writeQuiet(b, buildRawArray(entries));
                perStreamResults.add(b.toByteArray());
            }
        }
        return perStreamResults;
    }

    private static byte[] encodeStreamEntry(StreamId id, LinkedHashMap<String, String> fields) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        writeQuiet(b, "*2\r\n");
        writeQuiet(b, RespWriter.encodeBulkString(id.toString()));
        List<String> flat = new ArrayList<>();
        for (Map.Entry<String, String> f : fields.entrySet()) {
            flat.add(f.getKey());
            flat.add(f.getValue());
        }
        writeQuiet(b, ("*" + flat.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (String s : flat) {
            writeQuiet(b, RespWriter.encodeBulkString(s));
        }
        return b.toByteArray();
    }

    private static byte[] buildRawArray(List<byte[]> items) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        writeQuiet(b, ("*" + items.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (byte[] item : items) writeQuiet(b, item);
        return b.toByteArray();
    }

    private static void writeQuiet(ByteArrayOutputStream b, String s) {
        writeQuiet(b, s.getBytes(StandardCharsets.UTF_8));
    }

    private static void writeQuiet(ByteArrayOutputStream b, byte[] data) {
        b.write(data, 0, data.length);
    }

    // ---------------------------------------------------------------------
    // Misc
    // ---------------------------------------------------------------------

    private static void handleObject(String[] args, RespWriter out, RedisDatabase db) throws IOException {
        if (args.length >= 3 && args[1].equalsIgnoreCase("ENCODING")) {
            RedisValue v = db.get(args[2]);
            if (v == null) {
                out.writeNullBulkString();
                return;
            }
            String encoding = switch (v.type()) {
                case STRING -> {
                    String s = ((RedisValue.StringValue) v).value;
                    try {
                        Long.parseLong(s);
                        yield "int";
                    } catch (NumberFormatException e) {
                        yield s.length() <= 44 ? "embstr" : "raw";
                    }
                }
                case LIST -> "listpack";
                case STREAM -> "stream";
            };
            out.writeBulkString(encoding);
        } else {
            out.writeNullBulkString();
        }
    }

    private static Pattern globToRegex(String glob) {
        StringBuilder sb = new StringBuilder();
        for (char c : glob.toCharArray()) {
            switch (c) {
                case '*' -> sb.append(".*");
                case '?' -> sb.append('.');
                case '.' -> sb.append("\\.");
                default -> sb.append(c);
            }
        }
        return Pattern.compile(sb.toString());
    }
}
