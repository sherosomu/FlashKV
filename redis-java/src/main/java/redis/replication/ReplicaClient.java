package redis.replication;

import redis.commands.CommandDispatcher;
import redis.protocol.CountingInputStream;
import redis.protocol.RespReader;
import redis.protocol.RespWriter;
import redis.rdb.RdbLoader;
import redis.server.ClientContext;
import redis.server.ServerContext;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Runs on a replica. Connects out to the configured master, performs the
 * standard handshake (PING / REPLCONF / PSYNC), loads the initial RDB
 * snapshot, then continuously applies the master's propagated command
 * stream while tracking how many bytes have been processed (the replica's
 * replication offset).
 */
public class ReplicaClient implements Runnable {

    private final ServerContext srv;

    public ReplicaClient(ServerContext srv) {
        this.srv = srv;
    }

    @Override
    public void run() {
        String host = srv.config.getMasterHost();
        int port = srv.config.getMasterPort();
        try (Socket socket = new Socket(host, port)) {
            socket.setTcpNoDelay(true);
            OutputStream rawOut = socket.getOutputStream();
            RespWriter handshakeWriter = new RespWriter(rawOut);
            RespReader handshakeReader = new RespReader(socket.getInputStream());

            // 1. PING
            rawOut.write(RespWriter.encodeCommandArray("PING"));
            rawOut.flush();
            handshakeReader.readCommand(); // expect +PONG (readCommand tolerates simple strings poorly; see note below)

            // 2. REPLCONF listening-port <port>
            rawOut.write(RespWriter.encodeCommandArray("REPLCONF", "listening-port", String.valueOf(srv.config.getPort())));
            rawOut.flush();
            handshakeReader.readCommand();

            // 3. REPLCONF capa eof capa psync2
            rawOut.write(RespWriter.encodeCommandArray("REPLCONF", "capa", "eof", "capa", "psync2"));
            rawOut.flush();
            handshakeReader.readCommand();

            // 4. PSYNC ? -1
            rawOut.write(RespWriter.encodeCommandArray("PSYNC", "?", "-1"));
            rawOut.flush();
            String[] fullResyncReply = handshakeReader.readCommand(); // +FULLRESYNC <replid> <offset>
            long baseOffset = 0;
            if (fullResyncReply != null && fullResyncReply.length >= 3) {
                try {
                    baseOffset = Long.parseLong(fullResyncReply[2]);
                } catch (NumberFormatException ignored) {
                    // fall back to 0 if the reply couldn't be parsed
                }
            }

            // 5. Read the RDB payload the master sends (raw, no trailing CRLF)
            byte[] rdb = handshakeReader.readRawBulkPayload();
            RdbLoader.loadFromBytes(rdb, srv.database);

            // 6. Continuously apply the propagated command stream
            CountingInputStream counting = new CountingInputStream(socket.getInputStream());
            RespReader streamReader = new RespReader(counting);
            RespWriter ackWriter = new RespWriter(rawOut);
            ClientContext replicaCtx = new ClientContext("master", new RespWriter(OutputStream.nullOutputStream()));

            while (true) {
                String[] args = streamReader.readCommand();
                if (args == null) break;
                if (args.length == 0) continue;

                if (args[0].equalsIgnoreCase("REPLCONF") && args.length > 1 && args[1].equalsIgnoreCase("GETACK")) {
                    long offset = baseOffset + counting.getBytesRead();
                    srv.config.setMasterReplOffset(offset);
                    rawOut.write(RespWriter.encodeCommandArray("REPLCONF", "ACK", String.valueOf(offset)));
                    rawOut.flush();
                    continue;
                }

                CommandDispatcher.dispatch(args, replicaCtx, srv, true);
                srv.config.setMasterReplOffset(baseOffset + counting.getBytesRead());
            }
        } catch (IOException e) {
            System.err.println("Replication connection to " + host + ":" + port + " failed: " + e.getMessage());
        }
    }
}
