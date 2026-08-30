package redis.server;

import redis.commands.CommandDispatcher;
import redis.protocol.RespReader;
import redis.protocol.RespWriter;
import redis.rdb.EmptyRdb;
import redis.replication.ReplicationState;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Runs on its own thread for the lifetime of one client connection.
 */
public class ClientHandler implements Runnable {

    private final Socket socket;
    private final ServerContext srv;

    public ClientHandler(Socket socket, ServerContext srv) {
        this.socket = socket;
        this.srv = srv;
    }

    @Override
    public void run() {
        ClientContext ctx = null;
        try {
            socket.setTcpNoDelay(true);
            RespReader reader = new RespReader(socket.getInputStream());
            RespWriter writer = new RespWriter(socket.getOutputStream());
            ctx = new ClientContext(socket.getRemoteSocketAddress().toString(), writer);

            while (true) {
                String[] args = reader.readCommand();
                if (args == null) break; // client closed connection
                if (args.length == 0) continue;

                String cmd = args[0].toUpperCase();
                if (cmd.equals("PSYNC")) {
                    handlePsync(ctx, writer, socket.getOutputStream());
                    continue;
                }
                CommandDispatcher.dispatch(args, ctx, srv, false);
                if (cmd.equals("QUIT")) break;
            }
        } catch (IOException e) {
            // connection dropped; nothing else to do
        } finally {
            cleanup(ctx);
        }
    }

    private void handlePsync(ClientContext ctx, RespWriter writer, OutputStream rawOut) throws IOException {
        String replId = srv.config.getReplicationId();
        writer.writeSimpleString("FULLRESYNC " + replId + " " + srv.config.getMasterReplOffset());

        byte[] rdb = EmptyRdb.bytes();
        String header = "$" + rdb.length + "\r\n";
        rawOut.write(header.getBytes(StandardCharsets.UTF_8));
        rawOut.write(rdb);
        rawOut.flush();

        ReplicationState.ReplicaConnection rc = new ReplicationState.ReplicaConnection(rawOut);
        ctx.replicaConnection = rc;
        srv.replicationState.addReplica(rc);
    }

    private void cleanup(ClientContext ctx) {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
        if (ctx != null) {
            srv.pubSubManager.unsubscribeAll(ctx);
            if (ctx.replicaConnection != null) {
                srv.replicationState.removeReplica(ctx.replicaConnection);
            }
        }
    }
}
