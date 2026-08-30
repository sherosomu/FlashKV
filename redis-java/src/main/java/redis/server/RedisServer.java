package redis.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Listens for incoming TCP connections and hands each one off to a new
 * ClientHandler thread.
 */
public class RedisServer {

    private final ServerContext srv;

    public RedisServer(ServerContext srv) {
        this.srv = srv;
    }

    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(srv.config.getPort());
        serverSocket.setReuseAddress(true);
        System.out.println("Redis server listening on port " + srv.config.getPort()
                + (srv.config.isReplica() ? " (replica of " + srv.config.getMasterHost() + ":" + srv.config.getMasterPort() + ")" : " (master)"));

        while (true) {
            Socket client = serverSocket.accept();
            Thread t = new Thread(new ClientHandler(client, srv));
            t.setDaemon(true);
            t.start();
        }
    }
}
