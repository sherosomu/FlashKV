package redis;

import redis.config.ServerConfig;
import redis.core.RedisDatabase;
import redis.rdb.RdbLoader;
import redis.replication.ReplicaClient;
import redis.replication.ReplicationState;
import redis.server.RedisServer;
import redis.server.ServerContext;

public class Main {

    public static void main(String[] args) throws Exception {
        ServerConfig config = ServerConfig.fromArgs(args);
        RedisDatabase database = new RedisDatabase();
        RdbLoader.loadIfPresent(config.getRdbPath(), database);

        ReplicationState replicationState = new ReplicationState(config);
        ServerContext serverContext = new ServerContext(config, database, replicationState);

        if (config.isReplica()) {
            Thread replicaThread = new Thread(new ReplicaClient(serverContext), "replica-link");
            replicaThread.setDaemon(true);
            replicaThread.start();
        }

        new RedisServer(serverContext).start();
    }
}
