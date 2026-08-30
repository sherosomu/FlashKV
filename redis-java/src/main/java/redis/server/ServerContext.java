package redis.server;

import redis.config.ServerConfig;
import redis.core.RedisDatabase;
import redis.pubsub.PubSubManager;
import redis.replication.ReplicationState;

/**
 * Shared, server-wide services. One instance exists per running server
 * process and is handed to every client connection / command dispatch.
 */
public class ServerContext {

    public final ServerConfig config;
    public final RedisDatabase database;
    public final ReplicationState replicationState;
    public final PubSubManager pubSubManager = new PubSubManager();

    public ServerContext(ServerConfig config, RedisDatabase database, ReplicationState replicationState) {
        this.config = config;
        this.database = database;
        this.replicationState = replicationState;
    }
}
