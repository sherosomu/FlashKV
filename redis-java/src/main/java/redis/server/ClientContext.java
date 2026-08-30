package redis.server;

import redis.protocol.RespWriter;
import redis.pubsub.PubSubManager;
import redis.replication.ReplicationState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Per-connection state. One instance per accepted socket.
 */
public class ClientContext implements PubSubManager.Subscriber {

    public final String remoteAddress;
    public final RespWriter writer;

    // Transactions (MULTI/EXEC/DISCARD)
    public boolean inTransaction = false;
    public boolean transactionDirty = false;
    public final List<String[]> queuedCommands = new ArrayList<>();

    // Pub/Sub
    public final Set<String> subscribedChannels = new LinkedHashSet<>();

    // Replication: set true once this connection has issued PSYNC and become a replica stream
    public volatile boolean isReplicaConnection = false;
    // When this connection is a replica (post-PSYNC), this tracks its ack offset for WAIT.
    public volatile ReplicationState.ReplicaConnection replicaConnection = null;

    public ClientContext(String remoteAddress, RespWriter writer) {
        this.remoteAddress = remoteAddress;
        this.writer = writer;
    }

    public boolean isSubscribedMode() {
        return !subscribedChannels.isEmpty();
    }

    @Override
    public void deliverMessage(String channel, String payload) throws IOException {
        writer.writeRaw(PubSubManager.encodeMessage(channel, payload));
    }
}
