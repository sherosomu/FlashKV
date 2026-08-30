package redis.replication;

import redis.config.ServerConfig;
import redis.protocol.RespWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Lives on the master. Tracks every connected replica connection so writes
 * can be propagated to all of them, and supports the WAIT command by
 * requesting + counting acknowledgements.
 */
public class ReplicationState {

    public static final class ReplicaConnection {
        public final OutputStream rawOut;
        public volatile long ackOffset = 0;

        public ReplicaConnection(OutputStream rawOut) {
            this.rawOut = rawOut;
        }
    }

    private final ServerConfig config;
    private final List<ReplicaConnection> replicas = new CopyOnWriteArrayList<>();

    public ReplicationState(ServerConfig config) {
        this.config = config;
    }

    public void addReplica(ReplicaConnection rc) {
        replicas.add(rc);
    }

    public void removeReplica(ReplicaConnection rc) {
        replicas.remove(rc);
    }

    public int replicaCount() {
        return replicas.size();
    }

    /** Propagates a write command to all connected replicas and advances the master offset. */
    public synchronized void propagate(byte[] encodedCommand) {
        for (ReplicaConnection rc : replicas) {
            try {
                rc.rawOut.write(encodedCommand);
                rc.rawOut.flush();
            } catch (IOException e) {
                // connection likely dead; it will be removed by its handler thread
            }
        }
        config.addToReplOffset(encodedCommand.length);
    }

    /**
     * Implements WAIT: asks all replicas to report their offset (via
     * REPLCONF GETACK *) and blocks (up to timeoutMs) until at least
     * numReplicas have acknowledged an offset >= the offset at call time.
     */
    public int waitForAcks(int numReplicas, long timeoutMs) {
        long targetOffset = config.getMasterReplOffset();

        if (replicas.isEmpty()) {
            return 0;
        }
        if (targetOffset == 0) {
            // nothing propagated yet - all replicas are trivially caught up
            return replicas.size();
        }

        byte[] getAck = RespWriter.encodeCommandArray("REPLCONF", "GETACK", "*");
        for (ReplicaConnection rc : replicas) {
            try {
                rc.rawOut.write(getAck);
                rc.rawOut.flush();
            } catch (IOException ignored) {
            }
        }
        config.addToReplOffset(getAck.length);

        long deadline = System.currentTimeMillis() + timeoutMs;
        int acked = countAcked(targetOffset);
        while (acked < numReplicas && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            acked = countAcked(targetOffset);
        }
        return acked;
    }

    private int countAcked(long targetOffset) {
        int count = 0;
        for (ReplicaConnection rc : replicas) {
            if (rc.ackOffset >= targetOffset) {
                count++;
            }
        }
        return count;
    }
}
