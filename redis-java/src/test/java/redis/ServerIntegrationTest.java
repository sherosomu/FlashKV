package redis;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.config.ServerConfig;
import redis.core.RedisDatabase;
import redis.replication.ReplicaClient;
import redis.replication.ReplicationState;
import redis.server.RedisServer;
import redis.server.ServerContext;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Starts real servers on free ports and talks RESP to them over TCP. */
class ServerIntegrationTest {

    static ServerContext master;
    static int masterPort;

    static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    static ServerContext startServer(String... args) throws Exception {
        ServerConfig config = ServerConfig.fromArgs(args);
        ServerContext ctx = new ServerContext(config, new RedisDatabase(), new ReplicationState(config));
        Thread t = new Thread(() -> {
            try {
                new RedisServer(ctx).start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        t.setDaemon(true);
        t.start();
        // wait until it accepts connections
        for (int i = 0; i < 100; i++) {
            try (TestClient c = new TestClient(config.getPort())) {
                if ("PONG".equals(c.call("PING"))) return ctx;
            } catch (IOException e) {
                Thread.sleep(50);
            }
        }
        throw new IllegalStateException("server did not start");
    }

    @BeforeAll
    static void start() throws Exception {
        masterPort = freePort();
        master = startServer("--port", String.valueOf(masterPort), "--dir", "/nonexistent");
    }

    @AfterAll
    static void stop() {
        master.database.shutdown();
    }

    @Test
    void basicCommands() throws Exception {
        try (TestClient c = new TestClient(masterPort)) {
            assertEquals("OK", c.call("SET", "basic", "bar"));
            assertEquals("bar", c.callStr("GET", "basic"));
            assertEquals(3L, c.call("RPUSH", "basic:list", "a", "b", "c"));
            assertEquals(List.of("a", "b", "c"), c.callStr("LRANGE", "basic:list", "0", "-1"));
            assertEquals("a", c.callStr("LPOP", "basic:list"));
            assertEquals(List.of("c", "b"), c.callStr("RPOP", "basic:list", "5"));
            assertNull(c.call("LPOP", "basic:list"));
        }
    }

    @Test
    void binaryValuesRoundTrip() throws Exception {
        byte[] value = new byte[256];
        for (int i = 0; i < 256; i++) value[i] = (byte) i;
        try (TestClient c = new TestClient(masterPort)) {
            assertEquals("OK", c.call("SET", "bin", value));
            assertArrayEquals(value, (byte[]) c.call("GET", "bin"));
        }
    }

    @Test
    void utf8TextStillRoundTrips() throws Exception {
        try (TestClient c = new TestClient(masterPort)) {
            c.call("SET", "utf8", "héllo ✓ 你好");
            assertEquals("héllo ✓ 你好", c.callStr("GET", "utf8"));
        }
    }

    @Test
    void concurrentIncrOverTcpLosesNothing() throws Exception {
        int clients = 8, perClient = 2_000;
        RedisDatabaseTest.runConcurrently(clients, () -> {
            try (TestClient c = new TestClient(masterPort)) {
                for (int i = 0; i < perClient; i++) c.call("INCR", "tcp:counter");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        try (TestClient c = new TestClient(masterPort)) {
            assertEquals(String.valueOf(clients * perClient), c.callStr("GET", "tcp:counter"));
        }
    }

    @Test
    void concurrentPushAndPopNeverCrashOrLoseItems() throws Exception {
        int clients = 8, perClient = 2_000;
        AtomicInteger popped = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        RedisDatabaseTest.runConcurrently(clients * 2, new Runnable() {
            final AtomicInteger roles = new AtomicInteger();

            @Override
            public void run() {
                boolean pusher = roles.getAndIncrement() % 2 == 0;
                try (TestClient c = new TestClient(masterPort)) {
                    for (int i = 0; i < perClient; i++) {
                        Object r = pusher ? c.call("LPUSH", "tcp:list", "x") : c.call("LPOP", "tcp:list");
                        if (r instanceof RuntimeException) errors.incrementAndGet();
                        if (!pusher && r != null) popped.incrementAndGet();
                    }
                } catch (IOException e) {
                    throw new RuntimeException("connection dropped", e);
                }
            }
        });
        assertEquals(0, errors.get());
        try (TestClient c = new TestClient(masterPort)) {
            long left = (Long) c.call("LLEN", "tcp:list");
            assertEquals(clients * perClient, popped.get() + left, "every pushed item was popped or is still there");
        }
    }

    @Test
    void activeExpiryRemovesKeysThatAreNeverRead() throws Exception {
        try (TestClient c = new TestClient(masterPort)) {
            for (int i = 0; i < 300; i++) c.call("SET", "ttl:" + i, "v", "PX", "50");
        }
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline && hasTtlKeys()) Thread.sleep(50);
        assertFalse(hasTtlKeys(), "keys with an expired TTL should be gone without any client reading them");
    }

    private static boolean hasTtlKeys() {
        // isStored sees entries even after they expire, so this only turns false once they are reclaimed
        for (int i = 0; i < 300; i++) {
            if (master.database.isStored("ttl:" + i)) return true;
        }
        return false;
    }

    @Test
    void blpopIsWokenByPush() throws Exception {
        try (TestClient waiter = new TestClient(masterPort); TestClient pusher = new TestClient(masterPort)) {
            Thread t = new Thread(() -> {
                try {
                    Thread.sleep(200);
                    pusher.call("RPUSH", "bl:list", "hello");
                } catch (Exception ignored) {
                }
            });
            t.start();
            assertEquals(List.of("bl:list", "hello"), waiter.callStr("BLPOP", "bl:list", "5"));
            t.join();
        }
    }

    @Test
    void replicaReceivesWritesAndWaitCountsIt() throws Exception {
        int mPort = freePort();
        ServerContext m = startServer("--port", String.valueOf(mPort), "--dir", "/nonexistent");
        ServerConfig rConfig = ServerConfig.fromArgs(new String[]{"--port", String.valueOf(freePort()), "--replicaof", "localhost " + mPort});
        ServerContext replica = new ServerContext(rConfig, new RedisDatabase(), new ReplicationState(rConfig));
        Thread link = new Thread(new ReplicaClient(replica));
        link.setDaemon(true);
        link.start();
        try (TestClient c = new TestClient(mPort)) {
            long deadline = System.currentTimeMillis() + 5_000;
            while (m.replicationState.replicaCount() == 0 && System.currentTimeMillis() < deadline) Thread.sleep(20);
            assertEquals(1, m.replicationState.replicaCount());

            for (int i = 0; i < 200; i++) c.call("SET", "r" + i, "v" + i);
            c.call("RPUSH", "rlist", "a", "b");
            c.call("INCR", "rcount");
            assertEquals(1L, c.call("WAIT", "1", "2000"));
            assertEquals("v199", replica.database.getString("r199"));
            assertEquals("1", replica.database.getString("rcount"));
            assertEquals(2, replica.database.getListOrNull("rlist").items.size());
            assertEquals(m.config.getMasterReplOffset(), replica.config.getMasterReplOffset(),
                    "replica consumed exactly the bytes the master sent");
        } finally {
            m.database.shutdown();
            replica.database.shutdown();
        }
    }
}
