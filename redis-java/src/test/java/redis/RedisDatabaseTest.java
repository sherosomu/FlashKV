package redis;

import org.junit.jupiter.api.Test;
import redis.core.RedisDatabase;
import redis.core.RedisValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RedisDatabaseTest {

    private static long past() {
        return System.currentTimeMillis() - 1;
    }

    @Test
    void passiveExpiryHidesAndDeletesExpiredKey() {
        RedisDatabase db = new RedisDatabase(false);
        db.setString("k", "v", past());
        assertEquals(1, db.size(), "still stored until something touches it");
        assertNull(db.getString("k"));
        assertEquals(0, db.size(), "read deleted it");
    }

    @Test
    void activeExpiryCycleDeletesKeysNobodyReads() {
        RedisDatabase db = new RedisDatabase(false);
        for (int i = 0; i < 500; i++) db.setString("dead" + i, "v", past());
        for (int i = 0; i < 50; i++) db.setString("live" + i, "v", -1);
        for (int i = 0; i < 50; i++) db.setString("later" + i, "v", System.currentTimeMillis() + 60_000);

        int deleted = 0;
        for (int round = 0; round < 100 && db.size() > 100; round++) {
            deleted += db.activeExpireCycle();
        }
        assertEquals(500, deleted);
        assertEquals(100, db.size(), "keys without TTL and with a future TTL are untouched");
        assertEquals("v", db.getString("live0"));
        assertEquals("v", db.getString("later0"));
    }

    @Test
    void activeExpiryCycleRepeatsWhenMostOfTheSampleIsExpired() {
        RedisDatabase db = new RedisDatabase(false);
        for (int i = 0; i < 200; i++) db.setString("dead" + i, "v", past());
        // 100% of each sample is expired, so one call should loop well past one 20-key sample
        int deleted = db.activeExpireCycle();
        assertTrue(deleted > RedisDatabase.ACTIVE_EXPIRE_SAMPLE, "deleted only " + deleted);
    }

    @Test
    void activeExpiryDoesNotDeleteKeyRewrittenWithoutTtl() {
        RedisDatabase db = new RedisDatabase(false);
        db.setString("k", "old", past());
        db.setString("k", "new", -1);       // overwritten, no TTL now
        db.activeExpireCycle();
        assertEquals("new", db.getString("k"));
    }

    @Test
    void backgroundThreadExpiresKeysOnItsOwn() throws Exception {
        RedisDatabase db = new RedisDatabase(true);
        try {
            long at = System.currentTimeMillis() + 50;
            for (int i = 0; i < 1000; i++) db.setString("k" + i, "v", at);
            long deadline = System.currentTimeMillis() + 5_000;
            while (db.size() > 0 && System.currentTimeMillis() < deadline) Thread.sleep(50);
            assertEquals(0, db.size(), "no reads happened, so only active expiry can have removed them");
        } finally {
            db.shutdown();
        }
    }

    @Test
    void incrIsAtomicUnderContention() throws Exception {
        RedisDatabase db = new RedisDatabase(false);
        int threads = 16, perThread = 10_000;
        runConcurrently(threads, () -> {
            for (int i = 0; i < perThread; i++) db.incrBy("counter", 1);
        });
        assertEquals(String.valueOf(threads * perThread), db.getString("counter"));
    }

    @Test
    void incrKeepsTtlAndRejectsNonIntegers() {
        RedisDatabase db = new RedisDatabase(false);
        db.setString("n", "5", System.currentTimeMillis() + 60_000);
        assertEquals(6, db.incrBy("n", 1));
        assertTrue(db.pttl("n") > 0, "INCR must not drop the TTL");
        db.setString("s", "abc", -1);
        assertThrows(NumberFormatException.class, () -> db.incrBy("s", 1));
        assertEquals("abc", db.getString("s"), "failed INCR leaves value unchanged");
        db.setString("max", String.valueOf(Long.MAX_VALUE), -1);
        assertThrows(ArithmeticException.class, () -> db.incrBy("max", 1));
    }

    @Test
    void getOrCreateListReturnsOneListForAllThreads() throws Exception {
        RedisDatabase db = new RedisDatabase(false);
        int threads = 16;
        List<RedisValue.ListValue> seen = java.util.Collections.synchronizedList(new ArrayList<>());
        runConcurrently(threads, () -> seen.add(db.getOrCreateList("l")));
        for (RedisValue.ListValue l : seen) assertSame(seen.get(0), l);
    }

    @Test
    void setNxOnlyOneWriterWins() throws Exception {
        RedisDatabase db = new RedisDatabase(false);
        int threads = 16;
        java.util.concurrent.atomic.AtomicInteger winners = new java.util.concurrent.atomic.AtomicInteger();
        runConcurrently(threads, () -> {
            if (db.setStringIfAbsent("lock", Thread.currentThread().getName(), -1)) winners.incrementAndGet();
        });
        assertEquals(1, winners.get());
    }

    static void runConcurrently(int threads, Runnable task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                start.await();
                task.run();
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) f.get(30, TimeUnit.SECONDS);
        pool.shutdown();
    }
}
