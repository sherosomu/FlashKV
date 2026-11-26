package redis.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The in-memory keyspace. Thread-safe: a single database instance is shared
 * across all client-handling threads.
 *
 * Expiry is handled two ways, like Redis:
 *  - passive: every read checks the key's deadline and deletes it if expired;
 *  - active: a background thread runs {@link #activeExpireCycle()} every
 *    100 ms, sampling keys that have a TTL and deleting expired ones, so keys
 *    that are never read again still get reclaimed.
 *
 * Blocking commands (BLPOP, XREAD BLOCK) use a single monitor object and
 * wait/notify. All list and stream mutations happen while holding that
 * monitor, which also makes compound list operations (check-then-pop) atomic.
 */
public class RedisDatabase {

    /** Active expiry tuning, mirroring Redis's defaults (hz 10, 20 keys per sample, 25% threshold). */
    public static final long ACTIVE_EXPIRE_PERIOD_MS = 100;
    public static final int ACTIVE_EXPIRE_SAMPLE = 20;
    public static final int ACTIVE_EXPIRE_REPEAT_PERCENT = 25;
    public static final long ACTIVE_EXPIRE_TIME_BUDGET_MS = 25;

    private static final class Entry {
        final RedisValue value;
        volatile long expireAtMillis; // -1 means no expiry

        Entry(RedisValue value, long expireAtMillis) {
            this.value = value;
            this.expireAtMillis = expireAtMillis;
        }
    }

    private final Map<String, Entry> store = new ConcurrentHashMap<>();
    /** Keys that have (or recently had) a TTL. May hold stale names; the expiry cycle prunes them. */
    private final Set<String> volatileKeys = ConcurrentHashMap.newKeySet();
    /** Cursor into volatileKeys, kept across cycles so every key is eventually visited. */
    private Iterator<String> expireCursor = null;
    private final Object waitLock = new Object();
    private final ScheduledExecutorService expirer;

    /** Creates a database with the background active-expiry thread running. */
    public RedisDatabase() {
        this(true);
    }

    /** @param startActiveExpiry false for tests that want to drive {@link #activeExpireCycle()} by hand */
    public RedisDatabase(boolean startActiveExpiry) {
        if (startActiveExpiry) {
            expirer = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "active-expire");
                t.setDaemon(true);
                return t;
            });
            expirer.scheduleAtFixedRate(this::activeExpireCycleSafely,
                    ACTIVE_EXPIRE_PERIOD_MS, ACTIVE_EXPIRE_PERIOD_MS, TimeUnit.MILLISECONDS);
        } else {
            expirer = null;
        }
    }

    // ---------------- generic key operations ----------------

    public void setString(String key, String value, long expireAtMillis) {
        store.put(key, new Entry(new RedisValue.StringValue(value), expireAtMillis));
        if (expireAtMillis >= 0) volatileKeys.add(key);
    }

    /** Sets the key only if it does not exist (or has expired). Atomic. Returns true if set. */
    public boolean setStringIfAbsent(String key, String value, long expireAtMillis) {
        boolean[] set = {false};
        store.compute(key, (k, e) -> {
            if (e != null && !isExpired(e)) return e;
            set[0] = true;
            return new Entry(new RedisValue.StringValue(value), expireAtMillis);
        });
        if (set[0] && expireAtMillis >= 0) volatileKeys.add(key);
        return set[0];
    }

    /** Sets the key only if it already exists (and has not expired). Atomic. Returns true if set. */
    public boolean setStringIfPresent(String key, String value, long expireAtMillis) {
        boolean[] set = {false};
        store.compute(key, (k, e) -> {
            if (e == null || isExpired(e)) return null;
            set[0] = true;
            return new Entry(new RedisValue.StringValue(value), expireAtMillis);
        });
        if (set[0] && expireAtMillis >= 0) volatileKeys.add(key);
        return set[0];
    }

    /**
     * Atomically adds delta to the integer stored at key (missing key counts as 0),
     * keeping any existing TTL. ConcurrentHashMap.compute runs the whole
     * read-modify-write while holding that key's bin lock, so concurrent INCRs
     * can never read the same old value.
     *
     * @throws NumberFormatException if the value is not an integer or the result overflows
     * @throws WrongTypeException    if the key holds a non-string
     */
    public long incrBy(String key, long delta) {
        long[] result = new long[1];
        store.compute(key, (k, e) -> {
            long current = 0;
            long expireAt = -1;
            if (e != null && !isExpired(e)) {
                if (!(e.value instanceof RedisValue.StringValue sv)) {
                    throw new WrongTypeException();
                }
                current = Long.parseLong(sv.value);
                expireAt = e.expireAtMillis;
            }
            result[0] = Math.addExact(current, delta);
            return new Entry(new RedisValue.StringValue(Long.toString(result[0])), expireAt);
        });
        return result[0];
    }

    /** Returns the live (non-expired) value for a key, or null. Lazily evicts expired keys. */
    public RedisValue get(String key) {
        Entry e = store.get(key);
        if (e == null) return null;
        if (isExpired(e)) {
            // remove(key, e) only deletes this exact expired entry, never a value
            // another thread has just written under the same key.
            store.remove(key, e);
            return null;
        }
        return e.value;
    }

    public String getString(String key) {
        RedisValue v = get(key);
        if (v == null) return null;
        if (v instanceof RedisValue.StringValue sv) {
            return sv.value;
        }
        return null;
    }

    public boolean exists(String key) {
        return get(key) != null;
    }

    public boolean del(String key) {
        Entry e = store.remove(key);
        return e != null && !isExpired(e);
    }

    public Set<String> keys() {
        // snapshot, filtering out expired entries
        List<Map.Entry<String, Entry>> toRemove = new ArrayList<>();
        Set<String> result = new java.util.HashSet<>();
        for (Map.Entry<String, Entry> me : store.entrySet()) {
            if (isExpired(me.getValue())) {
                toRemove.add(me);
            } else {
                result.add(me.getKey());
            }
        }
        toRemove.forEach(me -> store.remove(me.getKey(), me.getValue()));
        return result;
    }

    /** True if an entry is physically stored, even if it has expired but not been reclaimed yet. */
    public boolean isStored(String key) {
        return store.containsKey(key);
    }

    public int size() {
        return store.size();
    }

    public boolean expire(String key, long atMillis) {
        Entry e = store.get(key);
        if (e == null || isExpired(e)) return false;
        e.expireAtMillis = atMillis;
        volatileKeys.add(key);
        return true;
    }

    public boolean persist(String key) {
        Entry e = store.get(key);
        if (e == null || isExpired(e)) return false;
        if (e.expireAtMillis < 0) return false;
        e.expireAtMillis = -1;
        return true;
    }

    /** Returns -2 if key doesn't exist, -1 if no expiry, else ms remaining. */
    public long pttl(String key) {
        Entry e = store.get(key);
        if (e == null || isExpired(e)) return -2;
        if (e.expireAtMillis < 0) return -1;
        return Math.max(0, e.expireAtMillis - System.currentTimeMillis());
    }

    private boolean isExpired(Entry e) {
        return e.expireAtMillis >= 0 && System.currentTimeMillis() >= e.expireAtMillis;
    }

    // ---------------- active expiry ----------------

    private void activeExpireCycleSafely() {
        try {
            activeExpireCycle();
        } catch (RuntimeException e) {
            // never let one bad cycle kill the scheduler thread
            System.err.println("active expire cycle failed: " + e);
        }
    }

    /**
     * One run of active expiry, modelled on Redis's activeExpireCycle:
     * check up to {@value #ACTIVE_EXPIRE_SAMPLE} keys that have a TTL, delete
     * the expired ones, and if more than {@value #ACTIVE_EXPIRE_REPEAT_PERCENT}%
     * of the sample was expired, assume there are many more and go again, until
     * a {@value #ACTIVE_EXPIRE_TIME_BUDGET_MS} ms time budget runs out.
     *
     * Like Redis 6+, it walks the TTL keys with a cursor that persists between
     * runs instead of picking purely random keys, so every key is visited
     * eventually and the work per run stays bounded.
     *
     * @return number of keys deleted
     */
    public synchronized int activeExpireCycle() {
        long start = System.currentTimeMillis();
        int totalExpired = 0;
        while (true) {
            int sampled = 0;
            int expired = 0;
            while (sampled < ACTIVE_EXPIRE_SAMPLE) {
                if (expireCursor == null || !expireCursor.hasNext()) {
                    if (sampled > 0 || volatileKeys.isEmpty()) break; // finished a full pass
                    expireCursor = volatileKeys.iterator();
                    if (!expireCursor.hasNext()) break;
                }
                String key = expireCursor.next();
                sampled++;
                Entry e = store.get(key);
                if (e == null || e.expireAtMillis < 0) {
                    volatileKeys.remove(key); // stale name: deleted, or TTL removed by PERSIST/SET
                    continue;
                }
                if (isExpired(e) && store.remove(key, e)) {
                    volatileKeys.remove(key);
                    expired++;
                }
            }
            totalExpired += expired;
            boolean manyExpired = sampled > 0 && expired * 100 > sampled * ACTIVE_EXPIRE_REPEAT_PERCENT;
            boolean outOfTime = System.currentTimeMillis() - start >= ACTIVE_EXPIRE_TIME_BUDGET_MS;
            if (!manyExpired || outOfTime) return totalExpired;
        }
    }

    /** Stops the background expiry thread (used by tests). */
    public void shutdown() {
        if (expirer != null) expirer.shutdownNow();
    }

    // ---------------- typed accessors used by command handlers ----------------

    /**
     * Gets (creating if absent) the list at key. Throws WrongTypeException if key holds a non-list.
     * Uses compute so two threads can never both create a list for the same key.
     */
    public RedisValue.ListValue getOrCreateList(String key) {
        Entry entry = store.compute(key, (k, e) -> {
            if (e != null && !isExpired(e)) {
                if (!(e.value instanceof RedisValue.ListValue)) {
                    throw new WrongTypeException();
                }
                return e;
            }
            return new Entry(new RedisValue.ListValue(), -1);
        });
        return (RedisValue.ListValue) entry.value;
    }

    public RedisValue.ListValue getListOrNull(String key) {
        RedisValue v = get(key);
        if (v == null) return null;
        if (!(v instanceof RedisValue.ListValue lv)) {
            throw new WrongTypeException();
        }
        return lv;
    }

    public RedisValue.StreamValue getOrCreateStream(String key) {
        Entry entry = store.compute(key, (k, e) -> {
            if (e != null && !isExpired(e)) {
                if (!(e.value instanceof RedisValue.StreamValue)) {
                    throw new WrongTypeException();
                }
                return e;
            }
            return new Entry(new RedisValue.StreamValue(), -1);
        });
        return (RedisValue.StreamValue) entry.value;
    }

    public RedisValue.StreamValue getStreamOrNull(String key) {
        RedisValue v = get(key);
        if (v == null) return null;
        if (!(v instanceof RedisValue.StreamValue sv)) {
            throw new WrongTypeException();
        }
        return sv;
    }

    /** Removes a key entirely if its list is now empty (matches Redis semantics). Call while holding the wait lock. */
    public void removeIfEmptyList(String key) {
        Entry e = store.get(key);
        if (e != null && e.value instanceof RedisValue.ListValue lv && lv.items.isEmpty()) {
            store.remove(key, e);
        }
    }

    // ---------------- blocking support ----------------

    public Object getWaitLock() {
        return waitLock;
    }

    public void signalDataChanged() {
        synchronized (waitLock) {
            waitLock.notifyAll();
        }
    }

    public static final class WrongTypeException extends RuntimeException {
        public WrongTypeException() {
            super("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }
}
