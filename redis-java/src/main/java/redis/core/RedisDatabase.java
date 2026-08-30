package redis.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The in-memory keyspace. Thread-safe: a single database instance is shared
 * across all client-handling threads.
 *
 * Blocking commands (BLPOP, XREAD BLOCK) use a single monitor object and
 * wait/notify. This is simple and correct (if not maximally scalable) - any
 * write that could unblock a waiter calls signalDataChanged().
 */
public class RedisDatabase {

    private static final class Entry {
        final RedisValue value;
        volatile long expireAtMillis; // -1 means no expiry

        Entry(RedisValue value, long expireAtMillis) {
            this.value = value;
            this.expireAtMillis = expireAtMillis;
        }
    }

    private final Map<String, Entry> store = new ConcurrentHashMap<>();
    private final Object waitLock = new Object();

    // ---------------- generic key operations ----------------

    public void setString(String key, String value, long expireAtMillis) {
        store.put(key, new Entry(new RedisValue.StringValue(value), expireAtMillis));
    }

    /** Returns the live (non-expired) value for a key, or null. Lazily evicts expired keys. */
    public RedisValue get(String key) {
        Entry e = store.get(key);
        if (e == null) return null;
        if (isExpired(e)) {
            store.remove(key);
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
        List<String> toRemove = new ArrayList<>();
        Set<String> result = new java.util.HashSet<>();
        for (Map.Entry<String, Entry> me : store.entrySet()) {
            if (isExpired(me.getValue())) {
                toRemove.add(me.getKey());
            } else {
                result.add(me.getKey());
            }
        }
        toRemove.forEach(store::remove);
        return result;
    }

    public boolean expire(String key, long atMillis) {
        Entry e = store.get(key);
        if (e == null || isExpired(e)) return false;
        e.expireAtMillis = atMillis;
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

    // ---------------- typed accessors used by command handlers ----------------

    /** Gets (creating if absent) the list at key. Throws WrongTypeException if key holds a non-list. */
    public RedisValue.ListValue getOrCreateList(String key) {
        Entry e = store.get(key);
        if (e != null && !isExpired(e)) {
            if (!(e.value instanceof RedisValue.ListValue)) {
                throw new WrongTypeException();
            }
            return (RedisValue.ListValue) e.value;
        }
        RedisValue.ListValue list = new RedisValue.ListValue();
        store.put(key, new Entry(list, -1));
        return list;
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
        Entry e = store.get(key);
        if (e != null && !isExpired(e)) {
            if (!(e.value instanceof RedisValue.StreamValue)) {
                throw new WrongTypeException();
            }
            return (RedisValue.StreamValue) e.value;
        }
        RedisValue.StreamValue stream = new RedisValue.StreamValue();
        store.put(key, new Entry(stream, -1));
        return stream;
    }

    public RedisValue.StreamValue getStreamOrNull(String key) {
        RedisValue v = get(key);
        if (v == null) return null;
        if (!(v instanceof RedisValue.StreamValue sv)) {
            throw new WrongTypeException();
        }
        return sv;
    }

    /** Removes a key entirely if its list is now empty (matches Redis semantics). */
    public void removeIfEmptyList(String key) {
        Entry e = store.get(key);
        if (e != null && e.value instanceof RedisValue.ListValue lv && lv.items.isEmpty()) {
            store.remove(key);
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
