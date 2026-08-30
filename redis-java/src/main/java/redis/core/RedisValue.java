package redis.core;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.TreeMap;

/**
 * Base type for anything stored under a key. Concrete subclasses hold the
 * actual data structure (string / list / stream).
 */
public abstract class RedisValue {

    public enum Type {
        STRING, LIST, STREAM
    }

    public abstract Type type();

    public String typeName() {
        return switch (type()) {
            case STRING -> "string";
            case LIST -> "list";
            case STREAM -> "stream";
        };
    }

    public static final class StringValue extends RedisValue {
        public volatile String value;

        public StringValue(String value) {
            this.value = value;
        }

        @Override
        public Type type() {
            return Type.STRING;
        }
    }

    public static final class ListValue extends RedisValue {
        public final LinkedList<String> items = new LinkedList<>();

        @Override
        public Type type() {
            return Type.LIST;
        }
    }

    public static final class StreamValue extends RedisValue {
        // Insertion-ordered by key since TreeMap is naturally sorted by StreamId
        public final TreeMap<StreamId, LinkedHashMap<String, String>> entries = new TreeMap<>();
        public StreamId lastId = new StreamId(0, 0);

        @Override
        public Type type() {
            return Type.STREAM;
        }
    }
}
