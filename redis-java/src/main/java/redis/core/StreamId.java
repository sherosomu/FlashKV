package redis.core;

import java.util.Objects;

/**
 * A Redis stream entry ID: <millisecondsTime>-<sequenceNumber>.
 */
public final class StreamId implements Comparable<StreamId> {

    public static final StreamId MIN = new StreamId(0, 0);
    public static final StreamId MAX = new StreamId(Long.MAX_VALUE, Long.MAX_VALUE);

    public final long ms;
    public final long seq;

    public StreamId(long ms, long seq) {
        this.ms = ms;
        this.seq = seq;
    }

    public static StreamId parseExplicit(String raw) {
        if (raw.equals("-")) return MIN;
        if (raw.equals("+")) return MAX;
        if (raw.contains("-")) {
            String[] parts = raw.split("-", 2);
            return new StreamId(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
        }
        return new StreamId(Long.parseLong(raw), 0);
    }

    /** Parses a range-start id: bare "5" becomes 5-0. */
    public static StreamId parseRangeStart(String raw) {
        if (raw.equals("-")) return MIN;
        if (raw.equals("+")) return MAX;
        if (raw.contains("-")) {
            String[] parts = raw.split("-", 2);
            return new StreamId(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
        }
        return new StreamId(Long.parseLong(raw), 0);
    }

    /** Parses a range-end id: bare "5" becomes 5-MAX_SEQ. */
    public static StreamId parseRangeEnd(String raw) {
        if (raw.equals("-")) return MIN;
        if (raw.equals("+")) return MAX;
        if (raw.contains("-")) {
            String[] parts = raw.split("-", 2);
            return new StreamId(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
        }
        return new StreamId(Long.parseLong(raw), Long.MAX_VALUE);
    }

    @Override
    public int compareTo(StreamId o) {
        int c = Long.compare(this.ms, o.ms);
        if (c != 0) return c;
        return Long.compare(this.seq, o.seq);
    }

    public StreamId next() {
        if (seq == Long.MAX_VALUE) {
            return new StreamId(ms + 1, 0);
        }
        return new StreamId(ms, seq + 1);
    }

    @Override
    public String toString() {
        return ms + "-" + seq;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StreamId)) return false;
        StreamId streamId = (StreamId) o;
        return ms == streamId.ms && seq == streamId.seq;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ms, seq);
    }
}
