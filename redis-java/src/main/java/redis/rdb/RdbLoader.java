package redis.rdb;

import redis.core.RedisDatabase;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * Parses an RDB (Redis Database) file well enough to restore string keys and
 * their expiry times into a RedisDatabase at startup. This intentionally
 * supports the subset of the RDB format exercised by the "RDB persistence"
 * stages of the challenge: header, aux fields, resizedb, string-encoded
 * key/value pairs, and both millisecond/second expiry opcodes.
 *
 * List/hash/set/zset encoded values are skipped defensively where possible,
 * but are not required for the challenge and are not fully implemented.
 */
public class RdbLoader {

    private static final int OP_AUX = 0xFA;
    private static final int OP_RESIZEDB = 0xFB;
    private static final int OP_EXPIRETIME_MS = 0xFC;
    private static final int OP_EXPIRETIME_SEC = 0xFD;
    private static final int OP_SELECTDB = 0xFE;
    private static final int OP_EOF = 0xFF;

    private final byte[] data;
    private int pos = 0;

    private RdbLoader(byte[] data) {
        this.data = data;
    }

    public static void loadIfPresent(String path, RedisDatabase db) {
        File f = new File(path);
        if (!f.exists() || !f.isFile()) {
            return;
        }
        try {
            byte[] bytes = Files.readAllBytes(f.toPath());
            new RdbLoader(bytes).parse(db);
        } catch (Exception e) {
            System.err.println("Warning: failed to load RDB file at " + path + ": " + e.getMessage());
        }
    }

    /** Loads an RDB image received in-memory, e.g. from a master during PSYNC full resync. */
    public static void loadFromBytes(byte[] bytes, RedisDatabase db) {
        try {
            new RdbLoader(bytes).parse(db);
        } catch (Exception e) {
            System.err.println("Warning: failed to parse RDB payload from master: " + e.getMessage());
        }
    }

    private void parse(RedisDatabase db) {
        if (data.length < 9 || data[0] != 'R' || data[1] != 'E' || data[2] != 'D' || data[3] != 'I' || data[4] != 'S') {
            return; // not a valid RDB file, start with empty DB
        }
        pos = 9; // skip "REDIS" + 4-digit version

        Long pendingExpireAt = null;

        while (pos < data.length) {
            int opcode = readByteUnsigned();
            if (opcode == OP_EOF) {
                break;
            } else if (opcode == OP_SELECTDB) {
                readLength(); // db number, ignored (single logical DB here)
            } else if (opcode == OP_RESIZEDB) {
                readLength(); // hash table size
                readLength(); // expire hash table size
            } else if (opcode == OP_AUX) {
                readString(); // key
                readString(); // value
            } else if (opcode == OP_EXPIRETIME_MS) {
                long ms = readLongLE();
                pendingExpireAt = ms;
            } else if (opcode == OP_EXPIRETIME_SEC) {
                long secs = readIntLE() & 0xFFFFFFFFL;
                pendingExpireAt = secs * 1000L;
            } else {
                // opcode here is actually the value-type byte for a key/value pair
                int valueType = opcode;
                String key = readString();
                Object value = readValue(valueType);
                long expireAt = pendingExpireAt != null ? pendingExpireAt : -1;
                pendingExpireAt = null;

                if (expireAt >= 0 && expireAt <= System.currentTimeMillis()) {
                    // already expired - do not load it
                    continue;
                }
                if (value instanceof String s) {
                    db.setString(key, s, expireAt);
                }
                // non-string types are parsed (to keep the stream position correct)
                // but not materialized, since the challenge only requires strings.
            }
        }
    }

    /** Reads and (mostly) decodes a value of the given RDB value-type. */
    private Object readValue(int valueType) {
        switch (valueType) {
            case 0x00: // string
                return readString();
            case 0x01: // list (legacy linked list) - length-prefixed strings
            case 0x02: // set
            {
                long len = readLength();
                for (long i = 0; i < len; i++) readString();
                return null;
            }
            case 0x03: // sorted set (legacy)
            {
                long len = readLength();
                for (long i = 0; i < len; i++) {
                    readString();
                    readString(); // score as string in legacy encoding
                }
                return null;
            }
            case 0x04: // hash
            {
                long len = readLength();
                for (long i = 0; i < len; i++) {
                    readString();
                    readString();
                }
                return null;
            }
            default:
                // Unsupported/encoded (ziplist, listpack, intset, quicklist, etc.)
                // We cannot safely skip these without full decoders, so stop
                // parsing further entries gracefully.
                pos = data.length;
                return null;
        }
    }

    private int readByteUnsigned() {
        return data[pos++] & 0xFF;
    }

    private long readLongLE() {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v |= ((long) (data[pos + i] & 0xFF)) << (8 * i);
        }
        pos += 8;
        return v;
    }

    private int readIntLE() {
        int v = 0;
        for (int i = 0; i < 4; i++) {
            v |= (data[pos + i] & 0xFF) << (8 * i);
        }
        pos += 4;
        return v;
    }

    /** RDB length encoding; returns the length, handling the special integer-encoding cases by returning -1
     *  after populating lastSpecialInt (not used here since we route those through readString). */
    private long readLength() {
        int first = readByteUnsigned();
        int type = (first & 0xC0) >> 6;
        if (type == 0) {
            return first & 0x3F;
        } else if (type == 1) {
            int second = readByteUnsigned();
            return ((first & 0x3F) << 8) | second;
        } else if (type == 2) {
            // 32-bit length, big-endian
            long v = 0;
            for (int i = 0; i < 4; i++) {
                v = (v << 8) | readByteUnsigned();
            }
            return v;
        } else {
            // type == 3: special encoding, caller (readString) needs to know which
            specialEncoding = first & 0x3F;
            return -1;
        }
    }

    private int specialEncoding = -1;

    private String readString() {
        long len = readLength();
        if (len == -1) {
            // special integer or compressed encoding
            switch (specialEncoding) {
                case 0: { // 8 bit int
                    int v = data[pos++];
                    return Integer.toString(v);
                }
                case 1: { // 16 bit int LE
                    int v = (data[pos] & 0xFF) | (data[pos + 1] << 8);
                    pos += 2;
                    return Integer.toString((short) v);
                }
                case 2: { // 32 bit int LE
                    int v = (data[pos] & 0xFF) | ((data[pos + 1] & 0xFF) << 8)
                            | ((data[pos + 2] & 0xFF) << 16) | ((data[pos + 3] & 0xFF) << 24);
                    pos += 4;
                    return Integer.toString(v);
                }
                case 3: { // LZF compressed string - not supported; best effort skip
                    long clen = readLength();
                    long ulen = readLength();
                    pos += clen; // skip compressed bytes; content unavailable
                    return "";
                }
                default:
                    return "";
            }
        }
        byte[] buf = new byte[(int) len];
        System.arraycopy(data, pos, buf, 0, (int) len);
        pos += (int) len;
        return new String(buf, StandardCharsets.UTF_8);
    }
}
