package redis.rdb;

import java.util.Base64;

/**
 * The canonical "empty" RDB file (base64-encoded), the same bytes real Redis
 * sends on a full resync when there is nothing to persist yet. Used by the
 * master side of replication when responding to PSYNC.
 */
public final class EmptyRdb {

    private static final String EMPTY_RDB_BASE64 =
            "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wOh5BiV/v";

    private EmptyRdb() {
    }

    public static byte[] bytes() {
        return Base64.getDecoder().decode(EMPTY_RDB_BASE64);
    }
}
