package redis.protocol;

import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps an InputStream and keeps a running total of bytes read.
 * Used on the replica side to compute the replication offset, since the
 * offset must reflect exactly how many bytes of the master's command
 * stream have been processed.
 */
public class CountingInputStream extends InputStream {

    private final InputStream delegate;
    private volatile long bytesRead = 0;

    public CountingInputStream(InputStream delegate) {
        this.delegate = delegate;
    }

    @Override
    public int read() throws IOException {
        int b = delegate.read();
        if (b != -1) {
            bytesRead++;
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = delegate.read(b, off, len);
        if (n > 0) {
            bytesRead += n;
        }
        return n;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public void resetCount() {
        bytesRead = 0;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
