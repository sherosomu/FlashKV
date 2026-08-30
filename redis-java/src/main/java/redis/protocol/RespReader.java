package redis.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Reads RESP (REdis Serialization Protocol) data off a raw InputStream.
 *
 * Supports:
 *  - RESP arrays of bulk strings (the standard way clients send commands, and
 *    the way a master propagates commands to replicas)
 *  - A minimal "inline command" fallback (plain text line) for tools like
 *    `nc` / `telnet` that don't speak RESP arrays
 *  - Raw bulk payload reads (used once, right after PSYNC, to read the RDB
 *    file bytes the master sends which are NOT terminated with \r\n)
 */
public class RespReader {

    private final InputStream in;

    public RespReader(InputStream in) {
        this.in = in;
    }

    /**
     * Reads a single command as an array of Strings. Returns null on clean EOF.
     */
    public String[] readCommand() throws IOException {
        int first = in.read();
        if (first == -1) {
            return null;
        }
        if (first != '*') {
            // Fallback: treat as inline command until newline.
            String rest = readLineRaw();
            String line = ((char) first) + (rest == null ? "" : rest);
            line = line.trim();
            if (line.isEmpty()) {
                return new String[0];
            }
            return line.split("\\s+");
        }

        int count = Integer.parseInt(readLineRaw().trim());
        if (count <= 0) {
            return new String[0];
        }
        String[] args = new String[count];
        for (int i = 0; i < count; i++) {
            int marker = in.read();
            if (marker == -1) {
                throw new IOException("Unexpected EOF reading bulk string header");
            }
            if (marker != '$') {
                throw new IOException("Protocol error: expected '$', got '" + (char) marker + "'");
            }
            int len = Integer.parseInt(readLineRaw().trim());
            byte[] buf = new byte[len];
            readFully(buf);
            // consume trailing CRLF after the bulk string content
            readCRLF();
            args[i] = new String(buf, StandardCharsets.UTF_8);
        }
        return args;
    }

    /**
     * Reads a raw bulk payload of the form "$<len>\r\n<len bytes, no trailing CRLF>".
     * This exact framing is what a Redis master sends for the RDB file right
     * after a FULLRESYNC response to PSYNC.
     */
    public byte[] readRawBulkPayload() throws IOException {
        int marker = in.read();
        if (marker == -1) {
            throw new IOException("EOF waiting for RDB bulk payload");
        }
        if (marker != '$') {
            throw new IOException("Expected '$' for RDB bulk payload, got '" + (char) marker + "'");
        }
        int len = Integer.parseInt(readLineRaw().trim());
        byte[] buf = new byte[len];
        readFully(buf);
        return buf;
    }

    /** Reads one line up to (and consuming) the terminating \r\n, without the CRLF. */
    private String readLineRaw() throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\r') {
                int next = in.read(); // consume \n
                if (next != '\n' && next != -1) {
                    sb.append((char) c).append((char) next);
                    continue;
                }
                return sb.toString();
            }
            sb.append((char) c);
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    private void readFully(byte[] buf) throws IOException {
        int total = 0;
        while (total < buf.length) {
            int n = in.read(buf, total, buf.length - total);
            if (n == -1) {
                throw new IOException("Unexpected EOF while reading bulk string body");
            }
            total += n;
        }
    }

    private void readCRLF() throws IOException {
        int cr = in.read();
        int lf = in.read();
        if (cr != '\r' || lf != '\n') {
            throw new IOException("Protocol error: expected CRLF");
        }
    }
}
