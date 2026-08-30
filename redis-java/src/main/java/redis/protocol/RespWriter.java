package redis.protocol;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Encodes and writes RESP values to an OutputStream. Also exposes static
 * helpers that build raw RESP byte arrays, which is useful for building the
 * exact bytes propagated from a master to its replicas.
 */
public class RespWriter {

    private final OutputStream out;

    public RespWriter(OutputStream out) {
        this.out = out;
    }

    public synchronized void writeSimpleString(String s) throws IOException {
        out.write(("+" + s + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    public synchronized void writeError(String message) throws IOException {
        out.write(("-" + message + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    public synchronized void writeInteger(long value) throws IOException {
        out.write((":" + value + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    public synchronized void writeBulkString(String s) throws IOException {
        out.write(encodeBulkString(s));
        out.flush();
    }

    public synchronized void writeNullBulkString() throws IOException {
        out.write("$-1\r\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    public synchronized void writeNullArray() throws IOException {
        out.write("*-1\r\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    public synchronized void writeEmptyArray() throws IOException {
        out.write("*0\r\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    public synchronized void writeStringArray(List<String> items) throws IOException {
        StringBuilder header = new StringBuilder();
        header.append('*').append(items.size()).append("\r\n");
        out.write(header.toString().getBytes(StandardCharsets.UTF_8));
        for (String item : items) {
            out.write(encodeBulkString(item));
        }
        out.flush();
    }

    /** Writes a raw, already-encoded RESP array (used for nested/complex replies). */
    public synchronized void writeRaw(byte[] raw) throws IOException {
        out.write(raw);
        out.flush();
    }

    public synchronized void writeRawNoFlush(byte[] raw) throws IOException {
        out.write(raw);
    }

    public synchronized void flush() throws IOException {
        out.flush();
    }

    // ---------- static encoding helpers (no I/O) ----------

    public static byte[] encodeBulkString(String s) {
        if (s == null) {
            return "$-1\r\n".getBytes(StandardCharsets.UTF_8);
        }
        byte[] data = s.getBytes(StandardCharsets.UTF_8);
        String header = "$" + data.length + "\r\n";
        byte[] headerBytes = header.getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[headerBytes.length + data.length + 2];
        System.arraycopy(headerBytes, 0, result, 0, headerBytes.length);
        System.arraycopy(data, 0, result, headerBytes.length, data.length);
        result[result.length - 2] = '\r';
        result[result.length - 1] = '\n';
        return result;
    }

    public static byte[] encodeSimpleString(String s) {
        return ("+" + s + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] encodeInteger(long v) {
        return (":" + v + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    /** Encodes a command as a RESP array of bulk strings, e.g. for propagation to replicas. */
    public static byte[] encodeCommandArray(String... parts) {
        StringBuilder header = new StringBuilder();
        header.append('*').append(parts.length).append("\r\n");
        byte[] headerBytes = header.toString().getBytes(StandardCharsets.UTF_8);

        int totalLen = headerBytes.length;
        byte[][] encodedParts = new byte[parts.length][];
        for (int i = 0; i < parts.length; i++) {
            encodedParts[i] = encodeBulkString(parts[i]);
            totalLen += encodedParts[i].length;
        }
        byte[] result = new byte[totalLen];
        int pos = 0;
        System.arraycopy(headerBytes, 0, result, pos, headerBytes.length);
        pos += headerBytes.length;
        for (byte[] part : encodedParts) {
            System.arraycopy(part, 0, result, pos, part.length);
            pos += part.length;
        }
        return result;
    }

    public static byte[] encodeCommandArray(List<String> parts) {
        return encodeCommandArray(parts.toArray(new String[0]));
    }
}
