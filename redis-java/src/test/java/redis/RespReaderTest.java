package redis;

import org.junit.jupiter.api.Test;
import redis.protocol.RespReader;
import redis.protocol.RespWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class RespReaderTest {

    private static RespReader reader(byte[] bytes) {
        return new RespReader(new ByteArrayInputStream(bytes));
    }

    @Test
    void parsesArrayOfBulkStrings() throws IOException {
        String[] cmd = reader("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".getBytes(StandardCharsets.US_ASCII)).readCommand();
        assertArrayEquals(new String[]{"SET", "foo", "bar"}, cmd);
    }

    @Test
    void bulkStringMayContainCrlf() throws IOException {
        String[] cmd = reader("*2\r\n$4\r\nECHO\r\n$4\r\na\r\nb\r\n".getBytes(StandardCharsets.US_ASCII)).readCommand();
        assertEquals("a\r\nb", cmd[1]);
    }

    @Test
    void parsesInlineCommand() throws IOException {
        assertArrayEquals(new String[]{"PING"}, reader("PING\r\n".getBytes(StandardCharsets.US_ASCII)).readCommand());
    }

    @Test
    void handlesFramesSplitAcrossReads() throws IOException {
        byte[] frame = "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n".getBytes(StandardCharsets.US_ASCII);
        // An InputStream that hands back at most one byte per read(), like a slow TCP connection
        InputStream trickle = new ByteArrayInputStream(frame) {
            @Override
            public synchronized int read(byte[] b, int off, int len) {
                return super.read(b, off, Math.min(len, 1));
            }
        };
        assertArrayEquals(new String[]{"ECHO", "hello"}, new RespReader(trickle).readCommand());
    }

    @Test
    void everyByteValueRoundTripsUnchanged() throws IOException {
        byte[] value = new byte[256];
        for (int i = 0; i < 256; i++) value[i] = (byte) i;

        ByteArrayOutputStream frame = new ByteArrayOutputStream();
        frame.writeBytes("*2\r\n$4\r\nECHO\r\n$256\r\n".getBytes(StandardCharsets.US_ASCII));
        frame.writeBytes(value);
        frame.writeBytes("\r\n".getBytes(StandardCharsets.US_ASCII));

        String parsed = reader(frame.toByteArray()).readCommand()[1];
        byte[] encoded = RespWriter.encodeBulkString(parsed);
        byte[] expectedHeader = "$256\r\n".getBytes(StandardCharsets.US_ASCII);
        for (int i = 0; i < expectedHeader.length; i++) assertEquals(expectedHeader[i], encoded[i]);
        for (int i = 0; i < 256; i++) assertEquals(value[i], encoded[expectedHeader.length + i], "byte " + i);
    }

    @Test
    void readsRawRdbPayloadWithoutTrailingCrlf() throws IOException {
        byte[] bytes = "$5\r\nREDIS*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII);
        RespReader r = reader(bytes);
        assertArrayEquals("REDIS".getBytes(StandardCharsets.US_ASCII), r.readRawBulkPayload());
        assertArrayEquals(new String[]{"PING"}, r.readCommand(), "next command is intact");
    }
}
