package redis;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Minimal RESP client for tests: sends byte[] args, returns the decoded reply. */
final class TestClient implements AutoCloseable {

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;

    TestClient(int port) throws IOException {
        socket = new Socket("localhost", port);
        socket.setSoTimeout(10_000);
        in = new BufferedInputStream(socket.getInputStream());
        out = socket.getOutputStream();
    }

    /** Sends a command; String args are sent as UTF-8. Returns String, Long, byte[] (bulk), List, or null. */
    Object call(Object... args) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.writeBytes(("*" + args.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
        for (Object a : args) {
            byte[] data = a instanceof byte[] bytes ? bytes : a.toString().getBytes(StandardCharsets.UTF_8);
            b.writeBytes(("$" + data.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
            b.writeBytes(data);
            b.writeBytes("\r\n".getBytes(StandardCharsets.US_ASCII));
        }
        out.write(b.toByteArray());
        out.flush();
        return readReply();
    }

    /** Like call, but bulk replies come back as UTF-8 Strings. */
    Object callStr(Object... args) throws IOException {
        return toStrings(call(args));
    }

    private static Object toStrings(Object o) {
        if (o instanceof byte[] bytes) return new String(bytes, StandardCharsets.UTF_8);
        if (o instanceof List<?> list) {
            List<Object> r = new ArrayList<>();
            for (Object x : list) r.add(toStrings(x));
            return r;
        }
        return o;
    }

    private Object readReply() throws IOException {
        int type = in.read();
        String line = readLine();
        switch (type) {
            case '+': return line;
            case '-': return new RuntimeException(line);
            case ':': return Long.parseLong(line);
            case '$': {
                int len = Integer.parseInt(line);
                if (len < 0) return null;
                byte[] data = in.readNBytes(len);
                in.readNBytes(2);
                return data;
            }
            case '*': {
                int n = Integer.parseInt(line);
                if (n < 0) return null;
                List<Object> items = new ArrayList<>();
                for (int i = 0; i < n; i++) items.add(readReply());
                return items;
            }
            default: throw new IOException("bad reply type " + (char) type);
        }
    }

    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != '\r') {
            if (c == -1) throw new IOException("EOF");
            sb.append((char) c);
        }
        in.read(); // \n
        return sb.toString();
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
