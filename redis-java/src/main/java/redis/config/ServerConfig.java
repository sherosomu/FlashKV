package redis.config;

/**
 * Holds all configuration derived from command line arguments, e.g.:
 *   --port 6380
 *   --dir /tmp/redis-data --dbfilename dump.rdb
 *   --replicaof "localhost 6379"
 */
public class ServerConfig {

    private int port = 6379;
    private String dir = "/tmp";
    private String dbfilename = "dump.rdb";

    // Replication
    private boolean replica = false;
    private String masterHost;
    private int masterPort;

    // Replication identity (used whether we are master or not, needed for INFO/PSYNC)
    private final String replicationId = randomReplId();
    private volatile long masterReplOffset = 0L;

    public static ServerConfig fromArgs(String[] args) {
        ServerConfig config = new ServerConfig();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--port" -> config.port = Integer.parseInt(args[++i]);
                case "--dir" -> config.dir = args[++i];
                case "--dbfilename" -> config.dbfilename = args[++i];
                case "--replicaof" -> {
                    String value = args[++i];
                    // codecrafters passes host and port as two separate args OR one quoted string
                    String host;
                    String portStr;
                    if (value.contains(" ")) {
                        String[] parts = value.split("\\s+");
                        host = parts[0];
                        portStr = parts[1];
                    } else {
                        host = value;
                        portStr = args[++i];
                    }
                    config.replica = true;
                    config.masterHost = host;
                    config.masterPort = Integer.parseInt(portStr);
                }
                default -> {
                    // ignore unknown flags to be forgiving
                }
            }
        }
        return config;
    }

    private static String randomReplId() {
        String chars = "0123456789abcdef";
        StringBuilder sb = new StringBuilder(40);
        java.util.Random r = new java.util.Random();
        for (int i = 0; i < 40; i++) {
            sb.append(chars.charAt(r.nextInt(chars.length())));
        }
        return sb.toString();
    }

    public int getPort() {
        return port;
    }

    public String getDir() {
        return dir;
    }

    public String getDbfilename() {
        return dbfilename;
    }

    public String getRdbPath() {
        return dir + java.io.File.separator + dbfilename;
    }

    public boolean isReplica() {
        return replica;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public String getReplicationId() {
        return replicationId;
    }

    public long getMasterReplOffset() {
        return masterReplOffset;
    }

    public synchronized void addToReplOffset(long delta) {
        masterReplOffset += delta;
    }

    public synchronized void setMasterReplOffset(long value) {
        masterReplOffset = value;
    }
}
