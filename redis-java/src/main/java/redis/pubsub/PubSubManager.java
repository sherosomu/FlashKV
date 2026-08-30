package redis.pubsub;

import redis.protocol.RespWriter;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Tracks channel subscriptions and delivers PUBLISH messages to subscribers.
 */
public class PubSubManager {

    public interface Subscriber {
        void deliverMessage(String channel, String payload) throws IOException;
    }

    private final ConcurrentHashMap<String, Set<Subscriber>> channels = new ConcurrentHashMap<>();

    public void subscribe(String channel, Subscriber subscriber) {
        channels.computeIfAbsent(channel, c -> new CopyOnWriteArraySet<>()).add(subscriber);
    }

    public void unsubscribe(String channel, Subscriber subscriber) {
        Set<Subscriber> subs = channels.get(channel);
        if (subs != null) {
            subs.remove(subscriber);
            if (subs.isEmpty()) {
                channels.remove(channel, subs);
            }
        }
    }

    public void unsubscribeAll(Subscriber subscriber) {
        for (String channel : channels.keySet()) {
            unsubscribe(channel, subscriber);
        }
    }

    /** Publishes a message, returning the number of subscribers it was delivered to. */
    public int publish(String channel, String message) {
        Set<Subscriber> subs = channels.get(channel);
        if (subs == null || subs.isEmpty()) {
            return 0;
        }
        int count = 0;
        for (Subscriber s : subs) {
            try {
                s.deliverMessage(channel, message);
                count++;
            } catch (IOException ignored) {
                // dead connection; will be cleaned up when its handler loop exits
            }
        }
        return count;
    }

    public static byte[] encodeMessage(String channel, String payload) {
        return RespWriter.encodeCommandArray("message", channel, payload);
    }
}
