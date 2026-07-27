package com.oddsockets;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.oddsockets.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Channel class for pub/sub messaging.
 *
 * Provides methods for subscribing, publishing, and managing presence on a
 * specific channel. All operations travel over the real Socket.IO connection
 * to the assigned OddSockets worker.
 *
 * @author Joe Wee
 * @since 0.1.0
 */
public class Channel {

    private static final Logger logger = LoggerFactory.getLogger(Channel.class);
    private static final long REQUEST_TIMEOUT_MS = 15000;

    private final String name;
    private final OddSockets client;
    private final AtomicBoolean subscribed;
    private final AtomicBoolean subscribing;
    private final List<Message> messageHistory;
    private final int maxHistorySize;

    private volatile Consumer<Map<String, Object>> messageHandler;
    private volatile SubscribeOptions subscribeOptions;

    /**
     * Creates a new channel instance.
     *
     * @param name   the channel name
     * @param client the OddSockets client
     */
    public Channel(String name, OddSockets client) {
        this.name = name;
        this.client = client;
        this.subscribed = new AtomicBoolean(false);
        this.subscribing = new AtomicBoolean(false);
        this.messageHistory = new CopyOnWriteArrayList<>();
        this.maxHistorySize = 100;
    }

    private WebSocketConnection socket() {
        WebSocketConnection socket = client.getSocket();
        if (socket == null) {
            throw new IllegalStateException("No socket connection available");
        }
        return socket;
    }

    private Predicate<JsonObject> channelMatch() {
        return o -> name.equals(getString(o, "channel"));
    }

    /**
     * Subscribe to the channel.
     *
     * @param callback message callback function
     * @param options  subscription options (can be null)
     * @return future that completes when subscribed
     */
    public CompletableFuture<Void> subscribe(Consumer<Map<String, Object>> callback, SubscribeOptions options) {
        if (callback == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Callback function is required"));
        }

        if (subscribed.get() || subscribing.get()) {
            this.messageHandler = callback;
            return CompletableFuture.completedFuture(null);
        }

        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client is not connected"));
        }

        subscribing.set(true);
        this.subscribeOptions = options != null ? options : new SubscribeOptions();

        return CompletableFuture.supplyAsync(() -> {
            try {
                JsonObject data = new JsonObject();
                data.addProperty("channel", name);
                data.add("options", subscribeOptionsPayload(this.subscribeOptions));

                socket().request("subscribe", data, "subscribed", channelMatch(), REQUEST_TIMEOUT_MS);

                this.messageHandler = callback;
                subscribed.set(true);
                subscribing.set(false);

                logger.info("Subscribed to channel: {}", name);
                return null;
            } catch (Exception e) {
                subscribing.set(false);
                logger.error("Failed to subscribe to channel {}: {}", name, e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Subscribe to the channel with default options.
     *
     * @param callback message callback function
     * @return future that completes when subscribed
     */
    public CompletableFuture<Void> subscribe(Consumer<Map<String, Object>> callback) {
        return subscribe(callback, null);
    }

    /**
     * Unsubscribe from the channel.
     *
     * @return future that completes when unsubscribed
     */
    public CompletableFuture<Void> unsubscribe() {
        if (!subscribed.get()) {
            return CompletableFuture.completedFuture(null);
        }
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client is not connected"));
        }

        return CompletableFuture.runAsync(() -> {
            try {
                JsonObject data = new JsonObject();
                data.addProperty("channel", name);

                socket().request("unsubscribe", data, "unsubscribed", channelMatch(), REQUEST_TIMEOUT_MS);

                subscribed.set(false);
                messageHandler = null;
                subscribeOptions = null;

                logger.info("Unsubscribed from channel: {}", name);
            } catch (Exception e) {
                logger.error("Failed to unsubscribe from channel {}: {}", name, e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Publish a message to this channel.
     *
     * @param message the message to publish
     * @param options the publish options (can be null)
     * @return future with the publish result
     */
    public CompletableFuture<OddSockets.PublishResult> publish(Object message, PublishOptions options) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client is not connected"));
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                MessageSizeValidator.validateMessageSize(message);

                JsonObject data = new JsonObject();
                data.addProperty("channel", name);
                data.add("message", client.getGson().toJsonTree(message));
                if (options != null) {
                    JsonObject opts = new JsonObject();
                    opts.addProperty("ttl", options.getTtl());
                    if (options.getMetadata() != null) {
                        opts.add("metadata", client.getGson().toJsonTree(options.getMetadata()));
                    }
                    data.add("options", opts);
                }

                JsonObject resp = socket().request("publish", data, "published", channelMatch(), REQUEST_TIMEOUT_MS);

                String messageId = getString(resp, "messageId");
                Long timestamp = resp.has("timestamp")
                        && resp.get("timestamp").isJsonPrimitive()
                        && resp.get("timestamp").getAsJsonPrimitive().isNumber()
                        ? resp.get("timestamp").getAsLong() : System.currentTimeMillis();

                logger.debug("Published message to channel '{}': {}", name, messageId);
                return new OddSockets.PublishResult(messageId, timestamp, name, true, null);
            } catch (Exception e) {
                logger.error("Failed to publish to channel {}: {}", name, e.getMessage());
                return new OddSockets.PublishResult(null, null, name, false, e.getMessage());
            }
        });
    }

    /**
     * Publish a message with default options.
     *
     * @param message the message to publish
     * @return future with the publish result
     */
    public CompletableFuture<OddSockets.PublishResult> publish(Object message) {
        return publish(message, null);
    }

    /**
     * Get message history for the channel.
     *
     * @param options history options (can be null)
     * @return future with the message history
     */
    public CompletableFuture<List<Map<String, Object>>> getHistory(HistoryOptions options) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client is not connected"));
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                JsonObject data = new JsonObject();
                data.addProperty("channel", name);
                data.addProperty("count", options != null ? options.getCount() : 50);
                if (options != null && options.getStart() != null) {
                    data.addProperty("start", options.getStart());
                }
                if (options != null && options.getEnd() != null) {
                    data.addProperty("end", options.getEnd());
                }

                // The worker emits "history" both as the explicit get_history
                // RESPONSE (query:true) and as a fire-and-forget on-join snapshot
                // (~10 msgs, no query flag). Gate resolution on query==true so the
                // snapshot can't satisfy this request with the wrong data.
                // BUG-2026-0727-0012.
                Predicate<JsonObject> historyMatch = channelMatch().and(
                        o -> o.has("query") && o.get("query").isJsonPrimitive()
                                && o.getAsJsonPrimitive("query").isBoolean()
                                && o.getAsJsonPrimitive("query").getAsBoolean());
                JsonObject resp = socket().request("get_history", data, "history", historyMatch, REQUEST_TIMEOUT_MS);

                List<Map<String, Object>> messages = new ArrayList<>();
                if (resp.has("messages") && resp.get("messages").isJsonArray()) {
                    for (JsonElement el : resp.getAsJsonArray("messages")) {
                        if (el.isJsonObject()) {
                            messages.add(client.getGson().fromJson(el, Map.class));
                        }
                    }
                }

                logger.debug("Retrieved {} messages from channel '{}' history", messages.size(), name);
                return messages;
            } catch (Exception e) {
                logger.error("Failed to get history for channel {}: {}", name, e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Get message history with default options.
     *
     * @return future with the message history
     */
    public CompletableFuture<List<Map<String, Object>>> getHistory() {
        return getHistory(null);
    }

    /**
     * Get current presence information.
     *
     * @return future with the presence information
     */
    public CompletableFuture<Map<String, Object>> getPresence() {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client is not connected"));
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                JsonObject data = new JsonObject();
                data.addProperty("channel", name);

                JsonObject resp = socket().request("get_presence", data, "presence", channelMatch(), REQUEST_TIMEOUT_MS);

                @SuppressWarnings("unchecked")
                Map<String, Object> presenceInfo = client.getGson().fromJson(resp, Map.class);
                logger.debug("Retrieved presence for channel '{}'", name);
                return presenceInfo;
            } catch (Exception e) {
                logger.error("Failed to get presence for channel {}: {}", name, e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Update user state on this channel.
     *
     * @param state user state object
     * @return future that completes when the update is sent
     */
    public CompletableFuture<Void> updateState(Map<String, Object> state) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client is not connected"));
        }

        return CompletableFuture.runAsync(() -> {
            JsonObject data = new JsonObject();
            data.addProperty("channel", name);
            data.add("state", client.getGson().toJsonTree(state));
            client.emit("update_state", data);
            logger.debug("Updated state for channel: {}", name);
        });
    }

    /**
     * Check if the channel is subscribed.
     *
     * @return true if subscribed, false otherwise
     */
    public boolean isSubscribed() {
        return subscribed.get();
    }

    /**
     * Get the channel name.
     *
     * @return the channel name
     */
    public String getName() {
        return name;
    }

    /**
     * Get cached message history.
     *
     * @return copy of cached messages
     */
    public List<Message> getCachedHistory() {
        return List.copyOf(messageHistory);
    }

    /**
     * Internal: Handle an incoming message broadcast (called by OddSockets).
     *
     * @param envelope the broadcast envelope
     */
    void handleMessage(Map<String, Object> envelope) {
        try {
            if (subscribeOptions != null && subscribeOptions.isRetainHistory()) {
                messageHistory.add(envelopeToMessage(envelope));
                if (messageHistory.size() > maxHistorySize) {
                    messageHistory.remove(0);
                }
            }

            Consumer<Map<String, Object>> handler = this.messageHandler;
            if (handler != null) {
                handler.accept(envelope);
            }
        } catch (Exception e) {
            logger.error("Error handling message for channel {}: {}", name, e.getMessage());
        }
    }

    private Message envelopeToMessage(Map<String, Object> envelope) {
        String id = envelope.get("id") != null ? String.valueOf(envelope.get("id")) : null;
        Object data = envelope.get("message");

        String userId = null;
        Object publisher = envelope.get("publisher");
        if (publisher instanceof Map<?, ?> pub) {
            Object uid = pub.get("userId");
            userId = uid != null ? String.valueOf(uid) : null;
        }

        Instant timestamp = Instant.now();
        Object ts = envelope.get("timestamp");
        if (ts instanceof Number number) {
            timestamp = Instant.ofEpochMilli(number.longValue());
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> metadata = envelope.get("metadata") instanceof Map
                ? (Map<String, Object>) envelope.get("metadata") : null;

        return new Message(id, name, data, timestamp, userId, metadata);
    }

    private static String getString(JsonObject o, String key) {
        if (o != null && o.has(key) && o.get(key).isJsonPrimitive()) {
            return o.get(key).getAsString();
        }
        return null;
    }

    private static JsonObject subscribeOptionsPayload(SubscribeOptions options) {
        JsonObject opts = new JsonObject();
        opts.addProperty("maxHistory", options.getMaxHistory());
        opts.addProperty("retainHistory", options.isRetainHistory());
        opts.addProperty("enablePresence", options.isEnablePresence());
        return opts;
    }

    @Override
    public String toString() {
        return "Channel{name='" + name + "', subscribed=" + subscribed.get()
                + ", messageHistory=" + messageHistory.size() + '}';
    }

    /**
     * Options for subscribing to a channel.
     */
    public static class SubscribeOptions {
        private int maxHistory = 100;
        private boolean retainHistory = true;
        private boolean enablePresence = false;

        public SubscribeOptions() {}

        public static Builder builder() {
            return new Builder();
        }

        public int getMaxHistory() { return maxHistory; }
        public void setMaxHistory(int maxHistory) { this.maxHistory = maxHistory; }
        public boolean isRetainHistory() { return retainHistory; }
        public void setRetainHistory(boolean retainHistory) { this.retainHistory = retainHistory; }
        public boolean isEnablePresence() { return enablePresence; }
        public void setEnablePresence(boolean enablePresence) { this.enablePresence = enablePresence; }

        public static class Builder {
            private final SubscribeOptions options = new SubscribeOptions();

            public Builder maxHistory(int maxHistory) {
                options.setMaxHistory(maxHistory);
                return this;
            }

            public Builder retainHistory(boolean retainHistory) {
                options.setRetainHistory(retainHistory);
                return this;
            }

            public Builder enablePresence(boolean enablePresence) {
                options.setEnablePresence(enablePresence);
                return this;
            }

            public SubscribeOptions build() {
                return options;
            }
        }
    }

    /**
     * Options for publishing messages.
     */
    public static class PublishOptions {
        private int ttl;
        private Map<String, Object> metadata;

        public PublishOptions() {}

        public static Builder builder() {
            return new Builder();
        }

        public int getTtl() { return ttl; }
        public void setTtl(int ttl) { this.ttl = ttl; }
        public Map<String, Object> getMetadata() { return metadata; }
        public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }

        public static class Builder {
            private final PublishOptions options = new PublishOptions();

            public Builder ttl(int ttl) {
                options.setTtl(ttl);
                return this;
            }

            public Builder metadata(Map<String, Object> metadata) {
                options.setMetadata(metadata);
                return this;
            }

            public PublishOptions build() {
                return options;
            }
        }
    }

    /**
     * Options for retrieving message history.
     */
    public static class HistoryOptions {
        private int count = 50;
        private String start;
        private String end;
        private boolean reverse = false;

        public HistoryOptions() {}

        public static Builder builder() {
            return new Builder();
        }

        public int getCount() { return count; }
        public void setCount(int count) { this.count = count; }
        public String getStart() { return start; }
        public void setStart(String start) { this.start = start; }
        public String getEnd() { return end; }
        public void setEnd(String end) { this.end = end; }
        public boolean isReverse() { return reverse; }
        public void setReverse(boolean reverse) { this.reverse = reverse; }

        public static class Builder {
            private final HistoryOptions options = new HistoryOptions();

            public Builder count(int count) {
                options.setCount(count);
                return this;
            }

            public Builder start(String start) {
                options.setStart(start);
                return this;
            }

            public Builder end(String end) {
                options.setEnd(end);
                return this;
            }

            public Builder reverse(boolean reverse) {
                options.setReverse(reverse);
                return this;
            }

            public HistoryOptions build() {
                return options;
            }
        }
    }
}
