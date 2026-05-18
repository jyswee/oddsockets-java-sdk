package com.oddsockets;

import com.oddsockets.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Channel class for pub/sub messaging
 * 
 * Provides methods for subscribing, publishing, and managing presence
 * on a specific channel within the OddSockets platform.
 * 
 * This implementation matches the JavaScript SDK pattern for consistency.
 * 
 * @author Joe Wee
 * @since 0.1.0
 */
public class Channel {
    
    private static final Logger logger = LoggerFactory.getLogger(Channel.class);
    
    private final String name;
    private final OddSockets client;
    private final AtomicBoolean subscribed;
    private final AtomicBoolean subscribing;
    private final List<Message> messageHistory;
    private final Map<String, Object> presence;
    private final int maxHistorySize;
    
    private volatile Consumer<Map<String, Object>> messageHandler;
    private volatile SubscribeOptions subscribeOptions;
    
    /**
     * Creates a new channel instance.
     * 
     * @param name the channel name
     * @param client the OddSockets client
     */
    public Channel(String name, OddSockets client) {
        this.name = name;
        this.client = client;
        this.subscribed = new AtomicBoolean(false);
        this.subscribing = new AtomicBoolean(false);
        this.messageHistory = new CopyOnWriteArrayList<>();
        this.presence = new ConcurrentHashMap<>();
        this.maxHistorySize = 100;
    }
    
    /**
     * Subscribe to the channel
     * 
     * @param callback Message callback function
     * @param options Subscription options (can be null)
     * @return CompletableFuture that completes when subscribed
     */
    public CompletableFuture<Void> subscribe(Consumer<Map<String, Object>> callback, SubscribeOptions options) {
        if (callback == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Callback function is required"));
        }
        
        if (subscribed.get() || subscribing.get()) {
            // Add callback to existing subscription
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
                WebSocketConnection socket = client.getSocket();
                if (socket == null) {
                    throw new IllegalStateException("No socket connection available");
                }
                
                // Send subscription request
                Map<String, Object> subscribeData = Map.of(
                    "channel", name,
                    "options", Map.of(
                        "maxHistory", this.subscribeOptions.getMaxHistory(),
                        "retainHistory", this.subscribeOptions.isRetainHistory(),
                        "enablePresence", this.subscribeOptions.isEnablePresence()
                    )
                );
                
                socket.emit("subscribe", subscribeData);
                
                // Wait for subscription confirmation (simulated)
                Thread.sleep(100);
                
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
     * Subscribe to the channel with default options
     * 
     * @param callback Message callback function
     * @return CompletableFuture that completes when subscribed
     */
    public CompletableFuture<Void> subscribe(Consumer<Map<String, Object>> callback) {
        return subscribe(callback, null);
    }
    
    /**
     * Unsubscribe from the channel
     * 
     * @return CompletableFuture that completes when unsubscribed
     */
    public CompletableFuture<Void> unsubscribe() {
        if (!subscribed.get()) {
            logger.debug("Channel '{}' not subscribed", name);
            return CompletableFuture.completedFuture(null);
        }
        
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client is not connected"));
        }
        
        return CompletableFuture.runAsync(() -> {
            try {
                WebSocketConnection socket = client.getSocket();
                if (socket == null) {
                    throw new IllegalStateException("No socket connection available");
                }
                
                // Send unsubscription request
                Map<String, Object> unsubscribeData = Map.of("channel", name);
                socket.emit("unsubscribe", unsubscribeData);
                
                // Wait for unsubscription confirmation (simulated)
                Thread.sleep(100);
                
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
     * Publish a message to this channel
     * 
     * @param message the message to publish
     * @param options the publish options (can be null)
     * @return CompletableFuture with the publish result
     */
    public CompletableFuture<OddSockets.PublishResult> publish(Object message, PublishOptions options) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client is not connected"));
        }
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Validate message size before publishing
                MessageSizeValidator.validateMessageSize(message);
                
                WebSocketConnection socket = client.getSocket();
                if (socket == null) {
                    throw new IllegalStateException("No socket connection available");
                }
                
                // Create message ID and timestamp
                String messageId = "msg_" + UUID.randomUUID().toString().substring(0, 12);
                long timestamp = System.currentTimeMillis();
                
                // Send publish request
                Map<String, Object> publishData = Map.of(
                    "channel", name,
                    "message", message,
                    "options", options != null ? Map.of(
                        "ttl", options.getTtl(),
                        "metadata", options.getMetadata() != null ? options.getMetadata() : Map.of()
                    ) : Map.of()
                );
                
                socket.emit("publish", publishData);
                
                // Wait for publish confirmation (simulated)
                Thread.sleep(50);
                
                logger.debug("Published message to channel '{}': {}", name, message);
                
                return new OddSockets.PublishResult(
                    messageId,
                    timestamp,
                    name,
                    true,
                    null
                );
                
            } catch (Exception e) {
                logger.error("Failed to publish to channel {}: {}", name, e.getMessage());
                return new OddSockets.PublishResult(
                    null,
                    null,
                    name,
                    false,
                    e.getMessage()
                );
            }
        });
    }
    
    /**
     * Publish a message with default options
     * 
     * @param message the message to publish
     * @return CompletableFuture with the publish result
     */
    public CompletableFuture<OddSockets.PublishResult> publish(Object message) {
        return publish(message, null);
    }
    
    /**
     * Get message history for the channel
     * 
     * @param options History options (can be null)
     * @return CompletableFuture with the message history
     */
    public CompletableFuture<List<Map<String, Object>>> getHistory(HistoryOptions options) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client is not connected"));
        }
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                WebSocketConnection socket = client.getSocket();
                if (socket == null) {
                    throw new IllegalStateException("No socket connection available");
                }
                
                // Send history request
                Map<String, Object> historyData = Map.of(
                    "channel", name,
                    "count", options != null ? options.getCount() : 50,
                    "start", options != null && options.getStart() != null ? options.getStart() : "",
                    "end", options != null && options.getEnd() != null ? options.getEnd() : ""
                );
                
                socket.emit("get_history", historyData);
                
                // Wait for history response (simulated)
                Thread.sleep(100);
                
                // Return cached history for now (in real implementation, this would come from server)
                List<Map<String, Object>> messages = new CopyOnWriteArrayList<>();
                for (Message msg : messageHistory) {
                    messages.add(Map.of(
                        "messageId", msg.getMessageId(),
                        "channel", msg.getChannel(),
                        "data", msg.getData(),
                        "timestamp", msg.getTimestamp().toEpochMilli(),
                        "userId", msg.getUserId()
                    ));
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
     * Get message history with default options
     * 
     * @return CompletableFuture with the message history
     */
    public CompletableFuture<List<Map<String, Object>>> getHistory() {
        return getHistory(null);
    }
    
    /**
     * Get current presence information
     * 
     * @return CompletableFuture with the presence information
     */
    public CompletableFuture<Map<String, Object>> getPresence() {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client is not connected"));
        }
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                WebSocketConnection socket = client.getSocket();
                if (socket == null) {
                    throw new IllegalStateException("No socket connection available");
                }
                
                // Send presence request
                Map<String, Object> presenceData = Map.of("channel", name);
                socket.emit("get_presence", presenceData);
                
                // Wait for presence response (simulated)
                Thread.sleep(50);
                
                Map<String, Object> presenceInfo = Map.of(
                    "channel", name,
                    "occupants", List.copyOf(presence.values()),
                    "count", presence.size(),
                    "timestamp", System.currentTimeMillis()
                );
                
                logger.debug("Retrieved presence for channel '{}': {} users", name, presence.size());
                return presenceInfo;
                
            } catch (Exception e) {
                logger.error("Failed to get presence for channel {}: {}", name, e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }
    
    /**
     * Update user state
     * 
     * @param state User state object
     * @return CompletableFuture that completes when state is updated
     */
    public CompletableFuture<Void> updateState(Map<String, Object> state) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client is not connected"));
        }
        
        return CompletableFuture.runAsync(() -> {
            try {
                WebSocketConnection socket = client.getSocket();
                if (socket == null) {
                    throw new IllegalStateException("No socket connection available");
                }
                
                // Send state update request
                Map<String, Object> stateData = Map.of("state", state);
                socket.emit("update_state", stateData);
                
                // Wait for state update confirmation (simulated)
                Thread.sleep(50);
                
                logger.debug("Updated state for channel: {}", name);
                
            } catch (Exception e) {
                logger.error("Failed to update state for channel {}: {}", name, e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }
    
    /**
     * Check if the channel is subscribed
     * 
     * @return true if subscribed, false otherwise
     */
    public boolean isSubscribed() {
        return subscribed.get();
    }
    
    /**
     * Get the channel name
     * 
     * @return the channel name
     */
    public String getName() {
        return name;
    }
    
    /**
     * Get current presence map
     * 
     * @return Copy of presence map
     */
    public Map<String, Object> getPresenceMap() {
        return Map.copyOf(presence);
    }
    
    /**
     * Get cached message history
     * 
     * @return Copy of cached messages
     */
    public List<Message> getCachedHistory() {
        return List.copyOf(messageHistory);
    }
    
    /**
     * Internal: Handle incoming message (called by OddSockets)
     * 
     * @param data Message data
     */
    void handleMessage(Map<String, Object> data) {
        try {
            // Add to history if enabled
            if (subscribeOptions != null && subscribeOptions.isRetainHistory()) {
                Message message = new Message(
                    (String) data.get("messageId"),
                    name,
                    data.get("message"),
                    Instant.ofEpochMilli((Long) data.get("timestamp")),
                    (String) data.get("userId"),
                    (Map<String, Object>) data.get("metadata")
                );
                
                messageHistory.add(message);
                
                // Trim history if too large
                if (messageHistory.size() > maxHistorySize) {
                    messageHistory.remove(0);
                }
            }
            
            // Deliver to message handler
            if (messageHandler != null) {
                messageHandler.accept(data);
            }
            
        } catch (Exception e) {
            logger.error("Error handling message for channel {}: {}", name, e.getMessage());
        }
    }
    
    /**
     * Internal: Handle subscription confirmation (called by OddSockets)
     * 
     * @param data Subscription data
     */
    void handleSubscribed(Map<String, Object> data) {
        logger.debug("Channel '{}' subscription confirmed", name);
    }
    
    /**
     * Internal: Handle unsubscription confirmation (called by OddSockets)
     * 
     * @param data Unsubscription data
     */
    void handleUnsubscribed(Map<String, Object> data) {
        logger.debug("Channel '{}' unsubscription confirmed", name);
    }
    
    /**
     * Internal: Handle publish confirmation (called by OddSockets)
     * 
     * @param data Publish confirmation data
     */
    void handlePublished(Map<String, Object> data) {
        logger.debug("Channel '{}' publish confirmed", name);
    }
    
    /**
     * Internal: Handle presence information (called by OddSockets)
     * 
     * @param data Presence data
     */
    void handlePresence(Map<String, Object> data) {
        try {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> occupants = (List<Map<String, Object>>) data.get("occupants");
            
            if (occupants != null) {
                presence.clear();
                for (Map<String, Object> occupant : occupants) {
                    String userId = (String) occupant.get("userId");
                    if (userId != null) {
                        presence.put(userId, occupant);
                    }
                }
            }
            
            logger.debug("Updated presence for channel '{}': {} users", name, presence.size());
            
        } catch (Exception e) {
            logger.error("Error handling presence for channel {}: {}", name, e.getMessage());
        }
    }
    
    /**
     * Internal: Handle presence changes (called by OddSockets)
     * 
     * @param data Presence change data
     */
    void handlePresenceChange(Map<String, Object> data) {
        try {
            String action = (String) data.get("action");
            @SuppressWarnings("unchecked")
            Map<String, Object> user = (Map<String, Object>) data.get("user");
            
            if (user != null) {
                String userId = (String) user.get("userId");
                if (userId != null) {
                    if ("join".equals(action)) {
                        presence.put(userId, user);
                    } else if ("leave".equals(action)) {
                        presence.remove(userId);
                    }
                }
            }
            
            logger.debug("Presence change for channel '{}': {} - {}", name, action, user);
            
        } catch (Exception e) {
            logger.error("Error handling presence change for channel {}: {}", name, e.getMessage());
        }
    }
    
    /**
     * Internal: Handle message history (called by OddSockets)
     * 
     * @param data History data
     */
    void handleHistory(Map<String, Object> data) {
        logger.debug("Received history for channel '{}': {}", name, data);
    }
    
    @Override
    public String toString() {
        return "Channel{" +
                "name='" + name + '\'' +
                ", subscribed=" + subscribed.get() +
                ", messageHistory=" + messageHistory.size() +
                ", presence=" + presence.size() +
                '}';
    }
    
    /**
     * Options for subscribing to a channel
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
     * Options for publishing messages
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
     * Options for retrieving message history
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
