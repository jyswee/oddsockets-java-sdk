package com.oddsockets;

import com.oddsockets.config.OddSocketsConfig;
import com.oddsockets.model.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

/**
 * OddSockets Java SDK
 * 
 * Provides a simple interface to the OddSockets real-time messaging platform.
 * Automatically handles manager discovery and Worker load balancing internally.
 * 
 * This implementation matches the JavaScript SDK pattern for consistency.
 * 
 * @author Joe Wee
 * @since 0.1.0
 */
public class OddSockets {
    
    private static final Logger logger = LoggerFactory.getLogger(OddSockets.class);
    
    private final OddSocketsConfig config;
    private final AtomicReference<ConnectionState> connectionState;
    private final Map<String, Channel> channels;
    private final Map<String, List<Consumer<Object>>> eventListeners;
    private final AtomicInteger reconnectAttempts;
    private final ScheduledExecutorService scheduler;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final ManagerDiscovery managerDiscovery;
    
    private volatile String workerUrl;
    private volatile String workerId;
    private volatile String clientIdentifier;
    private volatile Map<String, Object> sessionInfo;
    private volatile WebSocketConnection socket;
    private volatile int reconnectDelay = 1000; // Start with 1 second
    private static final int MAX_RECONNECT_ATTEMPTS = 5;
    
    /**
     * Connection states for the client.
     */
    public enum ConnectionState {
        DISCONNECTED,
        CONNECTING,
        CONNECTED,
        RECONNECTING
    }
    
    /**
     * Event types that can be emitted by the client.
     */
    public enum EventType {
        CONNECTING,
        CONNECTED,
        DISCONNECTED,
        RECONNECTING,
        MAX_RECONNECT_ATTEMPTS_REACHED,
        WORKER_ASSIGNED,
        ERROR
    }
    
    /**
     * Creates a new OddSockets client with the given configuration.
     * 
     * @param config the client configuration
     * @throws IllegalArgumentException if config is null or API key is missing
     */
    public OddSockets(OddSocketsConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("Configuration cannot be null");
        }
        
        if (config.getApiKey() == null || config.getApiKey().trim().isEmpty()) {
            throw new IllegalArgumentException("API key is required");
        }
        
        this.config = config;
        this.connectionState = new AtomicReference<>(ConnectionState.DISCONNECTED);
        this.channels = new ConcurrentHashMap<>();
        this.eventListeners = new ConcurrentHashMap<>();
        this.reconnectAttempts = new AtomicInteger(0);
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();
        this.objectMapper = new ObjectMapper();
        this.managerDiscovery = ManagerDiscovery.getInstance();
        this.clientIdentifier = generateClientIdentifier();
        
        logger.info("OddSockets client initialized for user: {} with client identifier: {}", 
            config.getUserId(), clientIdentifier);
        
        // Auto-connect if requested
        if (config.isAutoConnect()) {
            CompletableFuture.runAsync(() -> {
                try {
                    connect().get();
                } catch (Exception e) {
                    logger.warn("Auto-connect failed: {}", e.getMessage());
                    emitEvent(EventType.ERROR, e);
                }
            });
        }
    }
    
    /**
     * Connect to the OddSockets platform
     * Handles the Manager → Worker assignment internally
     */
    public CompletableFuture<Void> connect() {
        if (connectionState.get() == ConnectionState.CONNECTING || connectionState.get() == ConnectionState.CONNECTED) {
            return CompletableFuture.completedFuture(null);
        }
        
        connectionState.set(ConnectionState.CONNECTING);
        emitEvent(EventType.CONNECTING, null);
        
        return CompletableFuture.runAsync(() -> {
            try {
                // Step 1: Get worker assignment from manager
                getWorkerAssignment();
                
                // Step 2: Connect to assigned worker
                connectToWorker();
                
                connectionState.set(ConnectionState.CONNECTED);
                reconnectAttempts.set(0);
                reconnectDelay = 1000;
                emitEvent(EventType.CONNECTED, null);
                
                logger.info("Successfully connected to OddSockets worker: {}", workerId);
                
            } catch (Exception error) {
                connectionState.set(ConnectionState.DISCONNECTED);
                emitEvent(EventType.ERROR, error);
                
                // Auto-reconnect with exponential backoff
                if (reconnectAttempts.get() < MAX_RECONNECT_ATTEMPTS) {
                    scheduleReconnect();
                } else {
                    emitEvent(EventType.MAX_RECONNECT_ATTEMPTS_REACHED, null);
                }
                
                throw new RuntimeException("Connection failed", error);
            }
        });
    }
    
    /**
     * Disconnect from the platform
     */
    public CompletableFuture<Void> disconnect() {
        connectionState.set(ConnectionState.DISCONNECTED);
        
        return CompletableFuture.runAsync(() -> {
            if (socket != null) {
                socket.disconnect();
                socket = null;
            }
            
            workerUrl = null;
            workerId = null;
            emitEvent(EventType.DISCONNECTED, null);
            
            logger.info("Disconnected from OddSockets");
        });
    }
    
    /**
     * Get or create a channel
     * 
     * @param channelName Name of the channel
     * @return Channel instance
     * @throws IllegalArgumentException if channelName is null or empty
     */
    public Channel channel(String channelName) {
        if (channelName == null || channelName.trim().isEmpty()) {
            throw new IllegalArgumentException("Channel name must be a non-empty string");
        }
        
        return channels.computeIfAbsent(channelName, name -> {
            Channel channel = new Channel(name, this);
            logger.debug("Created channel: {}", name);
            return channel;
        });
    }
    
    /**
     * Get current connection state
     * 
     * @return Connection state
     */
    public ConnectionState getState() {
        return connectionState.get();
    }
    
    /**
     * Get assigned worker information
     * 
     * @return Worker info or null if not assigned
     */
    public WorkerInfo getWorkerInfo() {
        if (workerId == null || workerUrl == null) {
            return null;
        }
        
        return new WorkerInfo(workerId, workerUrl);
    }
    
    /**
     * Publish multiple messages at once
     * 
     * @param messages Array of message objects with channel, message, and options
     * @return CompletableFuture with array of publish results
     */
    public CompletableFuture<List<PublishResult>> publishBulk(List<BulkMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            throw new IllegalArgumentException("Messages must be a non-empty list");
        }
        
        if (!isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not connected to OddSockets"));
        }
        
        List<CompletableFuture<PublishResult>> futures = new ArrayList<>();
        
        for (BulkMessage msg : messages) {
            try {
                if (msg.getChannel() == null || msg.getMessage() == null) {
                    futures.add(CompletableFuture.completedFuture(
                        new PublishResult(null, null, null, false, "Missing channel or message")
                    ));
                    continue;
                }
                
                Channel channel = channel(msg.getChannel());
                CompletableFuture<PublishResult> future = channel.publish(msg.getMessage(), msg.getOptions())
                    .handle((result, throwable) -> {
                        if (throwable != null) {
                            return new PublishResult(null, null, msg.getChannel(), false, throwable.getMessage());
                        }
                        return result;
                    });
                
                futures.add(future);
                
            } catch (Exception e) {
                futures.add(CompletableFuture.completedFuture(
                    new PublishResult(null, null, msg.getChannel(), false, e.getMessage())
                ));
            }
        }
        
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> futures.stream()
                .map(CompletableFuture::join)
                .toList());
    }
    
    /**
     * Add an event listener
     * 
     * @param eventType the event type
     * @param listener the event listener
     */
    public void on(EventType eventType, Consumer<Object> listener) {
        eventListeners.computeIfAbsent(eventType.name(), k -> new ArrayList<>()).add(listener);
        logger.debug("Added listener for event: {}", eventType);
    }
    
    /**
     * Remove event listeners for the given event type
     * 
     * @param eventType the event type
     */
    public void off(EventType eventType) {
        eventListeners.remove(eventType.name());
        logger.debug("Removed all listeners for event: {}", eventType);
    }
    
    /**
     * Check if the client is connected
     * 
     * @return true if connected, false otherwise
     */
    public boolean isConnected() {
        return connectionState.get() == ConnectionState.CONNECTED && socket != null && socket.isConnected();
    }
    
    /**
     * Get client identifier used for session stickiness
     * 
     * @return Client identifier
     */
    public String getClientIdentifier() {
        return clientIdentifier;
    }
    
    /**
     * Get session information
     * 
     * @return Session info or null if not available
     */
    public Map<String, Object> getSessionInfo() {
        return sessionInfo;
    }
    
    /**
     * Get the configuration
     * 
     * @return the configuration
     */
    public OddSocketsConfig getConfig() {
        return config;
    }
    
    /**
     * Get socket instance (for Channel class)
     * 
     * @return WebSocket connection or null if not connected
     */
    WebSocketConnection getSocket() {
        return socket;
    }
    
    /**
     * Close the client and release all resources
     */
    public void close() {
        try {
            disconnect().get();
        } catch (Exception e) {
            logger.warn("Error during disconnect: {}", e.getMessage());
        }
        
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Internal: Get worker assignment from manager
     */
    private void getWorkerAssignment() throws Exception {
        try {
            // Discover the optimal manager URL automatically
            String managerUrl = managerDiscovery.discoverManagerUrl(config.getApiKey()).get();
            
            String requestUrl = String.format("%s/api/cluster/select-worker?apiKey=%s&userId=%s&clientIdentifier=%s",
                managerUrl,
                config.getApiKey(),
                config.getUserId() != null ? config.getUserId() : clientIdentifier,
                clientIdentifier
            );
            
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(requestUrl))
                .header("User-Agent", "OddSockets-Java-SDK/1.0.0")
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
            
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() != 200) {
                throw new IOException("Worker assignment failed with status: " + response.statusCode());
            }
            
            @SuppressWarnings("unchecked")
            Map<String, Object> responseData = objectMapper.readValue(response.body(), Map.class);
            
            if (!responseData.containsKey("url")) {
                throw new IOException("Invalid worker assignment response");
            }
            
            this.workerUrl = (String) responseData.get("url");
            this.workerId = (String) responseData.get("workerId");
            this.sessionInfo = (Map<String, Object>) responseData.get("session");
            
            Map<String, Object> workerAssignedData = Map.of(
                "workerId", workerId != null ? workerId : "",
                "workerUrl", workerUrl != null ? workerUrl : "",
                "session", sessionInfo != null ? sessionInfo : Map.of(),
                "clientIdentifier", clientIdentifier,
                "managerUrl", managerUrl
            );
            
            emitEvent(EventType.WORKER_ASSIGNED, workerAssignedData);
            
            logger.info("Worker assigned: {} at {}", workerId, workerUrl);
            
        } catch (Exception error) {
            // If manager is offline, try fallback logic
            if (error.getMessage().contains("Connection refused") || error.getMessage().contains("UnknownHost")) {
                throw new IOException("Manager is offline. Cannot assign worker without session stickiness.");
            }
            throw error;
        }
    }
    
    /**
     * Internal: Connect to assigned worker
     */
    private void connectToWorker() throws Exception {
        if (workerUrl == null) {
            throw new IllegalStateException("No worker URL available");
        }
        
        Map<String, Object> auth = Map.of(
            "apiKey", config.getApiKey(),
            "userId", config.getUserId() != null ? config.getUserId() : clientIdentifier
        );
        
        // Create WebSocket connection (simplified implementation)
        socket = new WebSocketConnection(workerUrl, auth);
        
        // Setup event handlers
        setupSocketEventHandlers();
        
        // Connect with timeout
        if (!socket.connect(15000)) {
            throw new IOException("Connection timeout");
        }
        
        logger.info("Connected to worker: {}", workerUrl);
    }
    
    /**
     * Internal: Setup socket event handlers
     */
    private void setupSocketEventHandlers() {
        if (socket == null) return;
        
        // Handle disconnection
        socket.onDisconnect((reason) -> {
            connectionState.set(ConnectionState.DISCONNECTED);
            emitEvent(EventType.DISCONNECTED, reason);
            
            // Auto-reconnect unless manually disconnected
            if (!"client_disconnect".equals(reason)) {
                scheduleReconnect();
            }
        });
        
        // Handle errors
        socket.onError((error) -> {
            emitEvent(EventType.ERROR, error);
        });
        
        // Forward channel-related events to appropriate channels
        socket.onMessage("message", (data) -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> messageData = (Map<String, Object>) data;
            String channelName = (String) messageData.get("channel");
            Channel channel = channels.get(channelName);
            if (channel != null) {
                channel.handleMessage(messageData);
            }
        });
        
        // Handle other channel events
        setupChannelEventHandlers();
    }
    
    /**
     * Internal: Setup channel event handlers
     */
    private void setupChannelEventHandlers() {
        String[] events = {"subscribed", "unsubscribed", "published", "presence", "presence_change", "history"};
        
        for (String event : events) {
            socket.onMessage(event, (data) -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> eventData = (Map<String, Object>) data;
                String channelName = (String) eventData.get("channel");
                Channel channel = channels.get(channelName);
                if (channel != null) {
                    switch (event) {
                        case "subscribed" -> channel.handleSubscribed(eventData);
                        case "unsubscribed" -> channel.handleUnsubscribed(eventData);
                        case "published" -> channel.handlePublished(eventData);
                        case "presence" -> channel.handlePresence(eventData);
                        case "presence_change" -> channel.handlePresenceChange(eventData);
                        case "history" -> channel.handleHistory(eventData);
                    }
                }
            });
        }
    }
    
    /**
     * Internal: Schedule reconnection with exponential backoff
     */
    private void scheduleReconnect() {
        if (connectionState.get() == ConnectionState.CONNECTED) return;
        
        connectionState.set(ConnectionState.RECONNECTING);
        int attempt = reconnectAttempts.incrementAndGet();
        
        int delay = Math.min(reconnectDelay * (int) Math.pow(2, attempt - 1), 30000);
        
        Map<String, Object> reconnectingData = Map.of(
            "attempt", attempt,
            "maxAttempts", MAX_RECONNECT_ATTEMPTS,
            "delay", delay
        );
        
        emitEvent(EventType.RECONNECTING, reconnectingData);
        
        scheduler.schedule(() -> {
            if (connectionState.get() == ConnectionState.RECONNECTING) {
                try {
                    connect().get();
                } catch (Exception e) {
                    logger.warn("Reconnection attempt {} failed: {}", attempt, e.getMessage());
                }
            }
        }, delay, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Internal: Generate consistent client identifier for session stickiness
     */
    private String generateClientIdentifier() {
        try {
            String baseId = config.getUserId() != null ? config.getUserId() : "default";
            String apiKeyHash = hashString(config.getApiKey());
            return apiKeyHash + "_" + baseId;
        } catch (Exception e) {
            logger.warn("Error generating client identifier: {}", e.getMessage());
            return "client_" + System.currentTimeMillis();
        }
    }
    
    /**
     * Internal: Simple hash function for API key
     */
    private String hashString(String str) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(str.getBytes());
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.substring(0, 8); // Take first 8 characters
        } catch (Exception e) {
            return String.valueOf(Math.abs(str.hashCode()));
        }
    }
    
    /**
     * Internal: Emit event to listeners
     */
    private void emitEvent(EventType eventType, Object data) {
        List<Consumer<Object>> listeners = eventListeners.get(eventType.name());
        if (listeners != null) {
            listeners.forEach(listener -> {
                try {
                    listener.accept(data);
                } catch (Exception e) {
                    logger.error("Error in event listener for {}: {}", eventType, e.getMessage());
                }
            });
        }
    }
    
    @Override
    public String toString() {
        return "OddSockets{" +
                "userId='" + config.getUserId() + '\'' +
                ", state=" + connectionState.get() +
                ", channels=" + channels.size() +
                ", workerId='" + workerId + '\'' +
                '}';
    }
    
    /**
     * Worker information
     */
    public static class WorkerInfo {
        private final String workerId;
        private final String workerUrl;
        
        public WorkerInfo(String workerId, String workerUrl) {
            this.workerId = workerId;
            this.workerUrl = workerUrl;
        }
        
        public String getWorkerId() { return workerId; }
        public String getWorkerUrl() { return workerUrl; }
    }
    
    /**
     * Represents a message for bulk publishing
     */
    public static class BulkMessage {
        private String channel;
        private Object message;
        private Channel.PublishOptions options;
        
        public BulkMessage() {}
        
        public BulkMessage(String channel, Object message) {
            this.channel = channel;
            this.message = message;
        }
        
        public BulkMessage(String channel, Object message, Channel.PublishOptions options) {
            this.channel = channel;
            this.message = message;
            this.options = options;
        }
        
        public String getChannel() { return channel; }
        public void setChannel(String channel) { this.channel = channel; }
        public Object getMessage() { return message; }
        public void setMessage(Object message) { this.message = message; }
        public Channel.PublishOptions getOptions() { return options; }
        public void setOptions(Channel.PublishOptions options) { this.options = options; }
    }
    
    /**
     * Represents the result of a publish operation
     */
    public static class PublishResult {
        private String messageId;
        private Long timestamp;
        private String channel;
        private boolean success;
        private String error;
        
        public PublishResult() {}
        
        public PublishResult(String messageId, Long timestamp, String channel, boolean success, String error) {
            this.messageId = messageId;
            this.timestamp = timestamp;
            this.channel = channel;
            this.success = success;
            this.error = error;
        }
        
        public String getMessageId() { return messageId; }
        public void setMessageId(String messageId) { this.messageId = messageId; }
        public Long getTimestamp() { return timestamp; }
        public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
        public String getChannel() { return channel; }
        public void setChannel(String channel) { this.channel = channel; }
        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }
        public String getError() { return error; }
        public void setError(String error) { this.error = error; }
    }
}
