package com.oddsockets;

import com.oddsockets.config.OddSocketsConfig;
import com.oddsockets.config.OddSocketsToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
    private final Gson gson;
    private final ManagerDiscovery managerDiscovery;

    /** Enhanced (Slack-like) feature surface: reactions, typing, threads, DMs, presence, notifications, search. */
    public final EnhancedFeatures enhanced;
    
    private volatile String workerUrl;
    private volatile String workerId;
    private volatile String clientIdentifier;
    private volatile Map<String, Object> sessionInfo;
    private volatile WebSocketConnection socket;
    private volatile int reconnectDelay = 1000; // Start with 1 second
    private static final int MAX_RECONNECT_ATTEMPTS = 5;

    /**
     * Enhanced-feature broadcasts the worker fans out to other members of a room.
     * These are delivered straight to app listeners registered via
     * {@link #on(String, Consumer)} (the socket dispatches by event name), so this
     * list is provided for discoverability/documentation rather than wiring. The
     * request/response acks consumed by {@link EnhancedFeatures} methods are
     * intentionally not in this list.
     */
    public static final List<String> ENHANCED_BROADCAST_EVENTS = List.of(
        // reactions, presence, threads, messages
        "reaction_added", "reaction_removed",
        // challenge / leaderboard / achievement
        "challenge_progress", "leaderboard_rank_change", "challenge_complete",
        "achievement_unlock", "achievement_progress",
        "challenge_invited", "challenge_reply_received", "challenge_invite_cancelled"
    );

    // Minted-token auth state (FEAT-2026-0824-0040). Populated only when a
    // tokenProvider is configured instead of an API key.
    private volatile String token;
    private volatile long tokenExpiresAt; // epoch millis, 0 = unknown
    private volatile ScheduledFuture<?> tokenRefreshFuture;
    private final Object tokenLock = new Object();
    
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
        /** Emitted when a minted realtime token is refreshed ahead of expiry (token auth only). (FEAT-2026-0824-0040) */
        TOKEN_REFRESHED,
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
        
        // Either an API key or a token provider must be present. Game clients
        // using minted tokens have no ak_ key. (FEAT-2026-0824-0040)
        if (config.getTokenProvider() == null
                && (config.getApiKey() == null || config.getApiKey().trim().isEmpty())) {
            throw new IllegalArgumentException("Either an API key or a token provider is required");
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
        this.gson = new Gson();
        this.managerDiscovery = ManagerDiscovery.getInstance();
        this.enhanced = new EnhancedFeatures(this);
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
                // Step 0: In token mode, resolve a fresh minted token before every
                // (re)connect so both the manager select-worker call and the worker
                // handshake present a currently-valid token. (FEAT-2026-0824-0040)
                if (isTokenMode()) {
                    resolveToken();
                }

                // Step 1: Get worker assignment from manager
                getWorkerAssignment();

                // Step 2: Connect to assigned worker
                connectToWorker();

                connectionState.set(ConnectionState.CONNECTED);
                reconnectAttempts.set(0);
                reconnectDelay = 1000;
                emitEvent(EventType.CONNECTED, null);

                if (isTokenMode()) {
                    scheduleTokenRefresh();
                }

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
            synchronized (tokenLock) {
                if (tokenRefreshFuture != null) {
                    tokenRefreshFuture.cancel(false);
                    tokenRefreshFuture = null;
                }
            }

            if (socket != null) {
                socket.close();
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
     * Fetch this tenant's headline usage tiles (MAU / DAU / total messages /
     * error-rate) for the account that owns the configured API key.
     *
     * <p>Server contract: {@code GET {managerUrl}/api/tenant/usage} with the
     * {@code X-API-Key} header. Requires an API key — keyless/token-only clients
     * have no owner key to scope by, so this throws for them.
     *
     * <p>HONESTY: any tile the server cannot compute yet comes back as null. This
     * method preserves null verbatim (boxed {@link Long}/{@link Double}, never
     * coerced to 0) so callers can render an em-dash instead of a fabricated zero.
     *
     * @return CompletableFuture with the usage statistics for the owning account
     */
    public CompletableFuture<UsageStats> getUsageStats() {
        if (isTokenMode() || config.getApiKey() == null || config.getApiKey().trim().isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                "getUsageStats requires an apiKey (keyless/token clients have no owner scope to query)"));
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                // Discover the manager exactly as the select-worker call does.
                String managerUrl = managerDiscovery.discoverManagerUrl(config.getApiKey(), config.getManagerUrl()).get();

                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(managerUrl + "/api/tenant/usage"))
                    .header("X-API-Key", config.getApiKey())
                    .header("User-Agent", "OddSockets-Java-SDK/1.0.0")
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() != 200) {
                    throw new IOException("Usage stats request failed with status: " + response.statusCode());
                }

                com.fasterxml.jackson.databind.JsonNode root = objectMapper.readTree(response.body());
                com.fasterxml.jackson.databind.JsonNode tiles = root.path("tiles");

                UsageStats stats = new UsageStats();
                stats.setMau(readNullableLong(tiles, "mau"));
                stats.setDau(readNullableLong(tiles, "dau"));
                stats.setTotalMessages(readNullableLong(tiles, "totalMessages"));
                stats.setErrorRate(readNullableDouble(tiles, "errorRate"));
                stats.setOwnerScope(root.hasNonNull("ownerScope") ? root.get("ownerScope").asText() : null);
                stats.setDetail(root.hasNonNull("detail") ? root.get("detail") : null);
                stats.setTimestamp(root.hasNonNull("timestamp") ? root.get("timestamp").asText() : null);
                return stats;

            } catch (Exception e) {
                throw new RuntimeException("Failed to fetch usage stats", e);
            }
        });
    }

    // Reads a numeric tile as a boxed Long. A missing tile or an explicit JSON
    // null returns null so it stays distinguishable from a real 0.
    private static Long readNullableLong(com.fasterxml.jackson.databind.JsonNode tiles, String name) {
        com.fasterxml.jackson.databind.JsonNode node = tiles.get(name);
        return (node != null && node.isNumber()) ? node.asLong() : null;
    }

    // Reads a numeric tile as a boxed Double. A missing tile or an explicit JSON
    // null returns null so it stays distinguishable from a real 0.
    private static Double readNullableDouble(com.fasterxml.jackson.databind.JsonNode tiles, String name) {
        com.fasterxml.jackson.databind.JsonNode node = tiles.get(name);
        return (node != null && node.isNumber()) ? node.asDouble() : null;
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
     * Shared Gson instance used for enhanced event payloads.
     *
     * @return the Gson instance
     */
    public Gson getGson() {
        return gson;
    }

    /**
     * Emit a raw event to the worker over the Socket.IO connection.
     *
     * @param event the event name
     * @param data  the payload (JsonElement, Map, or any Gson-serializable object)
     */
    public void emit(String event, Object data) {
        if (socket == null) {
            throw new IllegalStateException("Not connected to OddSockets");
        }
        socket.emit(event, toJsonElement(data));
    }

    /**
     * Register a persistent listener for a raw worker event (e.g. an enhanced
     * broadcast such as "user_typing", "reaction_added", or a challenge broadcast
     * like "leaderboard_rank_change" -- see {@link #ENHANCED_BROADCAST_EVENTS}).
     * The payload is delivered as a Gson JsonObject/JsonElement.
     *
     * @param event   the event name
     * @param handler the listener
     */
    public void on(String event, Consumer<Object> handler) {
        if (socket == null) {
            throw new IllegalStateException("Not connected to OddSockets");
        }
        socket.on(event, el -> handler.accept(unwrap(el)));
    }

    /**
     * Register a one-shot listener for a raw worker event.
     *
     * @param event   the event name
     * @param handler the listener
     */
    public void once(String event, Consumer<Object> handler) {
        if (socket == null) {
            throw new IllegalStateException("Not connected to OddSockets");
        }
        socket.once(event, el -> handler.accept(unwrap(el)));
    }

    private JsonElement toJsonElement(Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof JsonElement) {
            return (JsonElement) data;
        }
        return gson.toJsonTree(data);
    }

    private static Object unwrap(JsonElement el) {
        if (el == null) {
            return null;
        }
        return el.isJsonObject() ? el.getAsJsonObject() : el;
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
            // Use the manager this client was configured for, never a substitute
            String discoverKey = config.getApiKey() != null ? config.getApiKey() : "";
            String managerUrl = managerDiscovery.discoverManagerUrl(discoverKey, config.getManagerUrl()).get();

            // In token mode present the minted token instead of an API key.
            String credentialParam = isTokenMode()
                ? "token=" + urlEncode(token != null ? token : "")
                : "apiKey=" + urlEncode(config.getApiKey());

            String requestUrl = String.format("%s/api/cluster/select-worker?%s&userId=%s&clientIdentifier=%s",
                managerUrl,
                credentialParam,
                urlEncode(config.getUserId() != null ? config.getUserId() : clientIdentifier),
                urlEncode(clientIdentifier)
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
            // The configured manager is the only manager: report the failure rather
            // than quietly connecting somewhere else.
            String message = error.getMessage();
            if (message != null && (message.contains("Connection refused") || message.contains("UnknownHost"))) {
                throw new IOException("Manager " + config.getManagerUrl()
                    + " is unreachable. Cannot assign worker without session stickiness.", error);
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

        String uid = config.getUserId() != null ? config.getUserId() : clientIdentifier;

        // Create the real Socket.IO connection and wire handlers before connecting.
        // In token mode the minted token is presented in the handshake instead of
        // the API key. (FEAT-2026-0824-0040)
        socket = isTokenMode()
            ? new WebSocketConnection(workerUrl, null, token, uid)
            : new WebSocketConnection(workerUrl, config.getApiKey(), uid);
        setupSocketEventHandlers();

        // Connect (blocks until Socket.IO CONNECT ack or timeout).
        socket.connect(15000);

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
        socket.onError((error) -> emitEvent(EventType.ERROR, error));

        // Route incoming message broadcasts to the owning channel.
        socket.on("message", (payload) -> {
            if (payload == null || !payload.isJsonObject()) return;
            JsonObject envelope = payload.getAsJsonObject();
            String channelName = envelope.has("channel") ? envelope.get("channel").getAsString() : null;
            if (channelName == null) return;
            Channel channel = channels.get(channelName);
            if (channel != null) {
                channel.handleMessage(gson.fromJson(envelope, Map.class));
            }
        });
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
            // Token-mode clients have no API key; seed the hash with a stable
            // placeholder so session stickiness still works. (FEAT-2026-0824-0040)
            String seed = (config.getApiKey() != null && !config.getApiKey().isEmpty())
                ? config.getApiKey() : "token-client";
            String apiKeyHash = hashString(seed);
            return apiKeyHash + "_" + baseId;
        } catch (Exception e) {
            logger.warn("Error generating client identifier: {}", e.getMessage());
            return "client_" + System.currentTimeMillis();
        }
    }

    /**
     * Whether this client authenticates with a minted token (via a configured
     * tokenProvider) instead of an API key. (FEAT-2026-0824-0040)
     */
    private boolean isTokenMode() {
        return config.getTokenProvider() != null;
    }

    /**
     * Invoke the configured tokenProvider and cache the fresh token plus its
     * computed expiry.
     */
    private void resolveToken() throws Exception {
        OddSocketsToken tok = config.getTokenProvider().get().get();
        if (tok == null || tok.getToken() == null || tok.getToken().isEmpty()) {
            throw new IllegalStateException("tokenProvider returned an empty token");
        }
        synchronized (tokenLock) {
            this.token = tok.getToken();
            this.tokenExpiresAt = expiryFromToken(tok);
        }
    }

    /**
     * Schedule an ahead-of-expiry token refresh. Re-arms itself for each cycle.
     */
    private void scheduleTokenRefresh() {
        long expiresAt;
        synchronized (tokenLock) {
            expiresAt = this.tokenExpiresAt;
        }
        if (expiresAt <= 0) {
            return; // No expiry info; provider is called again on next reconnect.
        }

        long lead = config.getTokenRefreshLeadMs();
        long delay = expiresAt - System.currentTimeMillis() - lead;
        if (delay < 0) {
            delay = 0;
        }

        synchronized (tokenLock) {
            if (tokenRefreshFuture != null) {
                tokenRefreshFuture.cancel(false);
            }
            tokenRefreshFuture = scheduler.schedule(() -> {
                if (connectionState.get() != ConnectionState.CONNECTED) {
                    return;
                }
                try {
                    resolveToken();
                    WebSocketConnection s = socket;
                    if (s != null) {
                        s.updateAuth(token); // Carried by the next reconnect handshake.
                    }
                    emitEvent(EventType.TOKEN_REFRESHED, Map.of(
                        "expiresAt", tokenExpiresAt
                    ));
                    scheduleTokenRefresh(); // Re-arm for the next cycle.
                } catch (Exception e) {
                    logger.warn("Token refresh failed: {}", e.getMessage());
                    emitEvent(EventType.ERROR, e);
                }
            }, delay, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Compute an epoch-millis expiry from a minted token, preferring explicit
     * fields over decoding the JWT.
     */
    private long expiryFromToken(OddSocketsToken tok) {
        if (tok.getExpiresAt() != null && !tok.getExpiresAt().isEmpty()) {
            long ms = parseExpiresAt(tok.getExpiresAt());
            if (ms > 0) {
                return ms;
            }
        }
        if (tok.getExp() != null && tok.getExp() > 0) {
            return tok.getExp() * 1000L;
        }
        return expiryFromJwt(tok.getToken());
    }

    /**
     * Parse an expiresAt value that may be epoch seconds, epoch millis, or ISO-8601.
     *
     * @return epoch millis, or 0 if unparseable
     */
    private long parseExpiresAt(String value) {
        try {
            long n = Long.parseLong(value.trim());
            return n < 1_000_000_000_000L ? n * 1000L : n;
        } catch (NumberFormatException ignored) {
            // not numeric; try ISO-8601 below
        }
        try {
            return OffsetDateTime.parse(value).toInstant().toEpochMilli();
        } catch (Exception ignored) {
            return 0;
        }
    }

    /**
     * Decode a JWT payload's {@code exp} claim (epoch seconds) into epoch millis.
     *
     * @return epoch millis, or 0 if the token has no decodable exp
     */
    private long expiryFromJwt(String jwt) {
        try {
            String[] parts = jwt.split("\\.");
            if (parts.length < 2) {
                return 0;
            }
            byte[] decoded = Base64.getUrlDecoder().decode(padBase64(parts[1]));
            JsonObject payload = JsonParser.parseString(new String(decoded)).getAsJsonObject();
            if (payload.has("exp") && payload.get("exp").isJsonPrimitive()) {
                return payload.get("exp").getAsLong() * 1000L;
            }
        } catch (Exception ignored) {
            // opaque/undecodable token: fall through to 0
        }
        return 0;
    }

    private static String padBase64(String s) {
        int rem = s.length() % 4;
        if (rem == 0) {
            return s;
        }
        return s + "====".substring(rem);
    }

    private static String urlEncode(String s) {
        return java.net.URLEncoder.encode(s, java.nio.charset.StandardCharsets.UTF_8);
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
     * Headline usage analytics for the account that owns the configured API key,
     * as returned by {@code GET {managerUrl}/api/tenant/usage}.
     *
     * <p>HONESTY: each tile is a boxed nullable ({@link Long}/{@link Double}). Any
     * tile the server cannot compute yet is preserved as {@code null} — never
     * coerced to 0 — so callers can distinguish "unknown" from a real zero.
     */
    public static class UsageStats {
        private Long mau;
        private Long dau;
        private Long totalMessages;
        private Double errorRate;
        private String ownerScope;
        private Object detail;
        private String timestamp;

        public UsageStats() {}

        /** @return monthly active users, or null if the server could not compute it */
        public Long getMau() { return mau; }
        public void setMau(Long mau) { this.mau = mau; }
        /** @return daily active users, or null if the server could not compute it */
        public Long getDau() { return dau; }
        public void setDau(Long dau) { this.dau = dau; }
        /** @return total messages, or null if the server could not compute it */
        public Long getTotalMessages() { return totalMessages; }
        public void setTotalMessages(Long totalMessages) { this.totalMessages = totalMessages; }
        /** @return error rate, or null if the server could not compute it */
        public Double getErrorRate() { return errorRate; }
        public void setErrorRate(Double errorRate) { this.errorRate = errorRate; }
        /** @return the owner scope the tiles are aggregated over */
        public String getOwnerScope() { return ownerScope; }
        public void setOwnerScope(String ownerScope) { this.ownerScope = ownerScope; }
        /** @return optional additional detail returned by the server, or null */
        public Object getDetail() { return detail; }
        public void setDetail(Object detail) { this.detail = detail; }
        /** @return the server-side timestamp for this snapshot */
        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
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
