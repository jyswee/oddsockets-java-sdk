package com.oddsockets.config;

import com.oddsockets.ManagerDiscovery;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Configuration class for OddSockets client.
 * 
 * This class provides a builder pattern for configuring the OddSockets client
 * with various options such as API key, manager URL, connection settings, etc.
 * 
 * @author Joe Wee
 * @since 0.1.0
 */
public class OddSocketsConfig {
    
    private final String apiKey;
    private final Supplier<CompletableFuture<OddSocketsToken>> tokenProvider;
    private final long tokenRefreshLeadMs;
    private final String managerUrl;
    private final String userId;
    private final boolean autoConnect;
    private final int reconnectAttempts;
    private final Duration heartbeatInterval;
    private final Duration requestTimeout;

    private OddSocketsConfig(Builder builder) {
        this.apiKey = builder.apiKey;
        this.tokenProvider = builder.tokenProvider;
        this.tokenRefreshLeadMs = builder.tokenRefreshLeadMs;
        this.managerUrl = ManagerDiscovery.resolveManagerUrl(builder.managerUrl);
        this.userId = builder.userId != null ? builder.userId : "user_" + UUID.randomUUID().toString().substring(0, 8);
        this.autoConnect = builder.autoConnect;
        this.reconnectAttempts = builder.reconnectAttempts;
        this.heartbeatInterval = builder.heartbeatInterval != null ? builder.heartbeatInterval : Duration.ofSeconds(30);
        this.requestTimeout = builder.requestTimeout != null ? builder.requestTimeout : Duration.ofSeconds(10);
    }

    /**
     * Gets the API key.
     *
     * @return the API key
     */
    public String getApiKey() {
        return apiKey;
    }

    /**
     * Gets the async token provider used instead of an API key by game clients
     * that exchange a player JWT for a short-lived scoped token via the OddSockets
     * {@code /v1/token} front door. Called before every (re)connect and again
     * shortly before the token expires. (FEAT-2026-0824-0040)
     *
     * @return the token provider, or null when authenticating with an API key
     */
    public Supplier<CompletableFuture<OddSocketsToken>> getTokenProvider() {
        return tokenProvider;
    }

    /**
     * Gets how many milliseconds before expiry a minted token is refreshed.
     *
     * @return the refresh lead time in milliseconds
     */
    public long getTokenRefreshLeadMs() {
        return tokenRefreshLeadMs;
    }
    
    /**
     * Gets the manager URL that this client will use.
     *
     * <p>This is the URL supplied to the builder, or the {@code ODDSOCKETS_MANAGER_URL}
     * environment variable, or the public default endpoint - in that order.</p>
     *
     * @return the manager URL
     */
    public String getManagerUrl() {
        return managerUrl;
    }
    
    /**
     * Gets the user ID.
     * 
     * @return the user ID
     */
    public String getUserId() {
        return userId;
    }
    
    /**
     * Gets whether auto-connect is enabled.
     * 
     * @return true if auto-connect is enabled
     */
    public boolean isAutoConnect() {
        return autoConnect;
    }
    
    /**
     * Gets the maximum number of reconnection attempts.
     * 
     * @return the reconnection attempts
     */
    public int getReconnectAttempts() {
        return reconnectAttempts;
    }
    
    /**
     * Gets the heartbeat interval.
     * 
     * @return the heartbeat interval
     */
    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }
    
    /**
     * Gets the request timeout.
     * 
     * @return the request timeout
     */
    public Duration getRequestTimeout() {
        return requestTimeout;
    }
    
    /**
     * Creates a new builder instance.
     * 
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Builder class for OddSocketsConfig.
     */
    public static class Builder {
        private String apiKey;
        private Supplier<CompletableFuture<OddSocketsToken>> tokenProvider;
        private long tokenRefreshLeadMs = 120000;
        private String managerUrl;
        private String userId;
        private boolean autoConnect = true;
        private int reconnectAttempts = 5;
        private Duration heartbeatInterval;
        private Duration requestTimeout;

        /**
         * Sets the API key (required unless a token provider is set).
         *
         * @param apiKey the API key
         * @return this builder
         */
        public Builder apiKey(String apiKey) {
            this.apiKey = apiKey;
            return this;
        }

        /**
         * Sets an async token provider used instead of an API key. The supplier
         * returns a {@link CompletableFuture} that resolves a fresh minted realtime
         * token. (FEAT-2026-0824-0040)
         *
         * @param tokenProvider callback returning a fresh minted realtime token
         * @return this builder
         */
        public Builder tokenProvider(Supplier<CompletableFuture<OddSocketsToken>> tokenProvider) {
            this.tokenProvider = tokenProvider;
            return this;
        }

        /**
         * Sets how many milliseconds before expiry a minted token is refreshed
         * (default: 120000).
         *
         * @param tokenRefreshLeadMs the refresh lead time in milliseconds
         * @return this builder
         */
        public Builder tokenRefreshLeadMs(long tokenRefreshLeadMs) {
            this.tokenRefreshLeadMs = tokenRefreshLeadMs;
            return this;
        }
        
        /**
         * Sets the manager URL (optional).
         *
         * <p>When omitted, the {@code ODDSOCKETS_MANAGER_URL} environment variable is
         * used, falling back to the public default endpoint. A URL set here is always
         * used verbatim; the client never falls back to the default endpoint if it is
         * unreachable.</p>
         *
         * @param managerUrl the manager URL, must be an absolute http(s) URL
         * @return this builder
         */
        public Builder managerUrl(String managerUrl) {
            this.managerUrl = managerUrl;
            return this;
        }
        
        /**
         * Sets the user ID (optional).
         * 
         * @param userId the user ID
         * @return this builder
         */
        public Builder userId(String userId) {
            this.userId = userId;
            return this;
        }
        
        /**
         * Sets whether to auto-connect on client creation.
         * 
         * @param autoConnect true to auto-connect
         * @return this builder
         */
        public Builder autoConnect(boolean autoConnect) {
            this.autoConnect = autoConnect;
            return this;
        }
        
        /**
         * Sets the maximum number of reconnection attempts.
         * 
         * @param reconnectAttempts the reconnection attempts
         * @return this builder
         */
        public Builder reconnectAttempts(int reconnectAttempts) {
            this.reconnectAttempts = reconnectAttempts;
            return this;
        }
        
        /**
         * Sets the heartbeat interval.
         * 
         * @param heartbeatInterval the heartbeat interval
         * @return this builder
         */
        public Builder heartbeatInterval(Duration heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
            return this;
        }
        
        /**
         * Sets the request timeout.
         * 
         * @param requestTimeout the request timeout
         * @return this builder
         */
        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }
        
        /**
         * Builds the configuration.
         * 
         * @return the configuration
         * @throws IllegalArgumentException if the API key or manager URL is missing or invalid
         */
        public OddSocketsConfig build() {
            // Either an API key or a token provider is acceptable. A game client
            // using minted tokens has no ak_ key, so the format check only applies
            // in key mode. (FEAT-2026-0824-0040)
            if (tokenProvider == null) {
                if (apiKey == null || apiKey.trim().isEmpty()) {
                    throw new IllegalArgumentException("Either an API key or a token provider is required");
                }

                if (!apiKey.startsWith("ak_")) {
                    throw new IllegalArgumentException("Invalid API key format");
                }
            }

            return new OddSocketsConfig(this);
        }
    }
    
    @Override
    public String toString() {
        return "OddSocketsConfig{" +
                "apiKey='***'" +
                ", managerUrl='" + managerUrl + '\'' +
                ", userId='" + userId + '\'' +
                ", autoConnect=" + autoConnect +
                ", reconnectAttempts=" + reconnectAttempts +
                ", heartbeatInterval=" + heartbeatInterval +
                ", requestTimeout=" + requestTimeout +
                '}';
    }
}
