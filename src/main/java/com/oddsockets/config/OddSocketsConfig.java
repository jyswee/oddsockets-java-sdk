package com.oddsockets.config;

import java.time.Duration;
import java.util.UUID;

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
    private final String managerUrl;
    private final String userId;
    private final boolean autoConnect;
    private final int reconnectAttempts;
    private final Duration heartbeatInterval;
    private final Duration requestTimeout;
    
    private OddSocketsConfig(Builder builder) {
        this.apiKey = builder.apiKey;
        this.managerUrl = builder.managerUrl != null ? builder.managerUrl : "https://connect.oddsockets.tyga.network";
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
     * Gets the manager URL.
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
        private String managerUrl;
        private String userId;
        private boolean autoConnect = true;
        private int reconnectAttempts = 5;
        private Duration heartbeatInterval;
        private Duration requestTimeout;
        
        /**
         * Sets the API key (required).
         * 
         * @param apiKey the API key
         * @return this builder
         */
        public Builder apiKey(String apiKey) {
            this.apiKey = apiKey;
            return this;
        }
        
        /**
         * Sets the manager URL (optional).
         * 
         * @param managerUrl the manager URL
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
         * @throws IllegalArgumentException if API key is missing or invalid
         */
        public OddSocketsConfig build() {
            if (apiKey == null || apiKey.trim().isEmpty()) {
                throw new IllegalArgumentException("API key is required");
            }
            
            if (!apiKey.startsWith("ak_")) {
                throw new IllegalArgumentException("Invalid API key format");
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
