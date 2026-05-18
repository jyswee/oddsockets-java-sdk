package com.oddsockets;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * WebSocket connection wrapper for OddSockets.
 * 
 * This is a simplified implementation that simulates WebSocket behavior
 * for the Java SDK. In a production implementation, this would use
 * a real WebSocket library like Java-WebSocket or Tyrus.
 * 
 * This matches the JavaScript SDK Socket.IO pattern for consistency.
 */
public class WebSocketConnection {
    
    private static final Logger logger = LoggerFactory.getLogger(WebSocketConnection.class);
    
    private final String url;
    private final Map<String, Object> auth;
    private final AtomicBoolean connected;
    private final Map<String, Consumer<Object>> messageHandlers;
    private final ObjectMapper objectMapper;
    
    private volatile Consumer<String> disconnectHandler;
    private volatile Consumer<Exception> errorHandler;
    
    /**
     * Create a new WebSocket connection
     * 
     * @param url WebSocket URL
     * @param auth Authentication data
     */
    public WebSocketConnection(String url, Map<String, Object> auth) {
        this.url = url;
        this.auth = auth;
        this.connected = new AtomicBoolean(false);
        this.messageHandlers = new ConcurrentHashMap<>();
        this.objectMapper = new ObjectMapper();
        
        logger.debug("Created WebSocket connection for URL: {}", url);
    }
    
    /**
     * Connect to the WebSocket server
     * 
     * @param timeoutMs Connection timeout in milliseconds
     * @return true if connected successfully, false otherwise
     */
    public boolean connect(long timeoutMs) {
        try {
            logger.info("Connecting to WebSocket: {}", url);
            
            // Simulate connection process
            Thread.sleep(100); // Simulate network delay
            
            // Validate auth
            if (auth == null || !auth.containsKey("apiKey")) {
                throw new IllegalArgumentException("API key is required for authentication");
            }
            
            connected.set(true);
            logger.info("WebSocket connected successfully");
            
            return true;
            
        } catch (Exception e) {
            logger.error("Failed to connect to WebSocket: {}", e.getMessage());
            if (errorHandler != null) {
                errorHandler.accept(e);
            }
            return false;
        }
    }
    
    /**
     * Disconnect from the WebSocket server
     */
    public void disconnect() {
        if (connected.get()) {
            connected.set(false);
            logger.info("WebSocket disconnected");
            
            if (disconnectHandler != null) {
                disconnectHandler.accept("client_disconnect");
            }
        }
    }
    
    /**
     * Check if the connection is active
     * 
     * @return true if connected, false otherwise
     */
    public boolean isConnected() {
        return connected.get();
    }
    
    /**
     * Send a message to the server
     * 
     * @param event Event name
     * @param data Message data
     */
    public void emit(String event, Object data) {
        if (!connected.get()) {
            throw new IllegalStateException("WebSocket is not connected");
        }
        
        try {
            logger.debug("Emitting event '{}' with data: {}", event, data);
            
            // Simulate sending message
            // In a real implementation, this would serialize and send over WebSocket
            
            // Simulate server response for certain events
            simulateServerResponse(event, data);
            
        } catch (Exception e) {
            logger.error("Error emitting event '{}': {}", event, e.getMessage());
            if (errorHandler != null) {
                errorHandler.accept(e);
            }
        }
    }
    
    /**
     * Set disconnect event handler
     * 
     * @param handler Disconnect handler
     */
    public void onDisconnect(Consumer<String> handler) {
        this.disconnectHandler = handler;
    }
    
    /**
     * Set error event handler
     * 
     * @param handler Error handler
     */
    public void onError(Consumer<Exception> handler) {
        this.errorHandler = handler;
    }
    
    /**
     * Set message event handler
     * 
     * @param event Event name
     * @param handler Message handler
     */
    public void onMessage(String event, Consumer<Object> handler) {
        messageHandlers.put(event, handler);
        logger.debug("Registered handler for event: {}", event);
    }
    
    /**
     * Remove message event handler
     * 
     * @param event Event name
     */
    public void offMessage(String event) {
        messageHandlers.remove(event);
        logger.debug("Removed handler for event: {}", event);
    }
    
    /**
     * Simulate server responses for testing
     * This would not exist in a real WebSocket implementation
     */
    private void simulateServerResponse(String event, Object data) {
        try {
            // Simulate async response
            new Thread(() -> {
                try {
                    Thread.sleep(50); // Simulate network delay
                    
                    switch (event) {
                        case "subscribe" -> {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> subscribeData = (Map<String, Object>) data;
                            String channel = (String) subscribeData.get("channel");
                            
                            Map<String, Object> response = Map.of(
                                "channel", channel,
                                "success", true,
                                "timestamp", System.currentTimeMillis()
                            );
                            
                            Consumer<Object> handler = messageHandlers.get("subscribed");
                            if (handler != null) {
                                handler.accept(response);
                            }
                        }
                        
                        case "unsubscribe" -> {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> unsubscribeData = (Map<String, Object>) data;
                            String channel = (String) unsubscribeData.get("channel");
                            
                            Map<String, Object> response = Map.of(
                                "channel", channel,
                                "success", true,
                                "timestamp", System.currentTimeMillis()
                            );
                            
                            Consumer<Object> handler = messageHandlers.get("unsubscribed");
                            if (handler != null) {
                                handler.accept(response);
                            }
                        }
                        
                        case "publish" -> {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> publishData = (Map<String, Object>) data;
                            String channel = (String) publishData.get("channel");
                            
                            Map<String, Object> response = Map.of(
                                "channel", channel,
                                "messageId", "msg_" + System.currentTimeMillis(),
                                "timestamp", System.currentTimeMillis(),
                                "success", true
                            );
                            
                            Consumer<Object> handler = messageHandlers.get("published");
                            if (handler != null) {
                                handler.accept(response);
                            }
                        }
                        
                        case "get_history" -> {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> historyData = (Map<String, Object>) data;
                            String channel = (String) historyData.get("channel");
                            
                            Map<String, Object> response = Map.of(
                                "channel", channel,
                                "messages", java.util.List.of(),
                                "timestamp", System.currentTimeMillis()
                            );
                            
                            Consumer<Object> handler = messageHandlers.get("history");
                            if (handler != null) {
                                handler.accept(response);
                            }
                        }
                        
                        case "get_presence" -> {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> presenceData = (Map<String, Object>) data;
                            String channel = (String) presenceData.get("channel");
                            
                            Map<String, Object> response = Map.of(
                                "channel", channel,
                                "occupants", java.util.List.of(),
                                "count", 0,
                                "timestamp", System.currentTimeMillis()
                            );
                            
                            Consumer<Object> handler = messageHandlers.get("presence");
                            if (handler != null) {
                                handler.accept(response);
                            }
                        }
                    }
                    
                } catch (Exception e) {
                    logger.error("Error in simulated server response: {}", e.getMessage());
                }
            }).start();
            
        } catch (Exception e) {
            logger.error("Error setting up simulated server response: {}", e.getMessage());
        }
    }
    
    @Override
    public String toString() {
        return "WebSocketConnection{" +
                "url='" + url + '\'' +
                ", connected=" + connected.get() +
                '}';
    }
}
