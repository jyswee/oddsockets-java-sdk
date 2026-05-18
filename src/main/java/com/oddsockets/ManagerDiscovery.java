package com.oddsockets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Simple Manager Discovery Service
 * 
 * Always connects to the main manager endpoint which handles
 * all routing and load balancing transparently.
 * 
 * This matches the JavaScript SDK pattern for consistency.
 */
public class ManagerDiscovery {
    
    private static final Logger logger = LoggerFactory.getLogger(ManagerDiscovery.class);
    private static final String MANAGER_URL = "https://manager1.oddsockets.tyga.network";
    
    // Singleton instance
    private static final ManagerDiscovery INSTANCE = new ManagerDiscovery();
    
    private ManagerDiscovery() {}
    
    /**
     * Get the singleton instance.
     * 
     * @return the manager discovery instance
     */
    public static ManagerDiscovery getInstance() {
        return INSTANCE;
    }
    
    /**
     * Get the manager URL (always returns the main endpoint)
     * 
     * @param apiKey The OddSockets API key (not used, kept for compatibility)
     * @return CompletableFuture with the manager URL
     */
    public CompletableFuture<String> discoverManagerUrl(String apiKey) {
        logger.debug("Discovering manager URL for API key: {}", 
            apiKey != null ? apiKey.substring(0, Math.min(8, apiKey.length())) + "..." : "null");
        
        return CompletableFuture.completedFuture(MANAGER_URL);
    }
    
    /**
     * Clear cache (no-op, kept for compatibility)
     */
    public void clearCache() {
        // No cache to clear in simplified version
        logger.debug("Cache cleared (no-op in simplified version)");
    }
    
    /**
     * Get the manager URL synchronously.
     * 
     * @return the manager URL
     */
    public String getManagerUrl() {
        return MANAGER_URL;
    }
}
