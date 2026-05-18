package com.oddsockets.examples;

import com.oddsockets.OddSockets;
import com.oddsockets.Channel;
import com.oddsockets.config.OddSocketsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Basic example demonstrating OddSockets Java SDK usage.
 * 
 * This example shows the new JavaScript SDK pattern implementation:
 * - Manager discovery and worker assignment
 * - Session stickiness with client identifiers
 * - Message size validation (32KB limit)
 * - Event-driven architecture
 * - Automatic reconnection with exponential backoff
 * 
 * @author Joe Wee
 * @since 0.1.0
 */
public class BasicExample {
    
    private static final Logger logger = LoggerFactory.getLogger(BasicExample.class);
    
    public static void main(String[] args) {
        // Replace with your actual API key
        String apiKey = System.getenv("ODDSOCKETS_API_KEY");
        if (apiKey == null || apiKey.trim().isEmpty()) {
            apiKey = "ak_test_your_api_key_here";
            logger.warn("Using default API key. Set ODDSOCKETS_API_KEY environment variable for production use.");
        }
        
        try {
            runBasicExample(apiKey);
        } catch (Exception e) {
            logger.error("Example failed: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
    
    private static void runBasicExample(String apiKey) throws Exception {
        logger.info("=== OddSockets Java SDK Basic Example ===");
        logger.info("This example demonstrates the new JavaScript SDK pattern implementation");
        
        // Create configuration
        OddSocketsConfig config = OddSocketsConfig.builder()
            .apiKey(apiKey)
            .userId("java-example-user")
            .autoConnect(false) // We'll connect manually to show the process
            .build();
        
        // Create OddSockets client
        OddSockets client = new OddSockets(config);
        
        // Set up event listeners to demonstrate the event-driven architecture
        setupEventListeners(client);
        
        // Connect to the platform (demonstrates manager discovery and worker assignment)
        logger.info("Connecting to OddSockets platform...");
        client.connect().get(30, TimeUnit.SECONDS);
        
        // Get worker information (demonstrates session stickiness)
        OddSockets.WorkerInfo workerInfo = client.getWorkerInfo();
        if (workerInfo != null) {
            logger.info("Connected to worker: {} at {}", workerInfo.getWorkerId(), workerInfo.getWorkerUrl());
        }
        
        logger.info("Client identifier for session stickiness: {}", client.getClientIdentifier());
        
        // Create a channel
        Channel channel = client.channel("example-channel");
        
        // Set up message counter for the example
        CountDownLatch messageReceived = new CountDownLatch(1);
        
        // Subscribe to the channel
        logger.info("Subscribing to channel: {}", channel.getName());
        channel.subscribe(messageData -> {
            logger.info("Received message: {}", messageData);
            messageReceived.countDown();
        }, Channel.SubscribeOptions.builder()
            .retainHistory(true)
            .enablePresence(true)
            .maxHistory(50)
            .build()
        ).get(10, TimeUnit.SECONDS);
        
        // Publish a message (demonstrates message size validation)
        logger.info("Publishing message to channel...");
        Map<String, Object> message = Map.of(
            "type", "greeting",
            "text", "Hello from OddSockets Java SDK!",
            "timestamp", System.currentTimeMillis(),
            "user", "java-example-user"
        );
        
        OddSockets.PublishResult result = channel.publish(message, 
            Channel.PublishOptions.builder()
                .ttl(3600)
                .metadata(Map.of("source", "java-sdk-example"))
                .build()
        ).get(10, TimeUnit.SECONDS);
        
        if (result.isSuccess()) {
            logger.info("Message published successfully: {} at {}", result.getMessageId(), result.getTimestamp());
        } else {
            logger.error("Failed to publish message: {}", result.getError());
        }
        
        // Wait for message to be received
        if (messageReceived.await(5, TimeUnit.SECONDS)) {
            logger.info("Message received successfully!");
        } else {
            logger.warn("Message not received within timeout");
        }
        
        // Demonstrate message size validation
        logger.info("Testing message size validation...");
        try {
            // Create a large message that exceeds 32KB limit
            StringBuilder largeMessage = new StringBuilder();
            for (int i = 0; i < 40000; i++) { // 40KB of data
                largeMessage.append("x");
            }
            
            channel.publish(Map.of("data", largeMessage.toString())).get();
            logger.error("Large message should have been rejected!");
        } catch (Exception e) {
            logger.info("Message size validation working correctly: {}", e.getMessage());
        }
        
        // Get channel history
        logger.info("Retrieving channel history...");
        var history = channel.getHistory(
            Channel.HistoryOptions.builder()
                .count(10)
                .reverse(false)
                .build()
        ).get(5, TimeUnit.SECONDS);
        
        logger.info("Retrieved {} messages from history", history.size());
        
        // Get presence information
        logger.info("Getting presence information...");
        var presence = channel.getPresence().get(5, TimeUnit.SECONDS);
        logger.info("Channel presence: {} users", presence.get("count"));
        
        // Demonstrate bulk publishing
        logger.info("Testing bulk message publishing...");
        var bulkMessages = java.util.List.of(
            new OddSockets.BulkMessage("example-channel", Map.of("bulk", "message1")),
            new OddSockets.BulkMessage("example-channel", Map.of("bulk", "message2")),
            new OddSockets.BulkMessage("example-channel", Map.of("bulk", "message3"))
        );
        
        var bulkResults = client.publishBulk(bulkMessages).get(10, TimeUnit.SECONDS);
        long successCount = bulkResults.stream().mapToLong(r -> r.isSuccess() ? 1 : 0).sum();
        logger.info("Bulk publish completed: {}/{} messages successful", successCount, bulkResults.size());
        
        // Unsubscribe from channel
        logger.info("Unsubscribing from channel...");
        channel.unsubscribe().get(5, TimeUnit.SECONDS);
        
        // Disconnect from platform
        logger.info("Disconnecting from OddSockets platform...");
        client.disconnect().get(5, TimeUnit.SECONDS);
        
        // Close client and release resources
        client.close();
        
        logger.info("=== Example completed successfully! ===");
        logger.info("Key features demonstrated:");
        logger.info("✓ Manager discovery and worker assignment");
        logger.info("✓ Session stickiness with client identifiers");
        logger.info("✓ Event-driven architecture with listeners");
        logger.info("✓ Message size validation (32KB limit)");
        logger.info("✓ Channel subscription and publishing");
        logger.info("✓ Message history and presence");
        logger.info("✓ Bulk message publishing");
        logger.info("✓ Automatic reconnection support");
    }
    
    private static void setupEventListeners(OddSockets client) {
        // Connection events
        client.on(OddSockets.EventType.CONNECTING, data -> {
            logger.info("🔄 Connecting to OddSockets...");
        });
        
        client.on(OddSockets.EventType.CONNECTED, data -> {
            logger.info("✅ Connected to OddSockets successfully!");
        });
        
        client.on(OddSockets.EventType.DISCONNECTED, data -> {
            logger.info("❌ Disconnected from OddSockets: {}", data);
        });
        
        client.on(OddSockets.EventType.RECONNECTING, data -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> reconnectData = (Map<String, Object>) data;
            logger.info("🔄 Reconnecting... attempt {}/{} (delay: {}ms)", 
                reconnectData.get("attempt"), 
                reconnectData.get("maxAttempts"),
                reconnectData.get("delay"));
        });
        
        client.on(OddSockets.EventType.MAX_RECONNECT_ATTEMPTS_REACHED, data -> {
            logger.error("❌ Maximum reconnection attempts reached");
        });
        
        // Worker assignment event
        client.on(OddSockets.EventType.WORKER_ASSIGNED, data -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> workerData = (Map<String, Object>) data;
            logger.info("🎯 Worker assigned: {} at {} (session stickiness enabled)", 
                workerData.get("workerId"), 
                workerData.get("workerUrl"));
            logger.debug("Manager URL: {}", workerData.get("managerUrl"));
            logger.debug("Client identifier: {}", workerData.get("clientIdentifier"));
        });
        
        // Error events
        client.on(OddSockets.EventType.ERROR, data -> {
            if (data instanceof Exception) {
                Exception error = (Exception) data;
                logger.error("❌ OddSockets error: {}", error.getMessage());
            } else {
                logger.error("❌ OddSockets error: {}", data);
            }
        });
    }
}
