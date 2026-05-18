package com.oddsockets;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Message size validation utility.
 * 
 * Validates message sizes against industry standard limits (32KB)
 * to match PubNub, Socket.IO, and other real-time messaging platforms.
 * 
 * This matches the JavaScript SDK pattern for consistency.
 */
public class MessageSizeValidator {
    
    private static final Logger logger = LoggerFactory.getLogger(MessageSizeValidator.class);
    
    /**
     * Message size limits (industry standard - matches PubNub)
     */
    public static final int MAX_MESSAGE_SIZE = 32768; // 32KB in bytes
    public static final int MAX_MESSAGE_SIZE_KB = 32;
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * Validate message size
     * 
     * @param message Message to validate
     * @return Message size in bytes
     * @throws IllegalArgumentException If message exceeds size limit
     */
    public static int validateMessageSize(Object message) {
        if (message == null) {
            return 0;
        }
        
        try {
            String messageStr;
            if (message instanceof String) {
                messageStr = (String) message;
            } else {
                // Convert object to JSON string
                messageStr = objectMapper.writeValueAsString(message);
            }
            
            // Calculate size in bytes using UTF-8 encoding
            byte[] messageBytes = messageStr.getBytes(StandardCharsets.UTF_8);
            int messageSize = messageBytes.length;
            
            if (messageSize > MAX_MESSAGE_SIZE) {
                String errorMessage = String.format(
                    "Message size (%dKB) exceeds maximum allowed size of %dKB. " +
                    "This limit matches industry standards (PubNub, Socket.IO) for reliable real-time messaging.",
                    Math.round(messageSize / 1024.0),
                    MAX_MESSAGE_SIZE_KB
                );
                
                logger.warn("Message size validation failed: {}", errorMessage);
                throw new IllegalArgumentException(errorMessage);
            }
            
            logger.debug("Message size validation passed: {} bytes", messageSize);
            return messageSize;
            
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                throw e; // Re-throw size validation errors
            }
            
            logger.error("Error validating message size: {}", e.getMessage());
            throw new RuntimeException("Failed to validate message size", e);
        }
    }
    
    /**
     * Check if a message size is valid without throwing an exception
     * 
     * @param message Message to check
     * @return true if message size is valid, false otherwise
     */
    public static boolean isValidMessageSize(Object message) {
        try {
            validateMessageSize(message);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Get the size of a message in bytes
     * 
     * @param message Message to measure
     * @return Message size in bytes, or -1 if measurement fails
     */
    public static int getMessageSize(Object message) {
        try {
            return validateMessageSize(message);
        } catch (IllegalArgumentException e) {
            // Return the actual size even if it exceeds the limit
            try {
                String messageStr;
                if (message instanceof String) {
                    messageStr = (String) message;
                } else {
                    messageStr = objectMapper.writeValueAsString(message);
                }
                return messageStr.getBytes(StandardCharsets.UTF_8).length;
            } catch (Exception ex) {
                return -1;
            }
        } catch (Exception e) {
            return -1;
        }
    }
}
