package com.oddsockets.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.InstantSerializer;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a message received from OddSockets.
 * 
 * This class contains all the information about a message including its ID,
 * channel, data payload, timestamp, sender information, and optional metadata.
 * 
 * @author Joe Wee
 * @since 0.1.0
 */
public class Message {
    
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("channel")
    private String channel;
    
    @JsonProperty("data")
    private Object data;
    
    @JsonProperty("timestamp")
    @JsonSerialize(using = InstantSerializer.class)
    @JsonDeserialize(using = InstantDeserializer.class)
    private Instant timestamp;
    
    @JsonProperty("user_id")
    private String userId;
    
    @JsonProperty("metadata")
    private Map<String, Object> metadata;
    
    /**
     * Default constructor for JSON deserialization.
     */
    public Message() {
    }
    
    /**
     * Creates a new message.
     * 
     * @param id the message ID
     * @param channel the channel name
     * @param data the message data
     * @param timestamp the timestamp
     * @param userId the user ID
     * @param metadata the metadata
     */
    public Message(String id, String channel, Object data, Instant timestamp, String userId, Map<String, Object> metadata) {
        this.id = id;
        this.channel = channel;
        this.data = data;
        this.timestamp = timestamp;
        this.userId = userId;
        this.metadata = metadata;
    }
    
    /**
     * Gets the message ID.
     * 
     * @return the message ID
     */
    public String getId() {
        return id;
    }
    
    /**
     * Sets the message ID.
     * 
     * @param id the message ID
     */
    public void setId(String id) {
        this.id = id;
    }
    
    /**
     * Gets the channel name.
     * 
     * @return the channel name
     */
    public String getChannel() {
        return channel;
    }
    
    /**
     * Sets the channel name.
     * 
     * @param channel the channel name
     */
    public void setChannel(String channel) {
        this.channel = channel;
    }
    
    /**
     * Gets the message data.
     * 
     * @return the message data
     */
    public Object getData() {
        return data;
    }
    
    /**
     * Sets the message data.
     * 
     * @param data the message data
     */
    public void setData(Object data) {
        this.data = data;
    }
    
    /**
     * Gets the timestamp.
     * 
     * @return the timestamp
     */
    public Instant getTimestamp() {
        return timestamp;
    }
    
    /**
     * Sets the timestamp.
     * 
     * @param timestamp the timestamp
     */
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
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
     * Sets the user ID.
     * 
     * @param userId the user ID
     */
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    /**
     * Gets the metadata.
     * 
     * @return the metadata
     */
    public Map<String, Object> getMetadata() {
        return metadata;
    }
    
    /**
     * Sets the metadata.
     * 
     * @param metadata the metadata
     */
    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return Objects.equals(id, message.id) &&
                Objects.equals(channel, message.channel) &&
                Objects.equals(data, message.data) &&
                Objects.equals(timestamp, message.timestamp) &&
                Objects.equals(userId, message.userId) &&
                Objects.equals(metadata, message.metadata);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id, channel, data, timestamp, userId, metadata);
    }
    
    @Override
    public String toString() {
        return "Message{" +
                "id='" + id + '\'' +
                ", channel='" + channel + '\'' +
                ", data=" + data +
                ", timestamp=" + timestamp +
                ", userId='" + userId + '\'' +
                ", metadata=" + metadata +
                '}';
    }
}
