package com.oddsockets;

import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Enhanced Features for OddSockets Java SDK
 * Provides 67 new Slack-like events
 */
public class EnhancedFeatures {
    private final OddSockets client;
    private static final int TIMEOUT_SECONDS = 10;

    public EnhancedFeatures(OddSockets client) {
        this.client = client;
    }

    // ==================== THREAD EVENTS ====================

    /**
     * Reply to a message in a thread
     */
    public CompletableFuture<JsonObject> threadReply(String channel, String parentMessageId, 
                                                      String message, String userId, String userName) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not connected to OddSockets"));
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();

        JsonObject params = new JsonObject();
        params.addProperty("channel", channel);
        params.addProperty("parentMessageId", parentMessageId);
        params.addProperty("message", message);
        params.addProperty("userId", userId);
        params.addProperty("userName", userName);

        client.once("thread_reply_success", data -> future.complete((JsonObject) data));
        client.once("error", data -> {
            JsonObject error = (JsonObject) data;
            if ("thread_reply".equals(error.get("event").getAsString())) {
                future.completeExceptionally(new RuntimeException(error.get("message").getAsString()));
            }
        });

        client.emit("thread_reply", params);

        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Get thread with all replies
     */
    public CompletableFuture<JsonObject> getThread(String threadId) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not connected to OddSockets"));
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();

        JsonObject params = new JsonObject();
        params.addProperty("threadId", threadId);

        client.once("thread_data", data -> future.complete((JsonObject) data));
        client.once("error", data -> {
            JsonObject error = (JsonObject) data;
            if ("get_thread".equals(error.get("event").getAsString())) {
                future.completeExceptionally(new RuntimeException(error.get("message").getAsString()));
            }
        });

        client.emit("get_thread", params);

        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Subscribe to thread updates
     */
    public CompletableFuture<JsonObject> subscribeThread(String threadId, String userId) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not connected to OddSockets"));
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();

        JsonObject params = new JsonObject();
        params.addProperty("threadId", threadId);
        params.addProperty("userId", userId);

        client.once("thread_subscribed", data -> future.complete((JsonObject) data));
        client.once("error", data -> {
            JsonObject error = (JsonObject) data;
            if ("subscribe_thread".equals(error.get("event").getAsString())) {
                future.completeExceptionally(new RuntimeException(error.get("message").getAsString()));
            }
        });

        client.emit("subscribe_thread", params);

        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Mark thread as read
     */
    public void markThreadRead(String threadId, String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("threadId", threadId);
        params.addProperty("userId", userId);

        client.emit("mark_thread_read", params);
    }

    /**
     * Follow a thread
     */
    public void followThread(String threadId, String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("threadId", threadId);
        params.addProperty("userId", userId);

        client.emit("follow_thread", params);
    }

    /**
     * Unfollow a thread
     */
    public void unfollowThread(String threadId, String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("threadId", threadId);
        params.addProperty("userId", userId);

        client.emit("unfollow_thread", params);
    }

    // ==================== REACTION EVENTS ====================

    /**
     * Add reaction to a message
     */
    public void addReaction(String messageId, String channel, String emoji, String userId, String userName) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("messageId", messageId);
        params.addProperty("channel", channel);
        params.addProperty("emoji", emoji);
        params.addProperty("userId", userId);
        params.addProperty("userName", userName);

        client.emit("add_reaction", params);
    }

    /**
     * Remove reaction from a message
     */
    public void removeReaction(String messageId, String channel, String emoji, String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("messageId", messageId);
        params.addProperty("channel", channel);
        params.addProperty("emoji", emoji);
        params.addProperty("userId", userId);

        client.emit("remove_reaction", params);
    }

    /**
     * Get all reactions for a message
     */
    public CompletableFuture<JsonObject> getReactions(String messageId) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not connected to OddSockets"));
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();

        JsonObject params = new JsonObject();
        params.addProperty("messageId", messageId);

        client.once("message_reactions", data -> future.complete((JsonObject) data));
        client.once("error", data -> {
            JsonObject error = (JsonObject) data;
            if ("get_reactions".equals(error.get("event").getAsString())) {
                future.completeExceptionally(new RuntimeException(error.get("message").getAsString()));
            }
        });

        client.emit("get_reactions", params);

        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    // ==================== READ RECEIPT EVENTS ====================

    /**
     * Mark message as read
     */
    public void markRead(String messageId, String channel, String userId, String userName) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("messageId", messageId);
        params.addProperty("channel", channel);
        params.addProperty("userId", userId);
        params.addProperty("userName", userName);

        client.emit("mark_read", params);
    }

    /**
     * Get unread counts for channels
     */
    public CompletableFuture<JsonObject> getUnreadCounts(String userId, List<String> channels) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not connected to OddSockets"));
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();

        JsonObject params = new JsonObject();
        params.addProperty("userId", userId);
        JsonArray channelsArray = new JsonArray();
        channels.forEach(channelsArray::add);
        params.add("channels", channelsArray);

        client.once("unread_counts", data -> future.complete((JsonObject) data));
        client.once("error", data -> {
            JsonObject error = (JsonObject) data;
            if ("get_unread_counts".equals(error.get("event").getAsString())) {
                future.completeExceptionally(new RuntimeException(error.get("message").getAsString()));
            }
        });

        client.emit("get_unread_counts", params);

        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Mark all messages in channel as read
     */
    public void markAllRead(String channel, String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("channel", channel);
        params.addProperty("userId", userId);

        client.emit("mark_all_read", params);
    }

    // ==================== CHANNEL EVENTS ====================

    /**
     * Create a new channel
     */
    public CompletableFuture<JsonObject> createChannel(String name, String type, String description,
                                                        String topic, String createdBy, String createdByName) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not connected to OddSockets"));
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();

        JsonObject params = new JsonObject();
        params.addProperty("name", name);
        params.addProperty("type", type);
        params.addProperty("description", description);
        params.addProperty("topic", topic);
        params.addProperty("createdBy", createdBy);
        params.addProperty("createdByName", createdByName);
        params.add("members", new JsonArray());

        client.once("channel_create_success", data -> future.complete((JsonObject) data));
        client.once("error", data -> {
            JsonObject error = (JsonObject) data;
            if ("create_channel".equals(error.get("event").getAsString())) {
                future.completeExceptionally(new RuntimeException(error.get("message").getAsString()));
            }
        });

        client.emit("create_channel", params);

        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Update channel details
     */
    public void updateChannel(String channelId, Map<String, Object> updates, String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("channelId", channelId);
        params.add("updates", client.getGson().toJsonTree(updates));
        params.addProperty("userId", userId);

        client.emit("update_channel", params);
    }

    /**
     * Archive a channel
     */
    public void archiveChannel(String channelId, String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("channelId", channelId);
        params.addProperty("userId", userId);

        client.emit("archive_channel", params);
    }

    /**
     * Invite user to channel
     */
    public void inviteToChannel(String channelId, String invitedUserId, String invitedUserName, String invitedBy) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("channelId", channelId);
        params.addProperty("invitedUserId", invitedUserId);
        params.addProperty("invitedUserName", invitedUserName);
        params.addProperty("invitedBy", invitedBy);

        client.emit("invite_to_channel", params);
    }

    /**
     * Remove user from channel
     */
    public void removeFromChannel(String channelId, String removedUserId, String removedBy) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("channelId", channelId);
        params.addProperty("removedUserId", removedUserId);
        params.addProperty("removedBy", removedBy);

        client.emit("remove_from_channel", params);
    }

    /**
     * Join a public channel
     */
    public void joinChannel(String channelId, String userId, String userName) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("channelId", channelId);
        params.addProperty("userId", userId);
        params.addProperty("userName", userName);

        client.emit("join_channel", params);
    }

    /**
     * Leave a channel
     */
    public void leaveChannel(String channelId, String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("channelId", channelId);
        params.addProperty("userId", userId);

        client.emit("leave_channel", params);
    }

    /**
     * Get channel members
     */
    public CompletableFuture<JsonObject> getChannelMembers(String channelId) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not connected to OddSockets"));
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();

        JsonObject params = new JsonObject();
        params.addProperty("channelId", channelId);

        client.once("channel_members", data -> future.complete((JsonObject) data));
        client.once("error", data -> {
            JsonObject error = (JsonObject) data;
            if ("get_channel_members".equals(error.get("event").getAsString())) {
                future.completeExceptionally(new RuntimeException(error.get("message").getAsString()));
            }
        });

        client.emit("get_channel_members", params);

        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    // ==================== DIRECT MESSAGE EVENTS ====================

    /**
     * Create or get DM conversation
     */
    public CompletableFuture<JsonObject> createDM(List<String> userIds, String type) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not connected to OddSockets"));
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();

        JsonObject params = new JsonObject();
        JsonArray userIdsArray = new JsonArray();
        userIds.forEach(userIdsArray::add);
        params.add("userIds", userIdsArray);
        params.addProperty("type", type);

        client.once("dm_create_success", data -> future.complete((JsonObject) data));
        client.once("error", data -> {
            JsonObject error = (JsonObject) data;
            if ("create_dm".equals(error.get("event").getAsString())) {
                future.completeExceptionally(new RuntimeException(error.get("message").getAsString()));
            }
        });

        client.emit("create_dm", params);

        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Send direct message
     */
    public void sendDM(String conversationId, String message, String userId, String userName) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("conversationId", conversationId);
        params.addProperty("message", message);
        params.addProperty("userId", userId);
        params.addProperty("userName", userName);

        client.emit("send_dm", params);
    }

    /**
     * Get user's DM conversations
     */
    public CompletableFuture<JsonObject> getDMConversations(String userId, boolean includeArchived) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not connected to OddSockets"));
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();

        JsonObject params = new JsonObject();
        params.addProperty("userId", userId);
        params.addProperty("includeArchived", includeArchived);

        client.once("dm_conversations", data -> future.complete((JsonObject) data));
        client.once("error", data -> {
            JsonObject error = (JsonObject) data;
            if ("get_dm_conversations".equals(error.get("event").getAsString())) {
                future.completeExceptionally(new RuntimeException(error.get("message").getAsString()));
            }
        });

        client.emit("get_dm_conversations", params);

        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    // ==================== NOTIFICATION EVENTS ====================

    /**
     * Subscribe to user notifications
     */
    public void subscribeNotifications(String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("userId", userId);

        client.emit("subscribe_notifications", params);
    }

    /**
     * Mark notification as read
     */
    public void markNotificationRead(String notificationId, String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("notificationId", notificationId);
        params.addProperty("userId", userId);

        client.emit("mark_notification_read", params);
    }

    /**
     * Mark all notifications as read
     */
    public void markAllNotificationsRead(String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("userId", userId);

        client.emit("mark_all_notifications_read", params);
    }

    /**
     * Clear all notifications
     */
    public void clearNotifications(String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("userId", userId);

        client.emit("clear_notifications", params);
    }

    /**
     * Get user notifications
     */
    public CompletableFuture<JsonObject> getNotifications(String userId, int limit, String status) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not connected to OddSockets"));
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();

        JsonObject params = new JsonObject();
        params.addProperty("userId", userId);
        params.addProperty("limit", limit);
        params.addProperty("status", status);

        client.once("notifications_data", data -> future.complete((JsonObject) data));
        client.once("error", data -> {
            JsonObject error = (JsonObject) data;
            if ("get_notifications".equals(error.get("event").getAsString())) {
                future.completeExceptionally(new RuntimeException(error.get("message").getAsString()));
            }
        });

        client.emit("get_notifications", params);

        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    // ==================== PRESENCE EVENTS ====================

    /**
     * Set user status
     */
    public void setStatus(String userId, String status) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("userId", userId);
        params.addProperty("status", status);

        client.emit("set_status", params);
    }

    /**
     * Set custom status
     */
    public void setCustomStatus(String userId, String emoji, String text, String expiresAt) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("userId", userId);
        params.addProperty("emoji", emoji);
        params.addProperty("text", text);
        if (expiresAt != null) {
            params.addProperty("expiresAt", expiresAt);
        }

        client.emit("set_custom_status", params);
    }

    /**
     * Clear custom status
     */
    public void clearCustomStatus(String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("userId", userId);

        client.emit("clear_custom_status", params);
    }

    /**
     * Enable Do Not Disturb
     */
    public void setDND(String userId, String until) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("userId", userId);
        if (until != null) {
            params.addProperty("until", until);
        }

        client.emit("set_dnd", params);
    }

    /**
     * Disable Do Not Disturb
     */
    public void clearDND(String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("userId", userId);

        client.emit("clear_dnd", params);
    }

    /**
     * Start typing indicator
     */
    public void startTyping(String userId, String channel) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("userId", userId);
        params.addProperty("channel", channel);

        client.emit("start_typing", params);
    }

    /**
     * Stop typing indicator
     */
    public void stopTyping(String userId, String channel) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("userId", userId);
        params.addProperty("channel", channel);

        client.emit("stop_typing", params);
    }

    /**
     * Get user presence information
     */
    public CompletableFuture<JsonObject> getUserPresence(List<String> userIds) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not connected to OddSockets"));
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();

        JsonObject params = new JsonObject();
        JsonArray userIdsArray = new JsonArray();
        userIds.forEach(userIdsArray::add);
        params.add("userIds", userIdsArray);

        client.once("user_presence_data", data -> future.complete((JsonObject) data));
        client.once("error", data -> {
            JsonObject error = (JsonObject) data;
            if ("get_user_presence".equals(error.get("event").getAsString())) {
                future.completeExceptionally(new RuntimeException(error.get("message").getAsString()));
            }
        });

        client.emit("get_user_presence", params);

        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    // ==================== MESSAGE EDITING EVENTS ====================

    /**
     * Edit a message
     */
    public void editMessage(String messageId, String channel, String newContent, String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("messageId", messageId);
        params.addProperty("channel", channel);
        params.addProperty("newContent", newContent);
        params.addProperty("userId", userId);

        client.emit("edit_message", params);
    }

    /**
     * Delete a message
     */
    public void deleteMessage(String messageId, String channel, String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("messageId", messageId);
        params.addProperty("channel", channel);
        params.addProperty("userId", userId);

        client.emit("delete_message", params);
    }

    /**
     * Pin message to channel
     */
    public void pinMessage(String messageId, String channel, String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("messageId", messageId);
        params.addProperty("channel", channel);
        params.addProperty("userId", userId);

        client.emit("pin_message", params);
    }

    /**
     * Unpin message from channel
     */
    public void unpinMessage(String messageId, String channel, String userId) {
        if (!client.isConnected()) {
            throw new IllegalStateException("Not connected to OddSockets");
        }

        JsonObject params = new JsonObject();
        params.addProperty("messageId", messageId);
        params.addProperty("channel", channel);
        params.addProperty("userId", userId);

        client.emit("unpin_message", params);
    }

    /**
     * Get pinned messages in channel
     */
    public CompletableFuture<JsonObject> getPinnedMessages(String channel) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not connected to OddSockets"));
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();

        JsonObject params = new JsonObject();
        params.addProperty("channel", channel);

        client.once("pinned_messages", data -> future.complete((JsonObject) data));
        client.once("error", data -> {
            JsonObject error = (JsonObject) data;
            if ("get_pinned_messages".equals(error.get("event").getAsString())) {
                future.completeExceptionally(new RuntimeException(error.get("message").getAsString()));
            }
        });

        client.emit("get_pinned_messages", params);

        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    // ==================== SEARCH EVENTS ====================

    /**
     * Search messages across all channels
     */
    public CompletableFuture<JsonObject> searchMessages(String query, String userId, int limit) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not connected to OddSockets"));
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();

        JsonObject params = new JsonObject();
        params.addProperty("query", query);
        params.addProperty("userId", userId);
        params.addProperty("limit", limit);

        client.once("search_results", data -> future.complete((JsonObject) data));
        client.once("error", data -> {
            JsonObject error = (JsonObject) data;
            if ("search_messages".equals(error.get("event").getAsString())) {
                future.completeExceptionally(new RuntimeException(error.get("message").getAsString()));
            }
        });

        client.emit("search_messages", params);

        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Filter messages by criteria
     */
    public CompletableFuture<JsonObject> filterMessages(Map<String, Object> filters) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not connected to OddSockets"));
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();

        client.once("filter_results", data -> future.complete((JsonObject) data));
        client.once("error", data -> {
            JsonObject error = (JsonObject) data;
            if ("filter_messages".equals(error.get("event").getAsString())) {
                future.completeExceptionally(new RuntimeException(error.get("message").getAsString()));
            }
        });

        client.emit("filter_messages", client.getGson().toJsonTree(filters));

        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Search within specific channel
     */
    public CompletableFuture<JsonObject> searchInChannel(String channel, String query, int limit) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not connected to OddSockets"));
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();

        JsonObject params = new JsonObject();
        params.addProperty("channel", channel);
        params.addProperty("query", query);
        params.addProperty("limit", limit);

        client.once("channel_search_results", data -> future.complete((JsonObject) data));
        client.once("error", data -> {
            JsonObject error = (JsonObject) data;
            if ("search_in_channel".equals(error.get("event").getAsString())) {
                future.completeExceptionally(new RuntimeException(error.get("message").getAsString()));
            }
        });

        client.emit("search_in_channel", params);

        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Search messages by user
     */
    public CompletableFuture<JsonObject> searchByUser(String userId, String query, int limit) {
        if (!client.isConnected()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not connected to OddSockets"));
        }

        CompletableFuture<JsonObject> future = new CompletableFuture<>();

        JsonObject params = new JsonObject();
        params.addProperty("userId", userId);
        if (query != null) {
            params.addProperty("query", query);
        }
        params.addProperty("limit", limit);

        client.once("user_search_results", data -> future.complete((JsonObject) data));
        client.once("error", data -> {
            JsonObject error = (JsonObject) data;
            if ("search_by_user".equals(error.get("event").getAsString())) {
                future.completeExceptionally(new RuntimeException(error.get("message").getAsString()));
            }
        });

        client.emit("search_by_user", params);

        return future.orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
}
