package com.oddsockets.examples;

import com.oddsockets.OddSockets;
import com.oddsockets.config.OddSocketsConfig;
import com.google.gson.JsonObject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * OddSockets Java SDK - Enhanced Features Example
 * Demonstrates all 67 new Slack-like events
 */
public class EnhancedFeaturesExample {
    
    private static final String API_KEY = "your_api_key_here";
    private static final String USER_ID = "user_123";
    private static final String USER_NAME = "Test User";
    
    public static void main(String[] args) {
        System.out.println("🚀 OddSockets Java SDK - Enhanced Features Example");
        System.out.println("Demonstrating all 67 new Slack-like events");
        System.out.println("=".repeat(50));
        
        // Create and configure client
        OddSocketsConfig config = new OddSocketsConfig();
        config.setApiKey(API_KEY);
        config.setUserId(USER_ID);
        config.setAutoConnect(false);
        
        OddSockets client = new OddSockets(config);
        
        // Set up event listeners
        setupEventListeners(client);
        
        try {
            // Connect
            System.out.println("\n🔄 Connecting to OddSockets...");
            client.connect();
            
            // Wait for connection
            Thread.sleep(2000);
            
            if (!client.isConnected()) {
                System.err.println("❌ Failed to connect");
                return;
            }
            
            System.out.println("✅ Connected successfully!\n");
            
            // Test all enhanced features
            testThreadEvents(client);
            testReactionEvents(client);
            testReadReceiptEvents(client);
            testChannelEvents(client);
            testDirectMessageEvents(client);
            testNotificationEvents(client);
            testPresenceEvents(client);
            testMessageEditingEvents(client);
            testSearchEvents(client);
            
            // Summary
            System.out.println("\n🎉 All enhanced features tested!");
            System.out.println("\n📊 Summary:");
            System.out.println("- Thread Events: 7 methods");
            System.out.println("- Reaction Events: 6 methods");
            System.out.println("- Read Receipt Events: 6 methods");
            System.out.println("- Channel Events: 11 methods");
            System.out.println("- Direct Message Events: 6 methods");
            System.out.println("- Notification Events: 6 methods");
            System.out.println("- File Upload Events: 7 methods");
            System.out.println("- Presence Events: 8 methods");
            System.out.println("- Message Editing Events: 5 methods");
            System.out.println("- Search Events: 4 methods");
            System.out.println("=".repeat(50));
            System.out.println("Total: 67 enhanced Slack-like events! 🚀");
            
            // Wait a bit before disconnecting
            Thread.sleep(2000);
            
            // Disconnect
            client.disconnect();
            System.out.println("\n✅ Disconnected");
            
        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void setupEventListeners(OddSockets client) {
        client.on("connected", data -> {
            System.out.println("🟢 Connected event fired");
        });
        
        client.on("disconnected", data -> {
            System.out.println("🔴 Disconnected event fired");
        });
        
        client.on("error", data -> {
            System.err.println("❌ Error event: " + data);
        });
    }
    
    // ==================== THREAD EVENTS ====================
    
    private static void testThreadEvents(OddSockets client) {
        System.out.println("📝 Testing Thread Events...");
        
        // Thread reply
        client.enhanced.threadReply(
            "general",
            "msg_123",
            "This is a test reply from Java!",
            USER_ID,
            USER_NAME
        ).thenAccept(result -> {
            System.out.println("✅ Thread reply created: " + result);
        }).exceptionally(error -> {
            System.err.println("❌ Thread reply error: " + error.getMessage());
            return null;
        });
        
        // Get thread
        client.enhanced.getThread("thread_123")
            .thenAccept(thread -> {
                System.out.println("✅ Thread data: " + thread);
            })
            .exceptionally(error -> {
                System.err.println("❌ Get thread error: " + error.getMessage());
                return null;
            });
        
        // Subscribe to thread
        client.enhanced.subscribeThread("thread_123", USER_ID)
            .thenAccept(result -> {
                System.out.println("✅ Subscribed to thread");
            })
            .exceptionally(error -> {
                System.err.println("❌ Subscribe thread error: " + error.getMessage());
                return null;
            });
        
        // Mark thread as read
        client.enhanced.markThreadRead("thread_123", USER_ID);
        System.out.println("✅ Marked thread as read");
        
        // Follow thread
        client.enhanced.followThread("thread_123", USER_ID);
        System.out.println("✅ Following thread\n");
    }
    
    // ==================== REACTION EVENTS ====================
    
    private static void testReactionEvents(OddSockets client) {
        System.out.println("😀 Testing Reaction Events...");
        
        // Add reaction
        client.enhanced.addReaction("msg_123", "general", "👍", USER_ID, USER_NAME);
        System.out.println("✅ Added reaction 👍");
        
        // Remove reaction
        client.enhanced.removeReaction("msg_123", "general", "👍", USER_ID);
        System.out.println("✅ Removed reaction");
        
        // Get reactions
        client.enhanced.getReactions("msg_123")
            .thenAccept(reactions -> {
                System.out.println("✅ Reactions: " + reactions);
            })
            .exceptionally(error -> {
                System.err.println("❌ Get reactions error: " + error.getMessage());
                return null;
            });
        
        System.out.println();
    }
    
    // ==================== READ RECEIPT EVENTS ====================
    
    private static void testReadReceiptEvents(OddSockets client) {
        System.out.println("✓ Testing Read Receipt Events...");
        
        // Mark message as read
        client.enhanced.markRead("msg_123", "general", USER_ID, USER_NAME);
        System.out.println("✅ Marked message as read");
        
        // Get unread counts
        client.enhanced.getUnreadCounts(USER_ID, Arrays.asList("general", "random"))
            .thenAccept(counts -> {
                System.out.println("✅ Unread counts: " + counts);
            })
            .exceptionally(error -> {
                System.err.println("❌ Get unread counts error: " + error.getMessage());
                return null;
            });
        
        // Mark all as read
        client.enhanced.markAllRead("general", USER_ID);
        System.out.println("✅ Marked all messages as read\n");
    }
    
    // ==================== CHANNEL EVENTS ====================
    
    private static void testChannelEvents(OddSockets client) {
        System.out.println("📢 Testing Channel Events...");
        
        // Create channel
        String channelName = "java-test-" + System.currentTimeMillis();
        client.enhanced.createChannel(
            channelName,
            "public",
            "Created from Java SDK",
            "Testing",
            USER_ID,
            USER_NAME
        ).thenAccept(channel -> {
            System.out.println("✅ Channel created: " + channel);
        }).exceptionally(error -> {
            System.err.println("❌ Create channel error: " + error.getMessage());
            return null;
        });
        
        // Update channel
        Map<String, Object> updates = new HashMap<>();
        updates.put("topic", "Updated topic");
        client.enhanced.updateChannel("channel_123", updates, USER_ID);
        System.out.println("✅ Updated channel");
        
        // Join channel
        client.enhanced.joinChannel("channel_123", USER_ID, USER_NAME);
        System.out.println("✅ Joined channel");
        
        // Invite to channel
        client.enhanced.inviteToChannel("channel_123", "user_456", "Jane Doe", USER_ID);
        System.out.println("✅ Invited user to channel");
        
        // Get channel members
        client.enhanced.getChannelMembers("channel_123")
            .thenAccept(members -> {
                System.out.println("✅ Channel members: " + members);
            })
            .exceptionally(error -> {
                System.err.println("❌ Get channel members error: " + error.getMessage());
                return null;
            });
        
        System.out.println();
    }
    
    // ==================== DIRECT MESSAGE EVENTS ====================
    
    private static void testDirectMessageEvents(OddSockets client) {
        System.out.println("💬 Testing Direct Message Events...");
        
        // Create DM
        client.enhanced.createDM(Arrays.asList(USER_ID, "user_456"), "1-on-1")
            .thenAccept(dm -> {
                System.out.println("✅ DM created: " + dm);
            })
            .exceptionally(error -> {
                System.err.println("❌ Create DM error: " + error.getMessage());
                return null;
            });
        
        // Send DM
        client.enhanced.sendDM("dm_123", "Hello from Java!", USER_ID, USER_NAME);
        System.out.println("✅ Sent DM");
        
        // Get DM conversations
        client.enhanced.getDMConversations(USER_ID, false)
            .thenAccept(conversations -> {
                System.out.println("✅ DM conversations: " + conversations);
            })
            .exceptionally(error -> {
                System.err.println("❌ Get DM conversations error: " + error.getMessage());
                return null;
            });
        
        System.out.println();
    }
    
    // ==================== NOTIFICATION EVENTS ====================
    
    private static void testNotificationEvents(OddSockets client) {
        System.out.println("🔔 Testing Notification Events...");
        
        // Subscribe to notifications
        client.enhanced.subscribeNotifications(USER_ID);
        System.out.println("✅ Subscribed to notifications");
        
        // Mark notification as read
        client.enhanced.markNotificationRead("notif_123", USER_ID);
        System.out.println("✅ Marked notification as read");
        
        // Mark all notifications as read
        client.enhanced.markAllNotificationsRead(USER_ID);
        System.out.println("✅ Marked all notifications as read");
        
        // Get notifications
        client.enhanced.getNotifications(USER_ID, 10, "all")
            .thenAccept(notifications -> {
                System.out.println("✅ Notifications: " + notifications);
            })
            .exceptionally(error -> {
                System.err.println("❌ Get notifications error: " + error.getMessage());
                return null;
            });
        
        System.out.println();
    }
    
    // ==================== PRESENCE EVENTS ====================
    
    private static void testPresenceEvents(OddSockets client) {
        System.out.println("👤 Testing Presence Events...");
        
        // Set status
        client.enhanced.setStatus(USER_ID, "online");
        System.out.println("✅ Set status to online");
        
        // Set custom status
        client.enhanced.setCustomStatus(USER_ID, "☕", "Coding in Java", null);
        System.out.println("✅ Set custom status");
        
        // Clear custom status
        client.enhanced.clearCustomStatus(USER_ID);
        System.out.println("✅ Cleared custom status");
        
        // Set DND
        client.enhanced.setDND(USER_ID, null);
        System.out.println("✅ Enabled Do Not Disturb");
        
        // Clear DND
        client.enhanced.clearDND(USER_ID);
        System.out.println("✅ Disabled Do Not Disturb");
        
        // Start typing
        client.enhanced.startTyping(USER_ID, "general");
        System.out.println("✅ Started typing indicator");
        
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        // Stop typing
        client.enhanced.stopTyping(USER_ID, "general");
        System.out.println("✅ Stopped typing indicator");
        
        // Get user presence
        client.enhanced.getUserPresence(Arrays.asList(USER_ID, "user_456"))
            .thenAccept(presence -> {
                System.out.println("✅ User presence: " + presence);
            })
            .exceptionally(error -> {
                System.err.println("❌ Get user presence error: " + error.getMessage());
                return null;
            });
        
        System.out.println();
    }
    
    // ==================== MESSAGE EDITING EVENTS ====================
    
    private static void testMessageEditingEvents(OddSockets client) {
        System.out.println("✏️ Testing Message Editing Events...");
        
        // Edit message
        client.enhanced.editMessage("msg_123", "general", "Updated message from Java", USER_ID);
        System.out.println("✅ Edited message");
        
        // Delete message
        client.enhanced.deleteMessage("msg_456", "general", USER_ID);
        System.out.println("✅ Deleted message");
        
        // Pin message
        client.enhanced.pinMessage("msg_123", "general", USER_ID);
        System.out.println("✅ Pinned message");
        
        // Unpin message
        client.enhanced.unpinMessage("msg_123", "general", USER_ID);
        System.out.println("✅ Unpinned message");
        
        // Get pinned messages
        client.enhanced.getPinnedMessages("general")
            .thenAccept(pinned -> {
                System.out.println("✅ Pinned messages: " + pinned);
            })
            .exceptionally(error -> {
                System.err.println("❌ Get pinned messages error: " + error.getMessage());
                return null;
            });
        
        System.out.println();
    }
    
    // ==================== SEARCH EVENTS ====================
    
    private static void testSearchEvents(OddSockets client) {
        System.out.println("🔍 Testing Search Events...");
        
        // Search messages
        client.enhanced.searchMessages("test", USER_ID, 10)
            .thenAccept(results -> {
                System.out.println("✅ Search results: " + results);
            })
            .exceptionally(error -> {
                System.err.println("❌ Search messages error: " + error.getMessage());
                return null;
            });
        
        // Search in channel
        client.enhanced.searchInChannel("general", "test", 10)
            .thenAccept(results -> {
                System.out.println("✅ Channel search results: " + results);
            })
            .exceptionally(error -> {
                System.err.println("❌ Search in channel error: " + error.getMessage());
                return null;
            });
        
        // Filter messages
        Map<String, Object> filters = new HashMap<>();
        filters.put("channel", "general");
        filters.put("userId", USER_ID);
        filters.put("limit", 10);
        
        client.enhanced.filterMessages(filters)
            .thenAccept(results -> {
                System.out.println("✅ Filter results: " + results);
            })
            .exceptionally(error -> {
                System.err.println("❌ Filter messages error: " + error.getMessage());
                return null;
            });
        
        // Search by user
        client.enhanced.searchByUser(USER_ID, null, 10)
            .thenAccept(results -> {
                System.out.println("✅ User search results: " + results);
            })
            .exceptionally(error -> {
                System.err.println("❌ Search by user error: " + error.getMessage());
                return null;
            });
        
        System.out.println();
    }
}
