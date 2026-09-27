# OddSockets Java SDK

[![Maven Central](https://img.shields.io/maven-central/v/com.oddsockets/oddsockets-java-sdk.svg)](https://search.maven.org/artifact/com.oddsockets/oddsockets-java-sdk)
[![Javadoc](https://javadoc.io/badge2/com.oddsockets/oddsockets-java-sdk/javadoc.svg)](https://javadoc.io/doc/com.oddsockets/oddsockets-java-sdk)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Official Java SDK for OddSockets real-time messaging platform.

## Features

- **Enterprise Ready**: Built for production with comprehensive error handling
- **Spring Boot Friendly**: Drop the client into any Spring service as a bean
- **Enhanced Surface**: Slack-like reactions, threads, typing, presence and more
- **Low Latency**: Real-time delivery with automatic reconnection
- **Cost Effective**: No per-message pricing — a monthly message allowance
- **Cloud Native**: Perfect for microservices and enterprise applications

## 📦 Installation

### Maven

```xml
<dependency>
    <groupId>com.oddsockets</groupId>
    <artifactId>oddsockets-java-sdk</artifactId>
    <version>0.1.0-beta.1</version>
</dependency>
```

### Gradle

```gradle
implementation 'com.oddsockets:oddsockets-java-sdk:0.1.0-beta.1'
```

## 🏃‍♂️ Quick Start

### Basic Usage

```java
import com.oddsockets.OddSockets;
import com.oddsockets.Channel;
import com.oddsockets.Message;
import com.oddsockets.config.OddSocketsConfig;

public class BasicExample {
    public static void main(String[] args) {
        // Create client
        OddSocketsConfig config = OddSocketsConfig.builder()
            .apiKey("ak_live_1234567890abcdef")
            .managerUrl("https://connect.oddsockets.tyga.network")
            .userId("java-demo-user")
            .build();

        OddSockets client = new OddSockets(config);

        try {
            // Connect to OddSockets
            client.connect().get();

            // Create channel
            Channel channel = client.channel("my-channel");

            // Subscribe to messages
            channel.subscribe(message -> {
                System.out.println("Received: " + message.getData());
            }, SubscribeOptions.builder()
                .enablePresence(true)
                .retainHistory(true)
                .build()).get();

            // Publish a message
            channel.publish("Hello from Java! ☕", PublishOptions.builder()
                .metadata(Map.of("source", "java-sdk"))
                .storeInHistory(true)
                .build()).get();

            // Keep alive
            Thread.sleep(5000);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.disconnect();
        }
    }
}
```

### Spring Boot Integration

```java
@RestController
@RequiredArgsConstructor
public class MessageController {
    
    private final OddSockets oddSockets;
    
    @PostMapping("/send-message")
    public CompletableFuture<PublishResult> sendMessage(@RequestBody MessageRequest request) {
        Channel channel = oddSockets.channel(request.getChannel());
        
        return channel.publish(request.getMessage(), PublishOptions.builder()
            .metadata(Map.of("timestamp", Instant.now().toString()))
            .storeInHistory(true)
            .build());
    }
    
    @GetMapping("/channel/{name}/presence")
    public CompletableFuture<PresenceInfo> getPresence(@PathVariable String name) {
        return oddSockets.channel(name).getPresence();
    }
}
```

## Enhanced Features

Beyond core pub/sub, OddSockets ships a Slack-like **enhanced surface** — reactions,
typing indicators, threads, read receipts, presence/status, notifications, DMs,
channel management, message editing and search. It lives on `client.enhanced`.
The pattern is always the same:

1. **Send** an action with a `client.enhanced.*` method (camelCase).
2. **Receive** the paired broadcast with `client.on("<event>", handler)`.

```java
import com.google.gson.JsonObject;
import com.oddsockets.OddSockets;
import com.oddsockets.config.OddSocketsConfig;

OddSockets client = new OddSockets(OddSocketsConfig.builder()
    .apiKey("ak_live_1234567890abcdef")
    .userId("alice")
    .build());
client.connect().get();

// Receive-path: broadcasts from other users on the channel
client.on("user_typing",    data -> System.out.println("user is typing"));
client.on("reaction_added", data -> System.out.println("reaction added"));
client.on("thread_reply",   data -> System.out.println("new thread reply"));

// Send-path: enhanced actions over the live socket
client.enhanced.startTyping("alice", "room-42");
client.enhanced.addReaction("msg-1", "room-42", ":thumbsup:", "alice", "Alice");
client.enhanced.threadReply("room-42", "msg-1", "Replying in the thread", "alice", "Alice");
```

Each area exposes methods on `client.enhanced`; the worker broadcasts the paired
events which you handle with `client.on(...)`. Query methods (`get*`, `search*`)
return a `CompletableFuture<JsonObject>` that completes with the worker response.

| Area | Requests (`client.enhanced.*`) | Broadcast events (`client.on`) |
|------|--------------------------------|--------------------------------|
| Typing | `startTyping`, `stopTyping` | `user_typing`, `user_stopped_typing` |
| Reactions | `addReaction`, `removeReaction`, `getReactions` | `reaction_added`, `reaction_removed` |
| Threads | `threadReply`, `getThread`, `subscribeThread`, `followThread`, `markThreadRead` | `thread_reply`, `thread_subscribed`, `thread_followed`, `thread_read_updated` |
| Read receipts | `markRead`, `markAllRead`, `getUnreadCounts` | `user_read`, `unread_count_updated`, `all_marked_read` |
| Messages | `editMessage`, `deleteMessage`, `pinMessage`, `unpinMessage`, `getPinnedMessages`, `searchMessages` | `message_edited`, `message_deleted`, `message_pinned`, `message_unpinned` |
| Presence & status | `setStatus`, `setCustomStatus`, `setDND`, `getUserPresence` | `user_status_changed`, `custom_status_updated`, `dnd_status_changed` |
| Channels | `createChannel`, `updateChannel`, `archiveChannel`, `inviteToChannel`, `joinChannel`, `leaveChannel` | `channel_created`, `channel_updated`, `user_invited`, `user_joined_channel`, `user_left_channel` |
| DMs | `createDM`, `sendDM`, `getDMConversations` | `dm_created`, `dm_received` |
| Notifications | `subscribeNotifications`, `getNotifications`, `markNotificationRead`, `clearNotifications` | `notification`, `notification_read`, `notifications_cleared` |
| Search | `searchMessages`, `searchInChannel`, `searchByUser`, `filterMessages` | (future results) |

For any worker event not wrapped above, subscribe with the raw
`client.on("<event>", handler)` API — all enhanced broadcasts are forwarded onto
the client surface.

## Documentation

- **[SDK Documentation](https://docs.oddsockets.com/sdks/java)** - Guides and API overview
- **[Javadoc](https://javadoc.io/doc/com.oddsockets/oddsockets-java-sdk)** - Full API reference

## Examples

Explore the runnable examples:

- **[Basic Usage](src/main/java/com/oddsockets/examples/BasicExample.java)** - Simple messaging
- **[Enhanced Features](src/main/java/com/oddsockets/examples/EnhancedFeaturesExample.java)** - Reactions, threads, typing and more
- **[Two-client demo](demo/EnhancedDemo.java)** - End-to-end enhanced broadcast regression

## Configuration

### Client Options

```java
OddSocketsConfig config = OddSocketsConfig.builder()
    .apiKey("your-api-key")                    // Required (unless tokenProvider set): API key
    .tokenProvider(() -> future)               // Alternative to apiKey: async minted-token provider
    .tokenRefreshLeadMs(120000)                // Optional: refresh lead time before token expiry
    .managerUrl("manager-url")                 // Optional: Manager URL
    .userId("user-id")                         // Optional: User identifier
    .autoConnect(true)                         // Optional: Auto-connect on creation
    .reconnectAttempts(5)                      // Optional: Max reconnection attempts
    .heartbeatInterval(Duration.ofSeconds(30)) // Optional: Heartbeat interval
    .requestTimeout(Duration.ofSeconds(10))    // Optional: Request timeout
    .build();
```

### Manager URL

The manager URL is resolved in this order:

1. `managerUrl(...)` on the builder
2. the `ODDSOCKETS_MANAGER_URL` environment variable
3. `https://connect.oddsockets.tyga.network`

It must be an absolute `http://` or `https://` URL, otherwise the build fails with
`Invalid managerUrl: <value>`. Point it at a self-hosted or staging manager and the SDK
will use that endpoint and nothing else: if it is unreachable the connection fails with
the underlying error rather than falling back to the public endpoint.

### Token auth for game clients (`tokenProvider`)

Game and app clients that must not embed a long-lived API key can authenticate with
short-lived minted tokens instead. Supply an async `tokenProvider` (a
`Supplier<CompletableFuture<OddSocketsToken>>`) on the builder **instead of** `apiKey`.
The SDK calls it before every (re)connect, sends the minted token in place of the API
key, and refreshes the token ahead of its expiry — swapping the new credential into the
live connection without a forced reconnect.

```java
OddSocketsConfig config = OddSocketsConfig.builder()
    .tokenProvider(() -> CompletableFuture.supplyAsync(() -> {
        // Exchange your player's session/JWT for a scoped realtime token via
        // your backend or the OddSockets /v1/token front door.
        MintedToken minted = myBackend.mintRealtimeToken();
        OddSocketsToken token = new OddSocketsToken(minted.getToken());
        token.setExpiresAt(minted.getExpiresAt()); // ISO-8601, optional
        token.setExp(minted.getExp());             // epoch seconds, optional
        return token;
    }))
    .tokenRefreshLeadMs(120000) // refresh 2 min before expiry (default)
    .build();

OddSockets client = new OddSockets(config);
client.connect().get();

client.on(EventType.TOKEN_REFRESHED, data ->
    System.out.println("realtime token refreshed"));
```

`OddSocketsToken` carries the minted `token` (required) plus optional `expiresAt`
(ISO-8601), `exp` (epoch seconds), `baseUrl`, and `identity`. Only `token` is required;
the expiry fields let the SDK schedule an ahead-of-expiry refresh without decoding the
JWT itself.

### Channel Options

```java
// Subscribe with options
channel.subscribe(messageHandler, SubscribeOptions.builder()
    .enablePresence(true)                      // Enable presence tracking
    .retainHistory(true)                       // Retain message history
    .filterExpression("user.premium == true")  // Message filter expression
    .build());

// Publish with options
channel.publish(message, PublishOptions.builder()
    .ttl(3600)                                 // Time to live (seconds)
    .metadata(Map.of("priority", "high"))      // Additional metadata
    .storeInHistory(true)                      // Store in message history
    .build());
```

## Java Support

- Java 11+
- Spring Boot 2.7+ / 3.x
- Jakarta EE 9+

## Testing

```bash
# Run tests
./mvnw test

# Run tests with coverage
./mvnw test jacoco:report

# Run integration tests
./mvnw test -Dtest.profile=integration

# Run performance tests
./mvnw test -Dtest.profile=performance
```

## Building

```bash
# Build
./mvnw clean compile

# Package
./mvnw clean package

# Install to local repository
./mvnw clean install

# Deploy to Maven Central
./mvnw clean deploy -P release
```

## Performance

- **Automatic failover** - the cluster reroutes you if a node goes away, and the
  SDK reconnects and resubscribes on your behalf
- **No per-message pricing** - you buy a monthly message allowance, not individual sends
- **32 KB maximum message size** - split anything larger, or publish a reference to it
- **Non-blocking** - publishes return `CompletableFuture`, so they never block your caller

Uptime SLA is per plan (Pro 99.9%, Scale 99.95%, Enterprise 99.999%) — see
[pricing](https://oddsockets.com/#pricing) for the current commitment.

## Security

- **End-to-end encryption** available
- **API key authentication** with fine-grained permissions
- **Rate limiting** and abuse protection
- **GDPR compliant** data handling

## Framework Integrations

### Spring Boot Auto-Configuration

```yaml
# application.yml
oddsockets:
  api-key: ak_live_1234567890abcdef
  manager-url: https://connect.oddsockets.tyga.network
  user-id: spring-boot-user
  auto-connect: true
  reconnect-attempts: 5
  heartbeat-interval: 30s
```

```java
@Component
@RequiredArgsConstructor
public class MessageService {
    
    private final OddSockets oddSockets;
    
    @EventListener
    public void handleApplicationReady(ApplicationReadyEvent event) {
        Channel channel = oddSockets.channel("system-events");
        
        channel.subscribe(message -> {
            // Handle system messages
            log.info("System message: {}", message.getData());
        });
    }
}
```

### Microservices Architecture

```java
@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class MessageController {
    
    private final OddSockets oddSockets;
    
    @PostMapping("/broadcast")
    public CompletableFuture<List<PublishResult>> broadcast(@RequestBody BroadcastRequest request) {
        List<CompletableFuture<PublishResult>> futures = request.getChannels().stream()
            .map(channelName -> oddSockets.channel(channelName)
                .publish(request.getMessage(), PublishOptions.builder()
                    .metadata(Map.of("broadcast_id", request.getBroadcastId()))
                    .storeInHistory(true)
                    .build()))
            .collect(Collectors.toList());
        
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList()));
    }
}
```

## Other SDKs

OddSockets is available in multiple languages:

- **[JavaScript SDK](../javascript/)** - Browser + Node.js, TypeScript ready
- **[Python SDK](../python/)** - AsyncIO support, Django/Flask integrations
- **[Go SDK](../go/)** - High performance, goroutines and channels
- **[C# SDK](../csharp/)** - .NET Core/Framework, Azure integrations
- **[Swift SDK](../swift/)** - iOS native, Combine framework
- **[Kotlin SDK](../kotlin/)** - Android native, coroutines support

## Get an API Key

AI agents can sign up with a verified email in two steps — no dashboard, no human required.

**Step 1:** Request a verification code
```bash
curl -X POST https://oddsockets.com/api/agent-signup \
  -H "Content-Type: application/json" \
  -d '{"email": "you@example.com", "agentName": "my-agent", "platform": "java"}'
```

**Step 2:** Verify the 6-digit code from your email and get your API key
```bash
curl -X POST https://oddsockets.com/api/agent-signup/verify \
  -H "Content-Type: application/json" \
  -d '{"email": "you@example.com", "code": "123456", "agentName": "my-agent"}'
```

## Plans

No free tier — every plan starts with a 7-day free trial.

| | Starter | Pro | Scale | Enterprise |
|---|---|---|---|---|
| **Price** | $29/mo | $99/mo | $299/mo | Contact sales |
| **Messages/mo** | 5M | 25M | 100M | Unlimited |
| **Peak connections** | 200 | 1,000 | 5,000 | Unlimited |
| **MAU** | Unlimited | Unlimited | Unlimited | Unlimited |
| **Extra messages** | $2.50/M | $1.60/M | $1.00/M | Included |

Current pricing: [oddsockets.com/#pricing](https://oddsockets.com/#pricing).

All limits are enforced in real time.

## Get Accredited

<a href="https://tyga.games/accreditation"><img src="https://prodmedia.tyga.host/public/tyga.cloud/landing/tyga.games/tygagames-black-words.svg" alt="tyga.games accreditation" height="44"></a>

Prove you can build and operate real-time features on OddSockets — channels, presence, pub/sub, delivery guarantees and production liveops — on the stack itself. Three tiers (**TCU / TCA / TCP**), certified through **tyga.games** and delivered on ClassaaS.

[**Get accredited on tyga.games →**](https://tyga.games/accreditation)

## Support

- [Documentation](https://docs.oddsockets.com/sdks/java)
- [Issue Tracker](https://github.com/jyswee/oddsockets-java-sdk/issues)
- [Email Support](mailto:support@oddsockets.com)

## License

MIT License - Copyright (c) 2026 Joe Wee, Tyga.Cloud Ltd. See [LICENSE](LICENSE) for details.
