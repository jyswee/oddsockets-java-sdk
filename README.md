# OddSockets Java SDK

[![Maven Central](https://img.shields.io/maven-central/v/com.oddsockets/oddsockets-java-sdk.svg)](https://search.maven.org/artifact/com.oddsockets/oddsockets-java-sdk)
[![Javadoc](https://javadoc.io/badge2/com.oddsockets/oddsockets-java-sdk/javadoc.svg)](https://javadoc.io/doc/com.oddsockets/oddsockets-java-sdk)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Official Java SDK for OddSockets real-time messaging platform.

## Features

- **Enterprise Ready**: Built for production with comprehensive error handling
- **Spring Boot Integration**: Native Spring Boot starter and auto-configuration
- **Reactive Streams**: Full support for Project Reactor and RxJava
- **PubNub Compatible**: Drop-in replacement for PubNub Java SDK
- **High Performance**: 50% lower latency than PubNub
- **Cost Effective**: No per-message pricing, no message size limits
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

### Spring Boot Starter

```xml
<dependency>
    <groupId>com.oddsockets</groupId>
    <artifactId>oddsockets-spring-boot-starter</artifactId>
    <version>0.1.0-beta.1</version>
</dependency>
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
            .managerUrl("https://manager1.oddsockets.tyga.network")
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

### Reactive Streams (Project Reactor)

```java
import com.oddsockets.reactive.ReactiveOddSockets;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactiveExample {
    public static void main(String[] args) {
        ReactiveOddSockets client = ReactiveOddSockets.create(
            OddSocketsConfig.builder()
                .apiKey("ak_live_1234567890abcdef")
                .build()
        );

        client.connect()
            .then(client.channel("reactive-channel")
                .subscribe(SubscribeOptions.builder()
                    .enablePresence(true)
                    .build()))
            .thenMany(client.channel("reactive-channel")
                .messages())
            .doOnNext(message -> 
                System.out.println("Received: " + message.getData()))
            .take(10)
            .then(client.disconnect())
            .block();
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

### PubNub Migration

```java
import com.oddsockets.pubnub.PubNub;
import com.oddsockets.pubnub.PNConfiguration;
import com.oddsockets.pubnub.callbacks.SubscribeCallback;

public class PubNubMigration {
    public static void main(String[] args) {
        // Drop-in replacement for PubNub
        PNConfiguration config = new PNConfiguration();
        config.setPublishKey("ak_live_1234567890abcdef");
        config.setSubscribeKey("ak_live_1234567890abcdef");
        config.setUserId("user123");

        PubNub pubnub = new PubNub(config);

        // Subscribe
        pubnub.addListener(new SubscribeCallback() {
            @Override
            public void message(PubNub pubnub, PNMessageResult message) {
                System.out.println("Message: " + message.getMessage());
            }
        });

        pubnub.subscribe()
            .channels(Arrays.asList("my-channel"))
            .execute();

        // Publish
        pubnub.publish()
            .channel("my-channel")
            .message("Hello from Java!")
            .async((result, status) -> {
                System.out.println("Published: " + result.getTimetoken());
            });
    }
}
```

## Documentation

- **[API Reference](docs/api-reference.md)** - Complete Javadoc documentation
- **[Getting Started](docs/getting-started.md)** - Detailed setup guide
- **[Spring Boot Guide](docs/spring-boot.md)** - Spring Boot integration
- **[Migration Guide](docs/migration-guide.md)** - Migrate from PubNub
- **[Troubleshooting](docs/troubleshooting.md)** - Common issues and solutions

## Examples

Explore our comprehensive examples:

- **[Basic Usage](examples/basic/src/main/java/BasicExample.java)** - Simple messaging
- **[Spring Boot](examples/spring-boot/)** - Complete Spring Boot application
- **[Reactive Streams](examples/reactive/src/main/java/ReactiveExample.java)** - Project Reactor integration
- **[PubNub Migration](examples/pubnub-migration/src/main/java/MigrationExample.java)** - Migration example
- **[Microservices](examples/microservices/)** - Service-to-service messaging

## Configuration

### Client Options

```java
OddSocketsConfig config = OddSocketsConfig.builder()
    .apiKey("your-api-key")                    // Required: Your OddSockets API key
    .managerUrl("manager-url")                 // Optional: Manager URL
    .userId("user-id")                         // Optional: User identifier
    .autoConnect(true)                         // Optional: Auto-connect on creation
    .reconnectAttempts(5)                      // Optional: Max reconnection attempts
    .heartbeatInterval(Duration.ofSeconds(30)) // Optional: Heartbeat interval
    .requestTimeout(Duration.ofSeconds(10))    // Optional: Request timeout
    .build();
```

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
- Project Reactor 3.x
- RxJava 3.x
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

OddSockets Java SDK delivers superior performance:

- **50% lower latency** compared to PubNub
- **99.9% uptime** with automatic failover
- **Unlimited message size** - no artificial limits
- **High throughput** - handle millions of messages with reactive streams

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
  manager-url: https://manager1.oddsockets.tyga.network
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

### Reactive Streams Integration

```java
@Service
@RequiredArgsConstructor
public class ReactiveMessageService {
    
    private final ReactiveOddSockets oddSockets;
    
    public Flux<Message> getMessages(String channelName) {
        return oddSockets.channel(channelName)
            .subscribe(SubscribeOptions.builder()
                .enablePresence(true)
                .build())
            .thenMany(oddSockets.channel(channelName).messages());
    }
    
    public Mono<PublishResult> publishMessage(String channelName, Object message) {
        return oddSockets.channel(channelName)
            .publish(message, PublishOptions.builder()
                .storeInHistory(true)
                .build());
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

## Get a Free API Key

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

| | Free | Starter | Pro |
|---|---|---|---|
| **Price** | $0/mo | $49.99/mo | $299/mo |
| **MAU** | 100 | 1,000 | 50,000 |
| **Concurrent connections** | 50 | 1,000 | Unlimited |
| **Messages/day** | 10,000 | 4,320,000 | Unlimited |
| **Messages/minute** | 100 | 3,000 | Unlimited |
| **Channels** | 10 | Unlimited | Unlimited |
| **Storage** | 100MB (24h) | 50GB (6 months) | Unlimited |

All limits are enforced in real time.

## Support

- [Documentation](https://docs.oddsockets.com/sdks/java)
- [Issue Tracker](https://github.com/jyswee/oddsockets-java-sdk/issues)
- [Email Support](mailto:support@oddsockets.com)

## License

MIT License - Copyright (c) 2026 Joe Wee, Tyga.Cloud Ltd. See [LICENSE](LICENSE) for details.
