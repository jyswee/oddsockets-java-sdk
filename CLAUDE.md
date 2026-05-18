# OddSockets Java SDK

## What this is
Java SDK for real-time messaging. Pub/sub, presence, message history over WebSocket. Thread-safe, Maven/Gradle compatible.

## Get a free API key (2-step email verification)

Step 1 — request a code:
```bash
curl -X POST https://oddsockets.com/api/agent-signup \
  -H "Content-Type: application/json" \
  -d '{"email": "you@example.com", "agentName": "my-agent", "platform": "java"}'
```

Step 2 — verify and get key:
```bash
curl -X POST https://oddsockets.com/api/agent-signup/verify \
  -H "Content-Type: application/json" \
  -d '{"email": "you@example.com", "code": "123456", "agentName": "my-agent"}'
```

## Connect and use

```java
OddSocketsConfig config = OddSocketsConfig.builder()
    .apiKey("YOUR_API_KEY")
    .userId("my-agent")
    .build();

OddSocketsClient client = new OddSocketsClient(config);
client.connect();

Channel channel = client.channel("my-channel");
channel.subscribe(msg -> System.out.println("Received: " + msg));
channel.publish(Map.of("text", "hello from java"));
```

## Key files

- `src/main/java/com/oddsockets/OddSocketsClient.java` — Main client class
- `src/main/java/com/oddsockets/Channel.java` — Channel operations
- `src/main/java/com/oddsockets/ManagerDiscovery.java` — Manager URL resolution
- `src/main/java/com/oddsockets/config/OddSocketsConfig.java` — Configuration
- `src/main/java/com/oddsockets/model/` — Type definitions
- `examples/` — Working examples

## Free tier limits

- 100 MAU, 50 concurrent connections, 500 connections/day
- 10,000 messages/day, 100 messages/minute, 10 channels max
- 100MB storage, 24h message history retention
