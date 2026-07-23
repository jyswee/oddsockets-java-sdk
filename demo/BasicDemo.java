// BasicDemo runs a single-client pub/sub round-trip for the OddSockets Java SDK
// against the LIVE platform. It subscribes to a channel, publishes a nonce'd
// message, and asserts the SAME message comes back through the worker broadcast
// path - no mocks, no local echo.
//
//   java -cp <fat-jar>:out BasicDemo

import com.oddsockets.OddSockets;
import com.oddsockets.Channel;
import com.oddsockets.config.OddSocketsConfig;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BasicDemo {

    public static void main(String[] args) throws Exception {
        String apiKey = System.getenv("ODDSOCKETS_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            System.err.println("ODDSOCKETS_API_KEY is not set.");
            System.exit(1);
        }

        System.out.println("Connecting to OddSockets...");
        OddSockets client = new OddSockets(OddSocketsConfig.builder()
                .apiKey(apiKey)
                .userId("basic-demo")
                .autoConnect(false)
                .build());
        client.connect().get(20, TimeUnit.SECONDS);
        System.out.println("Connected.");

        OddSockets.WorkerInfo worker = client.getWorkerInfo();
        System.out.println("Worker: " + (worker != null ? worker.getWorkerId() : "unknown"));

        String channelName = "demo-" + UUID.randomUUID().toString().substring(0, 12);
        System.out.println("Using channel: " + channelName);

        String nonce = UUID.randomUUID().toString();
        CountDownLatch got = new CountDownLatch(1);

        Channel channel = client.channel(channelName);
        channel.subscribe(envelope -> {
            Object inner = envelope.get("message");
            if (inner instanceof Map<?, ?> m && nonce.equals(String.valueOf(m.get("nonce")))) {
                got.countDown();
            }
        }).get(20, TimeUnit.SECONDS);
        System.out.println("Subscribed.");

        System.out.println("Publishing message...");
        OddSockets.PublishResult result = channel.publish(Map.of("text", "hello", "nonce", nonce))
                .get(20, TimeUnit.SECONDS);
        if (!result.isSuccess()) {
            System.err.println("Publish failed: " + result.getError());
            System.exit(1);
        }
        System.out.println("Published message id: " + result.getMessageId());

        if (!got.await(20, TimeUnit.SECONDS)) {
            System.err.println("TIMEOUT - did not receive our own message back");
            System.exit(1);
        }

        System.out.println("OK - round-trip verified");
        client.close();
        System.exit(0);
    }
}
