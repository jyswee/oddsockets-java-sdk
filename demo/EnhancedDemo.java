// EnhancedDemo runs a two-client enhanced-events regression for the OddSockets
// Java SDK. It proves the RECEIVE path for enhanced (Slack-like) events: an
// action fired by one client (bob) is broadcast by the worker and surfaces on
// the OTHER client's public event stream (alice).
//
// Because publisher and subscriber are separate connections, an event reaching
// alice can only have travelled through the OddSockets worker - an honest
// end-to-end test, no local echo:
//
//   bob -> enhanced.startTyping  -> alice client.on("user_typing")
//   bob -> enhanced.addReaction  -> alice client.on("reaction_added")
//
//   java -cp <fat-jar>:out EnhancedDemo

import com.google.gson.JsonObject;
import com.oddsockets.Channel;
import com.oddsockets.OddSockets;
import com.oddsockets.config.OddSocketsConfig;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class EnhancedDemo {

    private static OddSockets connect(String apiKey, String userId) throws Exception {
        OddSockets client = new OddSockets(OddSocketsConfig.builder()
                .apiKey(apiKey)
                .userId(userId)
                .autoConnect(false)
                .build());
        client.connect().get(20, TimeUnit.SECONDS);
        return client;
    }

    public static void main(String[] args) throws Exception {
        String apiKey = System.getenv("ODDSOCKETS_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            System.err.println("ODDSOCKETS_API_KEY is not set.");
            System.exit(1);
        }

        System.out.println("[connect] connecting both clients...");
        OddSockets alice = connect(apiKey, "alice");
        OddSockets bob = connect(apiKey, "bob");

        System.out.println("[connect] alice = " + alice.getState() + ", bob = " + bob.getState());

        String channelName = "enh-" + UUID.randomUUID().toString().substring(0, 10);

        CountDownLatch typingLatch = new CountDownLatch(1);
        CountDownLatch reactionLatch = new CountDownLatch(1);

        // alice listens for the enhanced broadcasts on her PUBLIC event stream.
        alice.on("user_typing", data -> {
            if (data instanceof JsonObject m && "bob".equals(asString(m, "userId"))) {
                System.out.println("[alice] received 'user_typing' from bob (channel "
                        + asString(m, "channel") + ") - broadcast round-trip.");
                typingLatch.countDown();
            }
        });
        alice.on("reaction_added", data -> {
            if (data instanceof JsonObject m && m.has("emoji")) {
                System.out.println("[alice] received 'reaction_added' (" + asString(m, "emoji")
                        + ") from " + asString(m, "userId") + " - broadcast round-trip.");
                reactionLatch.countDown();
            }
        });

        // Both clients join the same room.
        Channel aliceCh = alice.channel(channelName);
        Channel bobCh = bob.channel(channelName);
        aliceCh.subscribe(msg -> { }, Channel.SubscribeOptions.builder().enablePresence(true).build())
                .get(20, TimeUnit.SECONDS);
        bobCh.subscribe(msg -> { }, Channel.SubscribeOptions.builder().enablePresence(true).build())
                .get(20, TimeUnit.SECONDS);
        System.out.println("[both] subscribed to " + channelName);

        // Let room membership settle, then fire enhanced actions from bob.
        Thread.sleep(500);

        System.out.println("[bob] enhanced.startTyping(bob) ...");
        bob.enhanced.startTyping("bob", channelName);

        OddSockets.PublishResult result = bobCh.publish(Map.of("text", "react to me"))
                .get(20, TimeUnit.SECONDS);
        System.out.println("[bob] published messageId=" + result.getMessageId()
                + ", enhanced.addReaction :thumbsup: ...");
        bob.enhanced.addReaction(result.getMessageId(), channelName, ":thumbsup:", "bob", "Bob");

        boolean gotTyping = typingLatch.await(20, TimeUnit.SECONDS);
        boolean gotReaction = reactionLatch.await(20, TimeUnit.SECONDS);

        if (!gotTyping || !gotReaction) {
            System.err.println("\nTIMEOUT - enhanced broadcast not received (typing=" + gotTyping
                    + " reaction=" + gotReaction + ")");
            System.exit(1);
        }

        System.out.println("\nOK - enhanced broadcast receive-path verified (user_typing + reaction_added)");
        alice.close();
        bob.close();
        System.exit(0);
    }

    private static String asString(JsonObject o, String key) {
        return o.has(key) && o.get(key).isJsonPrimitive() ? o.get(key).getAsString() : null;
    }
}
