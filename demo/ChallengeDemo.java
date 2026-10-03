// ChallengeDemo runs an HONEST two-client challenge-lifecycle regression for the
// OddSockets Java SDK against the LIVE worker (v1.2 wire contract). Two separate
// connections (alice + bob), DISTINCT userId, SAME apiKey (shared owner scope),
// both subscribed to 'lobby'. Because publisher and subscriber are separate
// connections, every broadcast reaching the other client can only have travelled
// through the OddSockets worker - no local echo.

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.oddsockets.Channel;
import com.oddsockets.OddSockets;
import com.oddsockets.config.OddSocketsConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ChallengeDemo {

    static int pass = 0, fail = 0;

    static void check(String name, boolean ok, String detail) {
        System.out.println((ok ? "  PASS " : "  FAIL ") + name + (detail == null ? "" : "  -> " + detail));
        if (ok) pass++; else fail++;
    }

    private static OddSockets connect(String apiKey, String managerUrl, String userId) throws Exception {
        OddSockets client = new OddSockets(OddSocketsConfig.builder()
                .apiKey(apiKey)
                .managerUrl(managerUrl)
                .userId(userId)
                .autoConnect(false)
                .build());
        client.connect().get(20, TimeUnit.SECONDS);
        return client;
    }

    private static String workerId(OddSockets client) {
        OddSockets.WorkerInfo info = client.getWorkerInfo();
        return info != null ? info.getWorkerId() : "unknown";
    }

    static String s(JsonObject o, String k) {
        return o != null && o.has(k) && o.get(k).isJsonPrimitive() ? o.get(k).getAsString() : null;
    }
    static Double num(JsonObject o, String k) {
        return o != null && o.has(k) && o.get(k).isJsonPrimitive() ? o.get(k).getAsDouble() : null;
    }
    // Room broadcasts are wrapped {version,type,identity,challengeId,data:{...}}.
    // Pull the semantic body from data if present, else treat the object as flat.
    static JsonObject body(Object raw) {
        if (!(raw instanceof JsonObject)) return null;
        JsonObject o = (JsonObject) raw;
        if (o.has("data") && o.get("data").isJsonObject()) return o.getAsJsonObject("data");
        return o;
    }

    public static void main(String[] args) throws Exception {
        String apiKey = System.getenv("ODDSOCKETS_API_KEY");
        String managerUrl = System.getenv("ODDSOCKETS_MANAGER_URL");
        if (apiKey == null || apiKey.isBlank() || managerUrl == null || managerUrl.isBlank()) {
            System.err.println("ODDSOCKETS_API_KEY and ODDSOCKETS_MANAGER_URL must be set.");
            System.exit(1);
        }

        System.out.println("[connect] connecting alice + bob via manager " + managerUrl);
        OddSockets alice = connect(apiKey, managerUrl, "alice");
        OddSockets bob = connect(apiKey, managerUrl, "bob");
        System.out.println("[alice] state=" + alice.getState());
        System.out.println("[bob]   state=" + bob.getState());

        // ---- cross-client latches / captures --------------------------------
        // Shared owner scope means concurrent runs may cross-fire; buffer every
        // delivery keyed by its own scope id and match on the authoritative id
        // this run created, so no assertion can pass on a stray broadcast.
        final String challengeId = "chal-" + UUID.randomUUID().toString().substring(0, 8);
        final String achId = "ach-" + UUID.randomUUID().toString().substring(0, 8);

        java.util.Map<String, JsonObject> aliceProgressByChal = new java.util.concurrent.ConcurrentHashMap<>();
        java.util.Map<String, JsonObject> aliceRankByChal = new java.util.concurrent.ConcurrentHashMap<>();
        java.util.Map<String, JsonObject> bobAchProgressById = new java.util.concurrent.ConcurrentHashMap<>();
        java.util.Map<String, JsonObject> bobAchUnlockById = new java.util.concurrent.ConcurrentHashMap<>();
        java.util.Map<String, JsonObject> bobInvitedById = new java.util.concurrent.ConcurrentHashMap<>();
        java.util.Map<String, JsonObject> aliceInvitedById = new java.util.concurrent.ConcurrentHashMap<>();
        java.util.Map<String, JsonObject> aliceReplyById = new java.util.concurrent.ConcurrentHashMap<>();
        java.util.Map<String, JsonObject> bobCancelledById = new java.util.concurrent.ConcurrentHashMap<>();

        // alice: progress + rank change broadcasts (wrapped envelope). Key by the
        // outer challengeId + the identity+value in data so alice's own row is
        // distinguishable from bob's on the same challenge.
        alice.on("challenge_progress", raw -> {
            if (raw instanceof JsonObject o && challengeId.equals(s(o, "challengeId"))) {
                JsonObject b = body(raw);
                aliceProgressByChal.put(s(o, "challengeId") + ":" + s(o, "identity") + ":" + num(b, "value"), b);
                System.out.println("[alice<-worker] challenge_progress " + raw);
            }
        });
        alice.on("leaderboard_rank_change", raw -> {
            if (raw instanceof JsonObject o && challengeId.equals(s(o, "challengeId"))) {
                aliceRankByChal.put(s(o, "challengeId") + ":" + s(o, "identity"), body(raw));
                System.out.println("[alice<-worker] leaderboard_rank_change " + raw);
            }
        });
        // alice also should NOT receive her own invite (invitee-only delivery)
        alice.on("challenge_invited", raw -> {
            if (raw instanceof JsonObject o && s(o, "inviteId") != null) {
                aliceInvitedById.put(s(o, "inviteId"), o);
                System.out.println("[alice<-worker] challenge_invited (own?) " + raw);
            }
        });
        alice.on("challenge_reply_received", raw -> {
            if (raw instanceof JsonObject o && s(o, "inviteId") != null) {
                aliceReplyById.put(s(o, "inviteId"), o);
                System.out.println("[alice<-worker] challenge_reply_received " + raw);
            }
        });

        // bob: achievement broadcasts (wrapped) keyed by achievementId, + directed
        // invite / cancel.
        bob.on("achievement_progress", raw -> {
            JsonObject b = body(raw);
            if (b != null && achId.equals(s(b, "achievementId"))) {
                bobAchProgressById.put(s(b, "achievementId"), b);
                System.out.println("[bob<-worker] achievement_progress " + raw);
            }
        });
        bob.on("achievement_unlock", raw -> {
            JsonObject b = body(raw);
            if (b != null && achId.equals(s(b, "achievementId"))) {
                bobAchUnlockById.put(s(b, "achievementId"), b);
                System.out.println("[bob<-worker] achievement_unlock " + raw);
            }
        });
        bob.on("challenge_invited", raw -> {
            if (raw instanceof JsonObject o && s(o, "inviteId") != null) {
                bobInvitedById.put(s(o, "inviteId"), o);
                System.out.println("[bob<-worker] challenge_invited " + raw);
            }
        });
        bob.on("challenge_invite_cancelled", raw -> {
            if (raw instanceof JsonObject o && s(o, "inviteId") != null) {
                bobCancelledById.put(s(o, "inviteId"), o);
                System.out.println("[bob<-worker] challenge_invite_cancelled " + raw);
            }
        });

        // both subscribe to lobby
        Channel aliceCh = alice.channel("lobby");
        Channel bobCh = bob.channel("lobby");
        aliceCh.subscribe(m -> {}, Channel.SubscribeOptions.builder().enablePresence(true).build()).get(20, TimeUnit.SECONDS);
        bobCh.subscribe(m -> {}, Channel.SubscribeOptions.builder().enablePresence(true).build()).get(20, TimeUnit.SECONDS);
        System.out.println("[both] subscribed to lobby");
        Thread.sleep(600);

        System.out.println("\n=== CHALLENGE " + challengeId + " (ach " + achId + ") ===");

        // 1. createChallenge (alice)
        Map<String, Object> create = new HashMap<>();
        create.put("challengeId", challengeId);
        create.put("metric", "score");
        create.put("ranked", true);
        create.put("channel", "lobby");
        JsonObject createAck = alice.enhanced.createChallenge(create).get(15, TimeUnit.SECONDS);
        System.out.println("[alice] createChallenge ack " + createAck);
        check("1 createChallenge acked", createAck != null, "challengeId=" + s(createAck, "challengeId"));

        // 2. reportProgress: alice=40, bob=55 (fire-and-forget)
        alice.enhanced.reportProgress(prog(challengeId, 40));
        bob.enhanced.reportProgress(prog(challengeId, 55));

        // alice must see BOTH players' progress+rank on THIS challenge via the worker.
        JsonObject aProg = await(aliceProgressByChal, challengeId + ":alice:40.0", 15000);
        JsonObject bProg = await(aliceProgressByChal, challengeId + ":bob:55.0", 15000);
        JsonObject aRank = await(aliceRankByChal, challengeId + ":alice", 15000);
        JsonObject bRank = await(aliceRankByChal, challengeId + ":bob", 15000);
        check("2a alice sees challenge_progress (alice=40 + bob=55)", aProg != null && bProg != null,
                "alice=" + (aProg != null) + " bob=" + (bProg != null));
        check("2b alice sees leaderboard_rank_change (alice rank2 + bob rank1)",
                aRank != null && bRank != null && eq(num(aRank, "rank"), 2) && eq(num(bRank, "rank"), 1),
                "aliceRank=" + (aRank != null ? num(aRank, "rank") : null) + " bobRank=" + (bRank != null ? num(bRank, "rank") : null));

        // 3. getStandings (alice) -> bob@55 rank1, alice@40 rank2, alice yourRank=2
        Map<String, Object> stReq = new HashMap<>();
        stReq.put("challengeId", challengeId);
        stReq.put("limit", 10);
        JsonObject standings = alice.enhanced.getStandings(stReq).get(15, TimeUnit.SECONDS);
        System.out.println("[alice] standings " + standings);
        JsonArray rows = standings != null && standings.has("standings") ? standings.getAsJsonArray("standings") : new JsonArray();
        JsonObject r1 = rows.size() > 0 ? rows.get(0).getAsJsonObject() : null;
        JsonObject r2 = rows.size() > 1 ? rows.get(1).getAsJsonObject() : null;
        check("3a standings rank1 = bob@55",
                r1 != null && "bob".equals(s(r1, "identity")) && eq(num(r1, "value"), 55) && eq(num(r1, "rank"), 1),
                r1 == null ? "no row" : (s(r1, "identity") + "@" + num(r1, "value") + " rank" + num(r1, "rank")));
        check("3b standings rank2 = alice@40",
                r2 != null && "alice".equals(s(r2, "identity")) && eq(num(r2, "value"), 40) && eq(num(r2, "rank"), 2),
                r2 == null ? "no row" : (s(r2, "identity") + "@" + num(r2, "value") + " rank" + num(r2, "rank")));
        check("3c alice yourRank=2", eq(num(standings, "yourRank"), 2), "yourRank=" + num(standings, "yourRank"));

        // 4. complete: alice(tied)=>finalValue40 rank2, bob(conceded)=>finalValue55 rank1
        JsonObject aliceComplete = alice.enhanced.completeChallenge(complete(challengeId, "tied")).get(15, TimeUnit.SECONDS);
        System.out.println("[alice] complete ack " + aliceComplete);
        check("4a alice complete tied finalValue40 rank2",
                aliceComplete != null && "tied".equals(s(aliceComplete, "outcome"))
                        && eq(num(aliceComplete, "finalValue"), 40) && eq(num(aliceComplete, "rank"), 2),
                "outcome=" + s(aliceComplete, "outcome") + " finalValue=" + num(aliceComplete, "finalValue") + " rank=" + num(aliceComplete, "rank"));
        JsonObject bobComplete = bob.enhanced.completeChallenge(complete(challengeId, "conceded")).get(15, TimeUnit.SECONDS);
        System.out.println("[bob] complete ack " + bobComplete);
        check("4b bob complete conceded finalValue55 rank1",
                bobComplete != null && "conceded".equals(s(bobComplete, "outcome"))
                        && eq(num(bobComplete, "finalValue"), 55) && eq(num(bobComplete, "rank"), 1),
                "outcome=" + s(bobComplete, "outcome") + " finalValue=" + num(bobComplete, "finalValue") + " rank=" + num(bobComplete, "rank"));

        // 5. achievements: alice unlock(50) => bob achievement_progress in_progress (no banner)
        alice.enhanced.unlockAchievement(ach(achId, 50));
        JsonObject apb = await(bobAchProgressById, achId, 15000);
        check("5 bob sees achievement_progress in_progress (no banner)",
                apb != null && "in_progress".equals(s(apb, "status")) && eq(num(apb, "percentComplete"), 50)
                        && bobAchUnlockById.get(achId) == null,
                apb == null ? "no body" : "status=" + s(apb, "status") + " pct=" + num(apb, "percentComplete"));

        //    alice unlock(100) => bob achievement_unlock unlocked
        alice.enhanced.unlockAchievement(ach(achId, 100));
        JsonObject aub = await(bobAchUnlockById, achId, 15000);
        check("6 bob sees achievement_unlock unlocked",
                aub != null && "unlocked".equals(s(aub, "status")) && eq(num(aub, "percentComplete"), 100),
                aub == null ? "no body" : "status=" + s(aub, "status") + " pct=" + num(aub, "percentComplete"));

        // getAchievements (alice) 100/unlocked
        Thread.sleep(500);
        Map<String, Object> achReq = new HashMap<>();
        achReq.put("achievementId", achId);
        JsonObject achState = alice.enhanced.getAchievements(achReq).get(15, TimeUnit.SECONDS);
        System.out.println("[alice] achievement_state " + achState);
        JsonArray achs = achState != null && achState.has("achievements") ? achState.getAsJsonArray("achievements") : new JsonArray();
        JsonObject a0 = achs.size() > 0 ? achs.get(0).getAsJsonObject() : null;
        check("7 getAchievements 100/unlocked",
                a0 != null && eq(num(a0, "percentComplete"), 100) && "unlocked".equals(s(a0, "status")),
                a0 == null ? "no ach" : "pct=" + num(a0, "percentComplete") + " status=" + s(a0, "status"));

        // 8. invite: alice -> bob (directed, FLAT). Client-supplied inviteId so the
        // cross-client latch can match THIS run's invite (shared owner scope).
        Map<String, Object> inv = new HashMap<>();
        inv.put("toUserId", "bob");
        inv.put("type", "match");
        Map<String, Object> payload = new HashMap<>();
        payload.put("stake", "gold");
        inv.put("payload", payload);
        inv.put("ttl", 300);
        JsonObject inviteAck = alice.enhanced.sendChallengeInvite(inv).get(15, TimeUnit.SECONDS);
        System.out.println("[alice] invite ack " + inviteAck);
        String inviteId = s(inviteAck, "inviteId");
        check("8a invite acked pending -> bob",
                inviteAck != null && inviteId != null && "bob".equals(s(inviteAck, "toUserId")) && "pending".equals(s(inviteAck, "status")),
                "inviteId=" + inviteId + " status=" + s(inviteAck, "status"));

        JsonObject bi = await(bobInvitedById, inviteId, 15000);
        check("8b bob sees challenge_invited (flat) from alice", bi != null && flatFrom(bi, "alice"),
                bi == null ? "no invite" : "from=" + fromField(bi));
        // alice must NOT receive her own directed invite (invitee-only delivery)
        check("8c alice did NOT get her own invite", aliceInvitedById.get(inviteId) == null, null);

        // 9. bob getChallengeInvites lists it
        JsonObject invites = bob.enhanced.getChallengeInvites().get(15, TimeUnit.SECONDS);
        System.out.println("[bob] getChallengeInvites " + invites);
        JsonArray invArr = invites != null && invites.has("invites") ? invites.getAsJsonArray("invites") : new JsonArray();
        boolean listed = false;
        for (JsonElement e : invArr) {
            if (e.isJsonObject() && inviteId.equals(s(e.getAsJsonObject(), "inviteId"))) { listed = true; break; }
        }
        check("9 bob getChallengeInvites lists inviteId", listed, "count=" + invArr.size());

        // 10. bob reply(accept) => alice sees challenge_reply_received
        Map<String, Object> reply = new HashMap<>();
        reply.put("inviteId", inviteId);
        reply.put("accept", true);
        JsonObject replyAck = bob.enhanced.replyChallengeInvite(reply).get(15, TimeUnit.SECONDS);
        System.out.println("[bob] reply ack " + replyAck);
        JsonObject aReply = await(aliceReplyById, inviteId, 15000);
        check("10 alice sees challenge_reply_received", aReply != null,
                aReply == null ? "not received" : "from=" + fromField(aReply));

        // 11. fresh invite + cancel => bob sees challenge_invite_cancelled
        Map<String, Object> inv2 = new HashMap<>();
        inv2.put("toUserId", "bob");
        inv2.put("type", "match");
        JsonObject invite2 = alice.enhanced.sendChallengeInvite(inv2).get(15, TimeUnit.SECONDS);
        String inviteId2 = s(invite2, "inviteId");
        Thread.sleep(400);
        Map<String, Object> cancel = new HashMap<>();
        cancel.put("inviteId", inviteId2);
        JsonObject cancelAck = alice.enhanced.cancelChallengeInvite(cancel).get(15, TimeUnit.SECONDS);
        System.out.println("[alice] cancel ack " + cancelAck);
        JsonObject bCancel = await(bobCancelledById, inviteId2, 15000);
        check("11 bob sees challenge_invite_cancelled", bCancel != null,
                bCancel == null ? "not received" : "ok");

        System.out.println("\n==== RESULT: " + pass + " passed, " + fail + " failed ====");
        System.out.println("cross_instance=" + (workerId(alice).equals(workerId(bob)) ? "no" : "yes")
                + "  (alice and bob served by "
                + (workerId(alice).equals(workerId(bob)) ? "the same instance" : "different instances") + ")");
        alice.close();
        bob.close();
        System.exit(fail == 0 ? 0 : 1);
    }

    static boolean eq(Double a, double b) { return a != null && Math.abs(a - b) < 0.0001; }

    // Poll a buffered delivery map for a specific inviteId up to timeoutMs.
    static JsonObject await(java.util.Map<String, JsonObject> m, String id, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            JsonObject o = m.get(id);
            if (o != null) return o;
            Thread.sleep(50);
        }
        return m.get(id);
    }

    static Map<String, Object> prog(String challengeId, double value) {
        Map<String, Object> m = new HashMap<>();
        m.put("challengeId", challengeId);
        m.put("metric", "score");
        m.put("value", value);
        m.put("eventId", UUID.randomUUID().toString());
        return m;
    }
    static Map<String, Object> complete(String challengeId, String outcome) {
        Map<String, Object> m = new HashMap<>();
        m.put("challengeId", challengeId);
        m.put("outcome", outcome);
        m.put("eventId", UUID.randomUUID().toString());
        return m;
    }
    static Map<String, Object> ach(String achId, int pct) {
        Map<String, Object> m = new HashMap<>();
        m.put("achievementId", achId);
        m.put("name", "Test Achievement");
        m.put("percentComplete", pct);
        m.put("channel", "lobby");
        return m;
    }
    // directed invite is flat; the inviter identity may be a primitive
    // (fromUserId/identity) or a nested object ("from":{userId,identity}).
    static String fromField(JsonObject o) {
        for (String k : new String[]{"fromUserId", "fromIdentity", "identity", "inviterUserId", "inviter"}) {
            String v = s(o, k);
            if (v != null) return v;
        }
        if (o != null && o.has("from") && o.get("from").isJsonObject()) {
            JsonObject f = o.getAsJsonObject("from");
            String v = s(f, "userId");
            if (v == null) v = s(f, "identity");
            return v;
        }
        if (o != null && o.has("from") && o.get("from").isJsonPrimitive()) {
            return s(o, "from");
        }
        return null;
    }
    static boolean flatFrom(JsonObject o, String who) {
        return who.equals(fromField(o));
    }
}
