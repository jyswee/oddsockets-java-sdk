package com.oddsockets;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Real Socket.IO (Engine.IO v4) client for OddSockets over a raw WebSocket.
 *
 * This is a hand-wired transport that speaks the OddSockets worker wire
 * contract directly - no local echo, no simulation:
 *
 *   dial   wss://&lt;worker&gt;/socket.io/?EIO=4&amp;transport=websocket
 *   OPEN   "0" (Engine.IO)        -&gt; send Socket.IO CONNECT "40{apiKey,userId}"
 *   PING   "2"                    -&gt; PONG "3"
 *   CONNECT ack "40..."           -&gt; connection established
 *   EVENT  "42[event,payload]"    -&gt; dispatched to handlers by event name
 *
 * Event payloads are delivered to handlers as Gson {@link JsonElement}s.
 */
public class WebSocketConnection {

    private static final Logger logger = LoggerFactory.getLogger(WebSocketConnection.class);

    private final String wsUrl;
    private final String apiKey;
    private final String userId;
    private final Gson gson = new Gson();

    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final CountDownLatch connectLatch = new CountDownLatch(1);
    private final Map<String, List<Handler>> handlers = new ConcurrentHashMap<>();

    private volatile WebSocketClient ws;
    private volatile Consumer<String> disconnectHandler;
    private volatile Consumer<Exception> errorHandler;

    private static final class Handler {
        final Consumer<JsonElement> fn;
        final boolean once;

        Handler(Consumer<JsonElement> fn, boolean once) {
            this.fn = fn;
            this.once = once;
        }
    }

    /**
     * Create a new Socket.IO connection wrapper.
     *
     * @param workerUrl the assigned worker base URL (http/https)
     * @param apiKey    the OddSockets API key (lands in handshake.auth)
     * @param userId    the connecting user id
     */
    public WebSocketConnection(String workerUrl, String apiKey, String userId) {
        this.wsUrl = toSocketIoUrl(workerUrl);
        this.apiKey = apiKey;
        this.userId = userId;
        logger.debug("Created Socket.IO connection for URL: {}", wsUrl);
    }

    static String toSocketIoUrl(String workerUrl) {
        String u = workerUrl;
        if (u.startsWith("https://")) {
            u = "wss://" + u.substring("https://".length());
        } else if (u.startsWith("http://")) {
            u = "ws://" + u.substring("http://".length());
        }
        if (u.endsWith("/")) {
            u = u.substring(0, u.length() - 1);
        }
        return u + "/socket.io/?EIO=4&transport=websocket";
    }

    /**
     * Connect to the worker and wait for the Socket.IO CONNECT ack.
     *
     * @param timeoutMs total time to wait for a live connection
     * @throws Exception if the WebSocket handshake or Socket.IO connect times out
     */
    public void connect(long timeoutMs) throws Exception {
        final WebSocketClient client = new WebSocketClient(URI.create(wsUrl)) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                logger.debug("WebSocket handshake complete: {}", wsUrl);
            }

            @Override
            public void onMessage(String message) {
                try {
                    handleRaw(message);
                } catch (Exception e) {
                    logger.error("Error handling frame: {}", e.getMessage());
                }
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                connected.set(false);
                connectLatch.countDown();
                Consumer<String> h = disconnectHandler;
                if (h != null) {
                    h.accept(remote ? "server_disconnect" : "client_disconnect");
                }
            }

            @Override
            public void onError(Exception ex) {
                Consumer<Exception> h = errorHandler;
                if (h != null) {
                    h.accept(ex);
                }
            }
        };

        this.ws = client;

        if (!client.connectBlocking(timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new java.io.IOException("WebSocket connection timed out: " + wsUrl);
        }

        if (!connectLatch.await(timeoutMs, TimeUnit.MILLISECONDS) || !connected.get()) {
            throw new java.io.IOException("Socket.IO connect timed out: " + wsUrl);
        }
    }

    private void handleRaw(String data) {
        if (data == null || data.isEmpty()) {
            return;
        }
        char type = data.charAt(0);
        switch (type) {
            case '0' -> { // Engine.IO OPEN -> send Socket.IO CONNECT with auth
                JsonObject auth = new JsonObject();
                auth.addProperty("apiKey", apiKey);
                auth.addProperty("userId", userId);
                writeRaw("40" + gson.toJson(auth));
            }
            case '1' -> close(); // Engine.IO CLOSE
            case '2' -> writeRaw("3"); // Engine.IO PING -> PONG
            case '4' -> handleSocketIo(data.substring(1)); // Socket.IO message
            default -> { /* ignore other Engine.IO frames */ }
        }
    }

    private void handleSocketIo(String data) {
        if (data.isEmpty()) {
            return;
        }
        char type = data.charAt(0);
        switch (type) {
            case '0' -> { // CONNECT ack
                connected.set(true);
                connectLatch.countDown();
            }
            case '2' -> handleEvent(data.substring(1)); // EVENT
            case '4' -> dispatch("error", parseJson(data.substring(1))); // ERROR
            default -> { /* ignore ACK/BINARY frames */ }
        }
    }

    private void handleEvent(String data) {
        int idx = data.indexOf('[');
        if (idx < 0) {
            return;
        }
        JsonElement parsed = parseJson(data.substring(idx));
        if (parsed == null || !parsed.isJsonArray()) {
            return;
        }
        JsonArray arr = parsed.getAsJsonArray();
        if (arr.isEmpty()) {
            return;
        }
        String event = arr.get(0).getAsString();
        JsonElement payload = arr.size() > 1 ? arr.get(1) : JsonNull.INSTANCE;
        dispatch(event, payload);
    }

    private void dispatch(String event, JsonElement payload) {
        List<Handler> list = handlers.get(event);
        if (list == null) {
            return;
        }
        for (Handler h : new ArrayList<>(list)) {
            if (h.once) {
                list.remove(h);
            }
            safeCall(h.fn, payload);
        }
    }

    /**
     * Register a persistent handler for an event.
     *
     * @param event the Socket.IO event name
     * @param fn    the handler receiving the event payload
     */
    public void on(String event, Consumer<JsonElement> fn) {
        add(event, fn, false);
    }

    /**
     * Register a one-shot handler for an event.
     *
     * @param event the Socket.IO event name
     * @param fn    the handler receiving the event payload
     */
    public void once(String event, Consumer<JsonElement> fn) {
        add(event, fn, true);
    }

    private void add(String event, Consumer<JsonElement> fn, boolean once) {
        handlers.computeIfAbsent(event, k -> new java.util.concurrent.CopyOnWriteArrayList<>())
                .add(new Handler(fn, once));
    }

    /**
     * Remove a previously registered handler by identity.
     *
     * @param event the Socket.IO event name
     * @param fn    the handler reference to remove
     */
    public void off(String event, Consumer<JsonElement> fn) {
        List<Handler> list = handlers.get(event);
        if (list != null) {
            list.removeIf(h -> h.fn == fn);
        }
    }

    /**
     * Emit an event to the worker.
     *
     * @param event the Socket.IO event name
     * @param data  the payload (serialized to JSON; null-valued keys pruned)
     */
    public void emit(String event, JsonElement data) {
        if (!connected.get()) {
            throw new IllegalStateException("Socket is not connected");
        }
        JsonArray frame = new JsonArray();
        frame.add(event);
        if (data != null && !data.isJsonNull()) {
            frame.add(pruneNulls(data));
        }
        writeRaw("42" + gson.toJson(frame));
    }

    /**
     * Emit an event and wait for a correlated response event.
     *
     * @param emitEvent the request event name
     * @param data      the request payload
     * @param respEvent the response event name to await
     * @param match     optional predicate to correlate the response (e.g. by channel)
     * @param timeoutMs how long to wait before failing
     * @return the matched response object
     * @throws Exception on timeout or a worker error
     */
    public JsonObject request(String emitEvent, JsonObject data, String respEvent,
                              Predicate<JsonObject> match, long timeoutMs) throws Exception {
        final CompletableFuture<JsonObject> future = new CompletableFuture<>();

        final Consumer<JsonElement> onResp = el -> {
            JsonObject o = el != null && el.isJsonObject() ? el.getAsJsonObject() : new JsonObject();
            if (match == null || match.test(o)) {
                future.complete(o);
            }
        };
        final Consumer<JsonElement> onErr = el -> {
            JsonObject o = el != null && el.isJsonObject() ? el.getAsJsonObject() : new JsonObject();
            future.completeExceptionally(new RuntimeException(extractErrorMessage(o)));
        };

        on(respEvent, onResp);
        once("error", onErr);
        try {
            emit(emitEvent, data);
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } finally {
            off(respEvent, onResp);
            off("error", onErr);
        }
    }

    /**
     * Check whether the socket is connected.
     *
     * @return true if the Socket.IO connection is live
     */
    public boolean isConnected() {
        return connected.get() && ws != null && ws.isOpen();
    }

    /**
     * Set the disconnect handler.
     *
     * @param handler receives a disconnect reason
     */
    public void onDisconnect(Consumer<String> handler) {
        this.disconnectHandler = handler;
    }

    /**
     * Set the error handler.
     *
     * @param handler receives transport exceptions
     */
    public void onError(Consumer<Exception> handler) {
        this.errorHandler = handler;
    }

    /**
     * Close the connection, sending a Socket.IO DISCONNECT first.
     */
    public void close() {
        WebSocketClient client = this.ws;
        if (client != null && client.isOpen()) {
            try {
                client.send("41"); // Socket.IO DISCONNECT
            } catch (Exception ignored) {
                // best effort
            }
            client.close();
        }
        connected.set(false);
    }

    private void writeRaw(String frame) {
        WebSocketClient client = this.ws;
        if (client != null && client.isOpen()) {
            client.send(frame);
        }
    }

    private void safeCall(Consumer<JsonElement> fn, JsonElement payload) {
        try {
            fn.accept(payload);
        } catch (Exception e) {
            logger.error("Error in event handler: {}", e.getMessage());
        }
    }

    private JsonElement parseJson(String s) {
        try {
            return JsonParser.parseString(s);
        } catch (Exception e) {
            return JsonNull.INSTANCE;
        }
    }

    private static String extractErrorMessage(JsonObject o) {
        if (o.has("message") && o.get("message").isJsonPrimitive()) {
            return o.get("message").getAsString();
        }
        if (o.has("error") && o.get("error").isJsonPrimitive()) {
            return o.get("error").getAsString();
        }
        return "worker error";
    }

    private static JsonElement pruneNulls(JsonElement el) {
        if (el.isJsonObject()) {
            JsonObject src = el.getAsJsonObject();
            JsonObject out = new JsonObject();
            for (Map.Entry<String, JsonElement> e : src.entrySet()) {
                if (e.getValue() == null || e.getValue().isJsonNull()) {
                    continue;
                }
                out.add(e.getKey(), pruneNulls(e.getValue()));
            }
            return out;
        }
        if (el.isJsonArray()) {
            JsonArray src = el.getAsJsonArray();
            JsonArray out = new JsonArray();
            for (JsonElement e : src) {
                if (e == null || e.isJsonNull()) {
                    continue;
                }
                out.add(pruneNulls(e));
            }
            return out;
        }
        return el;
    }

    @Override
    public String toString() {
        return "WebSocketConnection{url='" + wsUrl + "', connected=" + connected.get() + '}';
    }
}
