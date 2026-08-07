package com.oddsockets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

/**
 * Manager Discovery Service
 *
 * Resolves the manager endpoint that a client should talk to. The manager
 * handles all worker routing and load balancing transparently.
 *
 * Resolution order is: explicit configuration, then the
 * {@code ODDSOCKETS_MANAGER_URL} environment variable, then the public
 * default endpoint. The default is only ever used when nothing was
 * configured at all - a configured manager that is unreachable must surface
 * that failure, because silently redirecting a self-hosted or staging client
 * to production makes a misconfigured deployment look healthy.
 */
public class ManagerDiscovery {

    private static final Logger logger = LoggerFactory.getLogger(ManagerDiscovery.class);

    /**
     * The public manager endpoint, used only when no manager URL was configured.
     */
    public static final String DEFAULT_MANAGER_URL = "https://connect.oddsockets.tyga.network";

    /**
     * Environment variable consulted when no manager URL was configured explicitly.
     */
    public static final String MANAGER_URL_ENV_VAR = "ODDSOCKETS_MANAGER_URL";

    // Singleton instance. It holds no per-client state: the manager URL is always
    // supplied by the caller so that one client's configuration cannot leak into another's.
    private static final ManagerDiscovery INSTANCE = new ManagerDiscovery();

    private ManagerDiscovery() {}

    /**
     * Get the singleton instance.
     *
     * @return the manager discovery instance
     */
    public static ManagerDiscovery getInstance() {
        return INSTANCE;
    }

    /**
     * Resolve the manager URL for a client.
     *
     * @param apiKey The OddSockets API key
     * @param configuredManagerUrl The manager URL from the client configuration, may be null
     * @return CompletableFuture with the resolved manager URL
     * @throws IllegalArgumentException if the resolved URL is not an absolute http(s) URL
     */
    public CompletableFuture<String> discoverManagerUrl(String apiKey, String configuredManagerUrl) {
        String managerUrl = resolveManagerUrl(configuredManagerUrl);

        logger.debug("Resolved manager URL {} for API key: {}", managerUrl,
            apiKey != null ? apiKey.substring(0, Math.min(8, apiKey.length())) + "..." : "null");

        return CompletableFuture.completedFuture(managerUrl);
    }

    /**
     * Resolve and validate a manager URL.
     *
     * @param configuredManagerUrl The manager URL from the client configuration, may be null
     * @return the resolved manager URL, without any trailing slash
     * @throws IllegalArgumentException if the resolved URL is not an absolute http(s) URL
     */
    public static String resolveManagerUrl(String configuredManagerUrl) {
        String candidate = configuredManagerUrl;

        if (isBlank(candidate)) {
            candidate = System.getenv(MANAGER_URL_ENV_VAR);
        }

        if (isBlank(candidate)) {
            candidate = DEFAULT_MANAGER_URL;
        }

        return validateManagerUrl(candidate);
    }

    /**
     * Clear cache (no-op, kept for compatibility)
     */
    public void clearCache() {
        // No cache to clear in simplified version
        logger.debug("Cache cleared (no-op in simplified version)");
    }

    private static String validateManagerUrl(String managerUrl) {
        String candidate = managerUrl.trim();

        URI uri;
        try {
            uri = new URI(candidate);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid managerUrl: " + managerUrl);
        }

        if (!uri.isAbsolute() || uri.getHost() == null) {
            throw new IllegalArgumentException("Invalid managerUrl: " + managerUrl);
        }

        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        if (!"http".equals(scheme) && !"https".equals(scheme)) {
            throw new IllegalArgumentException("Invalid managerUrl: " + managerUrl);
        }

        while (candidate.endsWith("/")) {
            candidate = candidate.substring(0, candidate.length() - 1);
        }

        return candidate;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
