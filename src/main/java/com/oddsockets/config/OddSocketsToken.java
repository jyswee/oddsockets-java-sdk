package com.oddsockets.config;

/**
 * A minted realtime token returned by a {@link OddSocketsConfig#getTokenProvider()}.
 *
 * <p>Only {@link #getToken()} is required; the expiry fields let the SDK schedule an
 * ahead-of-expiry refresh without decoding the JWT itself. (FEAT-2026-0824-0040)</p>
 *
 * @author Joe Wee
 * @since 0.1.0
 */
public class OddSocketsToken {

    private String token;
    private String expiresAt;
    private Long exp;
    private String baseUrl;
    private String identity;

    /** Creates an empty token (for deserialization). */
    public OddSocketsToken() {
    }

    /**
     * Creates a token with just the minted value.
     *
     * @param token the minted realtime token (JWT)
     */
    public OddSocketsToken(String token) {
        this.token = token;
    }

    /**
     * Gets the minted realtime token (JWT) presented instead of an API key.
     *
     * @return the token
     */
    public String getToken() {
        return token;
    }

    /**
     * Sets the minted realtime token.
     *
     * @param token the token
     */
    public void setToken(String token) {
        this.token = token;
    }

    /**
     * Gets the optional ISO-8601 expiry.
     *
     * @return the expiry, or null
     */
    public String getExpiresAt() {
        return expiresAt;
    }

    /**
     * Sets the optional ISO-8601 expiry.
     *
     * @param expiresAt the expiry
     */
    public void setExpiresAt(String expiresAt) {
        this.expiresAt = expiresAt;
    }

    /**
     * Gets the optional epoch-seconds expiry claim.
     *
     * @return the exp claim, or null
     */
    public Long getExp() {
        return exp;
    }

    /**
     * Sets the optional epoch-seconds expiry claim.
     *
     * @param exp the exp claim
     */
    public void setExp(Long exp) {
        this.exp = exp;
    }

    /**
     * Gets the optional manager base URL the token is scoped to.
     *
     * @return the base URL, or null
     */
    public String getBaseUrl() {
        return baseUrl;
    }

    /**
     * Sets the optional manager base URL.
     *
     * @param baseUrl the base URL
     */
    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    /**
     * Gets the optional resolved caller identity.
     *
     * @return the identity, or null
     */
    public String getIdentity() {
        return identity;
    }

    /**
     * Sets the optional resolved caller identity.
     *
     * @param identity the identity
     */
    public void setIdentity(String identity) {
        this.identity = identity;
    }
}
