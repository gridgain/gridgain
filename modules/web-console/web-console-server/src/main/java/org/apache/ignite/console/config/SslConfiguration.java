/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.config;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * SSL configuration.
 */
public class SslConfiguration {
    /** SSL enabled flag. */
    private boolean enabled;

    /** Path to key store. */
    private String keyStore;

    /** Optional password for key store. */
    private String keyStorePwd;

    /** Path to trust store. */
    private String trustStore;

    /** Optional password for trust store. */
    private String trustStorePwd;

    /** Optional comma-separated list of SSL cipher suites. */
    private String cipherSuites;

    /** Authentication required flag. */
    private boolean clientAuth;

    /**
     * Empty constructor.
     */
    public SslConfiguration() {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param cc Configuration to copy.
     */
    public SslConfiguration(SslConfiguration cc) {
        enabled = cc.isEnabled();
        keyStore = cc.getKeyStore();
        keyStorePwd = cc.getKeyStorePassword();
        trustStore = cc.getTrustStore();
        trustStorePwd = cc.getTrustStorePassword();
        cipherSuites = cc.getCipherSuites();
        clientAuth = cc.isClientAuth();
    }

    /**
     * @return {@code true} if SSL enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @param enabled SSL enabled flag.
     * @return {@code this} for chaining.
     */
    public SslConfiguration setEnabled(boolean enabled) {
        this.enabled = enabled;

        return this;
    }

    /**
     * @return Path to key store.
     */
    public String getKeyStore() {
        return keyStore;
    }

    /**
     * @param keyStore Path to key store.
     * @return {@code this} for chaining.
     */
    public SslConfiguration setKeyStore(String keyStore) {
        this.keyStore = keyStore;

        return this;
    }

    /**
     * @return Key store password.
     */
    public String getKeyStorePassword() {
        return keyStorePwd;
    }

    /**
     * @param keyStorePwd Key store password.
     * @return {@code this} for chaining.
     */
    public SslConfiguration setKeyStorePassword(String keyStorePwd) {
        this.keyStorePwd = keyStorePwd;

        return this;
    }

    /**
     * @return Path to trust store.
     */
    public String getTrustStore() {
        return trustStore;
    }

    /**
     * @param trustStore Path to trust store.
     * @return {@code this} for chaining.
     */
    public SslConfiguration setTrustStore(String trustStore) {
        this.trustStore = trustStore;

        return this;
    }

    /**
     * @return Trust store password.
     */
    public String getTrustStorePassword() {
        return trustStorePwd;
    }

    /**
     * @param trustStorePwd Trust store password.
     * @return {@code this} for chaining.
     */
    public SslConfiguration setTrustStorePassword(String trustStorePwd) {
        this.trustStorePwd = trustStorePwd;

        return this;
    }

    /**
     * @return SSL cipher suites.
     */
    public String getCipherSuites() {
        return cipherSuites;
    }

    /**
     * @param cipherSuites SSL cipher suites.
     * @return {@code this} for chaining.
     */
    public SslConfiguration setCipherSuites(String cipherSuites) {
        this.cipherSuites = cipherSuites;

        return this;
    }

    /**
     * @return {@code true} if authentication required.
     */
    public boolean isClientAuth() {
        return clientAuth;
    }

    /**
     * @param clientAuth Authentication required flag.
     * @return {@code this} for chaining.
     */
    public SslConfiguration setClientAuth(boolean clientAuth) {
        this.clientAuth = clientAuth;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SslConfiguration.class, this);
    }
}
