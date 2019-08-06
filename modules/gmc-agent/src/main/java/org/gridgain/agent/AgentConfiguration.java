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

package org.gridgain.agent;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.ignite.internal.util.typedef.F;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.gridgain.agent.AgentUtils.secured;

/**
 * Agent configuration.
 *
 * TODO GG-20641 This class should be replaced with manipulation with metastore.
 */
public class AgentConfiguration implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 8955393951714120540L;

    /** Default server URI. */
    private static final String DFLT_SERVER_URI = "http://localhost:3000";

    /** */
    private String srvUri = DFLT_SERVER_URI;

    /** */
    private String lastSuccessConnectedSrvUri;

    /** */
    private String srvKeyStore;

    /** */
    private String srvKeyStorePass;

    /** */
    private String srvTrustStore;

    /** */
    private String srvTrustStorePass;

    /** */
    private List<String> cipherSuites;

    /**
     * @return Server URI.
     */
    public String serverUri() {
        return srvUri;
    }

    /**
     * @param srvUri URI.
     * @return {@code this} for chaining.
     */
    public AgentConfiguration serverUri(String srvUri) {
        this.srvUri = srvUri;

        return this;
    }

    /**
     * @return Server URI.
     */
    public String lastSuccessConnectedServerUri() {
        return lastSuccessConnectedSrvUri;
    }

    /**
     * @param lastSuccessConnectedSrvUri URI.
     * @return {@code this} for chaining.
     */
    public AgentConfiguration lastSuccessConnectedServerUri(String lastSuccessConnectedSrvUri) {
        this.lastSuccessConnectedSrvUri = lastSuccessConnectedSrvUri;

        return this;
    }


    /**
     * @return Path to server key store.
     */
    public String serverKeyStore() {
        return srvKeyStore;
    }

    /**
     * @param srvKeyStore Path to server key store.
     * @return {@code this} for chaining.
     */
    public AgentConfiguration serverKeyStore(String srvKeyStore) {
        this.srvKeyStore = srvKeyStore;

        return this;
    }

    /**
     * @return Server key store password.
     */
    public String serverKeyStorePassword() {
        return srvKeyStorePass;
    }

    /**
     * @param srvKeyStorePass Server key store password.
     * @return {@code this} for chaining.
     */
    public AgentConfiguration serverKeyStorePassword(String srvKeyStorePass) {
        this.srvKeyStorePass = srvKeyStorePass;

        return this;
    }

    /**
     * @return Path to server trust store.
     */
    public String serverTrustStore() {
        return srvTrustStore;
    }

    /**
     * @param srvTrustStore Path to server trust store.
     * @return {@code this} for chaining.
     */
    public AgentConfiguration serverTrustStore(String srvTrustStore) {
        this.srvTrustStore = srvTrustStore;

        return this;
    }

    /**
     * @return Server trust store password.
     */
    public String serverTrustStorePassword() {
        return srvTrustStorePass;
    }

    /**
     * @param srvTrustStorePass Server trust store password.
     * @return {@code this} for chaining.
     */
    public AgentConfiguration serverTrustStorePassword(String srvTrustStorePass) {
        this.srvTrustStorePass = srvTrustStorePass;

        return this;
    }

    /**
     * @return SSL cipher suites.
     */
    public List<String> cipherSuites() {
        return cipherSuites;
    }

    /**
     * @param cipherSuites SSL cipher suites.
     * @return {@code this} for chaining.
     */
    public AgentConfiguration cipherSuites(List<String> cipherSuites) {
        this.cipherSuites = cipherSuites;

        return this;
    }

    /**
     * @param cfgUrl URL.
     */
    public void load(URL cfgUrl) throws IOException {
        Properties props = new Properties();

        try (Reader reader = new InputStreamReader(cfgUrl.openStream(), UTF_8)) {
            props.load(reader);
        }

        String val = props.getProperty("server-uri");

        if (val != null)
            serverUri(val);

        val = props.getProperty("server-key-store");

        if (val != null)
            serverKeyStore(val);

        val = props.getProperty("server-key-store-password");

        if (val != null)
            serverKeyStorePassword(val);

        val = props.getProperty("server-trust-store");

        if (val != null)
            serverTrustStore(val);

        val = props.getProperty("server-trust-store-password");

        if (val != null)
            serverTrustStorePassword(val);

        val = props.getProperty("cipher-suites");

        if (val != null)
            cipherSuites(Arrays.asList(val.split(",")));
    }


    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder();

        String nl = System.lineSeparator();

        sb.append("URI to GMC                      : ").append(srvUri).append(nl);

        if (!F.isEmpty(srvKeyStore))
            sb.append("Server key store                : ").append(srvKeyStore).append(nl);

        if (!F.isEmpty(srvKeyStorePass))
            sb.append("Server key store password       : ").append(secured(srvKeyStorePass)).append(nl);

        if (!F.isEmpty(srvTrustStore))
            sb.append("Server trust store              : ").append(srvTrustStore).append(nl);

        if (!F.isEmpty(srvTrustStorePass))
            sb.append("Server trust store password     : ").append(secured(srvTrustStorePass)).append(nl);

        if (!F.isEmpty(cipherSuites))
            sb.append("Cipher suites                   : ").append(String.join(", ", cipherSuites)).append(nl);

        return sb.toString();
    }
}
