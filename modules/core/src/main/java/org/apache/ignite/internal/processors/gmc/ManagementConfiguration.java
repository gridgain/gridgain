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

package org.apache.ignite.internal.processors.gmc;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This class defines Management Console Agent configuration.
 */
public class ManagementConfiguration extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default server URI. */
    private static final String DFLT_SERVER_URI = "http://localhost:3000";

    /** */
    private boolean enable = true;

    /** */
    private List<String> srvUris = Collections.singletonList(DFLT_SERVER_URI);

    /** */
    @GridToStringExclude
    private String srvKeyStore;

    /** */
    @GridToStringExclude
    private String srvKeyStorePass;

    /** */
    @GridToStringExclude
    private String srvTrustStore;

    /** */
    @GridToStringExclude
    private String srvTrustStorePass;

    /** */
    private List<String> cipherSuites;

    /** Session timeout, in milliseconds. */
    private long sesTimeout = 5 * 60 * 1000;

    /** Session expiration timeout, in milliseconds. */
    private long sesExpirationTimeout = 30 * 1000;

    /**
     * @return value of enable
     */
    public boolean isEnable() {
        return enable;
    }

    /**
     * @param enable Enable.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setEnable(boolean enable) {
        this.enable = enable;

        return this;
    }

    /**
     * @return Server URI.
     */
    public List<String> getServerUris() {
        return srvUris;
    }

    /**
     * @param srvUri URI.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setServerUris(List<String> srvUri) {
        this.srvUris = srvUri;

        return this;
    }

    /**
     * @return Server key store.
     */
    public String getServerKeyStore() {
        return srvKeyStore;
    }

    /**
     * @param srvKeyStore Server key store.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setServerKeyStore(String srvKeyStore) {
        this.srvKeyStore = srvKeyStore;

        return this;
    }

    /**
     * @return Server key store password.
     */
    public String getServerKeyStorePassword() {
        return srvKeyStorePass;
    }

    /**
     * @param srvKeyStorePass Server key store password.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setServerKeyStorePassword(String srvKeyStorePass) {
        this.srvKeyStorePass = srvKeyStorePass;

        return this;
    }

    /**
     * @return Server trust store.
     */
    public String getServerTrustStore() {
        return srvTrustStore;
    }

    /**
     * @param srvTrustStore Path to server trust store.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setServerTrustStore(String srvTrustStore) {
        this.srvTrustStore = srvTrustStore;

        return this;
    }

    /**
     * @return Server trust store password.
     */
    public String getServerTrustStorePassword() {
        return srvTrustStorePass;
    }

    /**
     * @param srvTrustStorePass Server trust store password.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setServerTrustStorePassword(String srvTrustStorePass) {
        this.srvTrustStorePass = srvTrustStorePass;

        return this;
    }

    /**
     * @return SSL cipher suites.
     */
    public List<String> getCipherSuites() {
        return cipherSuites;
    }

    /**
     * @param cipherSuites SSL cipher suites.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setCipherSuites(List<String> cipherSuites) {
        this.cipherSuites = cipherSuites;

        return this;
    }

    /**
     * @return Session timeout.
     */
    public long getSessionTimeout() {
        return sesTimeout;
    }

    /**
     * @param sesTimeout Session timeout in milliseconds.
     */
    public ManagementConfiguration setSessionTimeout(long sesTimeout) {
        this.sesTimeout = sesTimeout;
        return this;
    }

    /**
     * @return Session expiration timeout in milliseconds after which we are try to re-authenticate.
     */
    public long getSessionExpirationTimeout() {
        return sesExpirationTimeout;
    }

    /**
     * @param sesExpirationTimeout Session expiration timeout.
     */
    public ManagementConfiguration setSessionExpirationTimeout(long sesExpirationTimeout) {
        this.sesExpirationTimeout = sesExpirationTimeout;
        return this;
    }

    /**
     * @return {@code True} if contains server endpoints.
     */
    public boolean hasServerUris() {
        return !F.isEmpty(srvUris);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ManagementConfiguration that = (ManagementConfiguration)o;
        
        return enable == that.enable &&
            Objects.equals(srvUris, that.srvUris) &&
            Objects.equals(srvKeyStore, that.srvKeyStore) &&
            Objects.equals(srvKeyStorePass, that.srvKeyStorePass) &&
            Objects.equals(srvTrustStore, that.srvTrustStore) &&
            Objects.equals(srvTrustStorePass, that.srvTrustStorePass) &&
            Objects.equals(cipherSuites, that.cipherSuites) &&
            Objects.equals(sesTimeout, that.sesTimeout) &&
            Objects.equals(sesExpirationTimeout, that.sesExpirationTimeout);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(
            enable,
            srvUris,
            srvKeyStore,
            srvKeyStorePass,
            srvTrustStore,
            srvTrustStorePass,
            cipherSuites,
            sesTimeout,
            sesExpirationTimeout
        );
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(enable);
        U.writeCollection(out, srvUris);
        U.writeString(out, srvKeyStore);
        U.writeString(out, srvKeyStorePass);
        U.writeString(out, srvTrustStore);
        U.writeString(out, srvTrustStorePass);
        U.writeCollection(out, cipherSuites);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        enable = in.readBoolean();
        srvUris = U.readList(in);
        srvKeyStore = U.readString(in);
        srvKeyStorePass = U.readString(in);
        srvTrustStore = U.readString(in);
        srvTrustStorePass = U.readString(in);
        cipherSuites = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ManagementConfiguration.class, this);
    }
}
