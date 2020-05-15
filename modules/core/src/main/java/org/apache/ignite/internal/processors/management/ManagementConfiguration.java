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

package org.apache.ignite.internal.processors.management;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.IgniteProperties;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.util.Arrays.asList;

/**
 * This class defines Control Center agent configuration.
 */
public class ManagementConfiguration extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default control center URI. */
    private static final String DFLT_CONTROL_CENTER_URIS = F.isEmpty(IgniteProperties.get("ignite.control.center.uris"))
        ? "http://localhost:3000" : IgniteProperties.get("ignite.control.center.uris");

    /** */
    private List<String> uris = asList(DFLT_CONTROL_CENTER_URIS.split(","));

    /** */
    private boolean enabled = true;

    /** */
    @GridToStringExclude
    private byte[] keyStore;

    /** */
    @GridToStringExclude
    private String keyStorePass;

    /** */
    @GridToStringExclude
    private String keyStoreType;

    /** */
    @GridToStringExclude
    private byte[] trustStore;

    /** */
    @GridToStringExclude
    private String trustStorePass;

    /** */
    @GridToStringExclude
    private String trustStoreType;

    /** */
    private List<String> cipherSuites;

    /** Security session timeout, in milliseconds. */
    private long securitySesTimeout = 5 * 60 * 1000;

    /** Security session expiration timeout, in milliseconds. */
    private long securitySesExpirationTimeout = 30 * 1000;

    /**
     * @return Value of enabled flag.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @param enabled Enabled.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setEnabled(boolean enabled) {
        this.enabled = enabled;

        return this;
    }

    /**
     * @return Control Center URI.
     */
    public List<String> getUris() {
        return uris;
    }

    /**
     * @param consoleUris URI.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setUris(List<String> consoleUris) {
        this.uris = consoleUris;

        return this;
    }

    /**
     * @return Control Center key store.
     */
    public byte[] getKeyStore() {
        return keyStore;
    }

    /**
     * @param keyStore Control Center key store.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setKeyStore(byte[] keyStore) {
        this.keyStore = keyStore;

        return this;
    }

    /**
     * @return Control Center key store password.
     */
    public String getKeyStorePassword() {
        return keyStorePass;
    }

    /**
     * @param keyStorePass Control Center key store password.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setKeyStorePassword(String keyStorePass) {
        this.keyStorePass = keyStorePass;

        return this;
    }

    /**
     * @return Control Center key store type.
     */
    public String getKeyStoreType() {
        return keyStoreType;
    }

    /**
     * @param keyStoreType Control Center key store type.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;

        return this;
    }

    /**
     * @return Control Center trust store.
     */
    public byte[] getTrustStore() {
        return trustStore;
    }

    /**
     * @param trustStore Path to Control Center trust store.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setTrustStore(byte[] trustStore) {
        this.trustStore = trustStore;

        return this;
    }

    /**
     * @return Control Center trust store password.
     */
    public String getTrustStorePassword() {
        return trustStorePass;
    }

    /**
     * @param trustStorePass Console trust store password.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setTrustStorePassword(String trustStorePass) {
        this.trustStorePass = trustStorePass;

        return this;
    }

    /**
     * @return Control Center trust store type.
     */
    public String getTrustStoreType() {
        return trustStoreType;
    }

    /**
     * @param trustStoreType Console trust store type.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setTrustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;

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
     * @return Security session timeout.
     */
    public long getSecuritySessionTimeout() {
        return securitySesTimeout;
    }

    /**
     * @param securitySesTimeout Session timeout in milliseconds.
     */
    public ManagementConfiguration setSecuritySessionTimeout(long securitySesTimeout) {
        this.securitySesTimeout = securitySesTimeout;

        return this;
    }

    /**
     * @return Security session expiration timeout in milliseconds after which we are try to re-authenticate.
     */
    public long getSecuritySessionExpirationTimeout() {
        return securitySesExpirationTimeout;
    }

    /**
     * @param securitySesExpirationTimeout Session expiration timeout.
     */
    public ManagementConfiguration setSecuritySessionExpirationTimeout(long securitySesExpirationTimeout) {
        this.securitySesExpirationTimeout = securitySesExpirationTimeout;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ManagementConfiguration that = (ManagementConfiguration)o;

        return enabled == that.enabled &&
            Objects.equals(uris, that.uris) &&
            Objects.equals(keyStore, that.keyStore) &&
            Objects.equals(keyStorePass, that.keyStorePass) &&
            Objects.equals(trustStore, that.trustStore) &&
            Objects.equals(trustStorePass, that.trustStorePass) &&
            Objects.equals(cipherSuites, that.cipherSuites) &&
            Objects.equals(securitySesTimeout, that.securitySesTimeout) &&
            Objects.equals(securitySesExpirationTimeout, that.securitySesExpirationTimeout);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(
            enabled,
            uris,
            keyStore,
            keyStorePass,
            trustStore,
            trustStorePass,
            cipherSuites,
            securitySesTimeout,
            securitySesExpirationTimeout
        );
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(enabled);
        U.writeCollection(out, uris);
        U.writeString(out, keyStoreType);
        U.writeByteArray(out, keyStore);
        U.writeString(out, keyStorePass);
        U.writeString(out, trustStoreType);
        U.writeByteArray(out, trustStore);
        U.writeString(out, trustStorePass);
        U.writeCollection(out, cipherSuites);
        out.writeLong(securitySesTimeout);
        out.writeLong(securitySesExpirationTimeout);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        enabled = in.readBoolean();
        uris = U.readList(in);
        keyStoreType = U.readString(in);
        keyStore = U.readByteArray(in);
        keyStorePass = U.readString(in);
        trustStoreType = U.readString(in);
        trustStore = U.readByteArray(in);
        trustStorePass = U.readString(in);
        cipherSuites = U.readList(in);
        securitySesTimeout = in.readLong();
        securitySesExpirationTimeout = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ManagementConfiguration.class, this);
    }
}
