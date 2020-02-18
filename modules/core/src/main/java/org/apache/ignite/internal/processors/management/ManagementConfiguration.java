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

    /** Default console URI. */
    private static final String DFLT_CONSOLE_URIS = F.isEmpty(IgniteProperties.get("ignite.control.center.uris"))
        ? "http://localhost:3000" : IgniteProperties.get("ignite.control.center.uris");

    /** */
    private List<String> consoleUris = asList(DFLT_CONSOLE_URIS.split(","));

    /** */
    private boolean enabled = true;

    /** */
    @GridToStringExclude
    private String consoleKeyStore;

    /** */
    @GridToStringExclude
    private String consoleKeyStorePass;

    /** */
    @GridToStringExclude
    private String consoleTrustStore;

    /** */
    @GridToStringExclude
    private String consoleTrustStorePass;

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
    public List<String> getConsoleUris() {
        return consoleUris;
    }

    /**
     * @param consoleUris URI.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setConsoleUris(List<String> consoleUris) {
        this.consoleUris = consoleUris;

        return this;
    }

    /**
     * @return Control Center key store.
     */
    public String getConsoleKeyStore() {
        return consoleKeyStore;
    }

    /**
     * @param consoleKeyStore Control Center key store.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setConsoleKeyStore(String consoleKeyStore) {
        this.consoleKeyStore = consoleKeyStore;

        return this;
    }

    /**
     * @return Control Center key store password.
     */
    public String getConsoleKeyStorePassword() {
        return consoleKeyStorePass;
    }

    /**
     * @param consoleKeyStorePass Control Center key store password.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setConsoleKeyStorePassword(String consoleKeyStorePass) {
        this.consoleKeyStorePass = consoleKeyStorePass;

        return this;
    }

    /**
     * @return Control Center trust store.
     */
    public String getConsoleTrustStore() {
        return consoleTrustStore;
    }

    /**
     * @param consoleTrustStore Path to Control Center trust store.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setConsoleTrustStore(String consoleTrustStore) {
        this.consoleTrustStore = consoleTrustStore;

        return this;
    }

    /**
     * @return Control Center trust store password.
     */
    public String getConsoleTrustStorePassword() {
        return consoleTrustStorePass;
    }

    /**
     * @param consoleTrustStorePass Console trust store password.
     * @return {@code this} for chaining.
     */
    public ManagementConfiguration setConsoleTrustStorePassword(String consoleTrustStorePass) {
        this.consoleTrustStorePass = consoleTrustStorePass;

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
            Objects.equals(consoleUris, that.consoleUris) &&
            Objects.equals(consoleKeyStore, that.consoleKeyStore) &&
            Objects.equals(consoleKeyStorePass, that.consoleKeyStorePass) &&
            Objects.equals(consoleTrustStore, that.consoleTrustStore) &&
            Objects.equals(consoleTrustStorePass, that.consoleTrustStorePass) &&
            Objects.equals(cipherSuites, that.cipherSuites) &&
            Objects.equals(securitySesTimeout, that.securitySesTimeout) &&
            Objects.equals(securitySesExpirationTimeout, that.securitySesExpirationTimeout);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(
            enabled,
            consoleUris,
            consoleKeyStore,
            consoleKeyStorePass,
            consoleTrustStore,
            consoleTrustStorePass,
            cipherSuites,
            securitySesTimeout,
            securitySesExpirationTimeout
        );
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(enabled);
        U.writeCollection(out, consoleUris);
        U.writeString(out, consoleKeyStore);
        U.writeString(out, consoleKeyStorePass);
        U.writeString(out, consoleTrustStore);
        U.writeString(out, consoleTrustStorePass);
        U.writeCollection(out, cipherSuites);
        out.writeLong(securitySesTimeout);
        out.writeLong(securitySesExpirationTimeout);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        enabled = in.readBoolean();
        consoleUris = U.readList(in);
        consoleKeyStore = U.readString(in);
        consoleKeyStorePass = U.readString(in);
        consoleTrustStore = U.readString(in);
        consoleTrustStorePass = U.readString(in);
        cipherSuites = U.readList(in);
        securitySesTimeout = in.readLong();
        securitySesExpirationTimeout = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ManagementConfiguration.class, this);
    }
}
