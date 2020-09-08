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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.EnumSet;
import java.util.Objects;
import java.util.TimeZone;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;

import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext.VER_2_4_0;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext.VER_2_7_0;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext.VER_2_8_0;

/**
 * Protocol context for JDBC protocol. Holds protocol version and supported features.
 */
public class JdbcProtocolContext {
    /** Protocol version. */
    private final ClientListenerProtocolVersion ver;

    /** Features. */
    private final EnumSet<JdbcThinFeature> features;

    /** Server timezone. */
    private final TimeZone srvTz;

    /** {@code true} on client side. */
    private final boolean client;

    /** {@code true} if binary should not be deserialized. */
    private final boolean keepBinary;

    /**
     * @param ver Protocol version.
     * @param features Supported features.
     * @param srvTz Server timezone.
     * @param keepBinary Wether to keep objects in binary form.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public JdbcProtocolContext(ClientListenerProtocolVersion ver, EnumSet<JdbcThinFeature> features, TimeZone srvTz,
        boolean client, boolean keepBinary) {
        assert Objects.nonNull(features);

        this.ver = ver;
        this.features = features;
        this.srvTz = srvTz;
        this.client = client;
        this.keepBinary = keepBinary;
    }

    /**
     * @return {@code true} if JDBC streaming supported.
     */
    public boolean isStreamingSupported() {
        return ver.compareTo(VER_2_4_0) >= 0;
    }

    /**
     * @return {@code true} if JDBC streaming supported.
     */
    public boolean isAutoCommitSupported() {
        return ver.compareTo(VER_2_7_0) >= 0;
    }

    /**
     * @return {@code true} if JDBC streaming supported.
     */
    public boolean isTableTypesSupported() {
        return ver.compareTo(VER_2_8_0) >= 0;
    }

    /**
     * @return {@code true} if JDBC streaming supported.
     */
    public boolean isAffinityAwarenessSupported() {
        return ver.compareTo(VER_2_8_0) >= 0;
    }

    /**
     * @param feature {@code true} if given feature supported.
     */
    public boolean isFeatureSupported(JdbcThinFeature feature) {
        return features.contains(feature);
    }

    /**
     * @return Supported features.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    EnumSet<JdbcThinFeature> features() {
        return features;
    }

    /**
     * @return {@code true} on client side.
     */
    public boolean client() {
        return client;
    }

    /**
     * @return {@code true} if binary should not be deserialized.
     */
    public boolean keepBinary() {
        return keepBinary;
    }

    /**
     * @return Server time zone.
     */
    public TimeZone serverTimeZone() {
        return srvTz;
    }
}
