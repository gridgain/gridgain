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

package org.apache.ignite.internal.processors.platform.client;

import java.util.EnumSet;
import java.util.TimeZone;

import org.apache.ignite.internal.ThinProtocolFeature;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;

/**
 * Protocol context for thin client protocol. Holds protocol version and supported features.
 */
public class ClientProtocolContext {
    /** Protocol version. */
    private final ClientListenerProtocolVersion ver;

    /** Features. */
    private final EnumSet<ClientBitmaskFeature> features;

    /** Client timezone. */
    private TimeZone clientTz;

    /**
     * @param ver Protocol version.
     * @param features Supported features.
     */
    public ClientProtocolContext(ClientListenerProtocolVersion ver, EnumSet<ClientBitmaskFeature> features) {
        this.ver = ver;
        this.features = features != null ? features : EnumSet.noneOf(ClientBitmaskFeature.class);
    }

    /**
     * @return {@code true} if bitmask protocol feature supported.
     */
    public boolean isFeatureSupported(ClientBitmaskFeature feature) {
        return features.contains(feature);
    }

    /**
     * @return {@code true} if protocol version feature supported.
     */
    public boolean isFeatureSupported(ClientProtocolVersionFeature feature) {
        return isFeatureSupported(ver, feature);
    }

    /**
     * @return Supported features.
     */
    public EnumSet<ClientBitmaskFeature> features() {
        return features;
    }

    /**
     * @return Supported features as byte array.
     */
    public byte[] featureBytes() {
        return ThinProtocolFeature.featuresAsBytes(features);
    }

    /**
     * @return Protocol version.
     */
    public ClientListenerProtocolVersion version() {
        return ver;
    }

    /**
     * Check if the feature was supported in the protocol version.
     * @param ver Protocol version.
     * @param feature Feature which support should be checked.
     * @return {@code true} if the feature was supported in the protocol version.
     */
    public static boolean isFeatureSupported(ClientListenerProtocolVersion ver, ClientProtocolVersionFeature feature) {
        return ver.compareTo(feature.verIntroduced()) >= 0;
    }

    /**
     * @return Client time zone.
     */
    public TimeZone clientTimeZone() {
        return clientTz;
    }

    /**
     * @param clientTz Client time zone.
     */
    public void clientTimeZone(TimeZone clientTz) {
        this.clientTz = clientTz;
    }
}
