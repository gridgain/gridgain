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

package org.apache.ignite.internal.client.thin;

import java.util.EnumSet;

/**
 * Protocol Context.
 */
public class ProtocolContext {
    /** Protocol version. */
    private final ProtocolVersion ver;

    /** Features. */
    private final EnumSet<ProtocolBitmaskFeature> features;

    /**
     * @param ver Protocol version.
     * @param features Supported features.
     */
    public ProtocolContext(ProtocolVersion ver, EnumSet<ProtocolBitmaskFeature> features) {
        this.ver = ver;
        this.features = features != null ? features : EnumSet.noneOf(ProtocolBitmaskFeature.class);
    }

    /**
     * @return {@code true} if bitmask protocol feature supported.
     */
    public boolean isFeatureSupported(ProtocolBitmaskFeature feature) {
        return features.contains(feature);
    }

    /**
     * @return {@code true} if protocol version feature supported.
     */
    public boolean isFeatureSupported(ProtocolVersionFeature feature) {
        return isFeatureSupported(ver, feature);
    }

    /**
     * @return Supported features.
     */
    public EnumSet<ProtocolBitmaskFeature> features() {
        return features;
    }

    /**
     * @return Protocol version.
     */
    public ProtocolVersion version() {
        return ver;
    }

    /**
     * Check if the feature was supported in the protocol version.
     * @param ver Protocol version.
     * @param feature Feature which support should be checked.
     * @return {@code true} if the feature was supported in the protocol version.
     */
    public static boolean isFeatureSupported(ProtocolVersion ver, ProtocolVersionFeature feature) {
        return ver.compareTo(feature.verIntroduced()) >= 0;
    }
}
