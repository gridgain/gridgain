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
import org.apache.ignite.internal.ThinProtocolFeature;

/**
 * Defines supported features for thin client.
 */
public enum ClientBitmaskFeature implements ThinProtocolFeature {
    /** Feature for user attributes. */
    USER_ATTRIBUTES(0),

    /** Feature for use default query timeout if the qry timeout isn't set explicitly. */
    DEFAULT_QRY_TIMEOUT(1);

    /** */
    private static final EnumSet<ClientBitmaskFeature> ALL_FEATURES_AS_ENUM_SET =
        EnumSet.allOf(ClientBitmaskFeature.class);

    /** Feature id. */
    private final int featureId;

    /**
     * @param id Feature ID.
     */
    ClientBitmaskFeature(int id) {
        featureId = id;
    }

    /** {@inheritDoc} */
    @Override public int featureId() {
        return featureId;
    }

    /**
     * @param bytes Feature byte array.
     * @return Set of supported features.
     */
    public static EnumSet<ClientBitmaskFeature> enumSet(byte[] bytes) {
        return ThinProtocolFeature.enumSet(bytes, ClientBitmaskFeature.class);
    }

    /** */
    public static EnumSet<ClientBitmaskFeature> allFeaturesAsEnumSet() {
        return ALL_FEATURES_AS_ENUM_SET.clone();
    }
}
