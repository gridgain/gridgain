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
import org.apache.ignite.internal.AbstractThinProtocolFeature;

/**
 * Defines supported features for JDBC thin client.
 */
public enum JdbcThinFeature implements AbstractThinProtocolFeature {
    RESERVED(0);

    /** Feature id. */
    private final int featureId;

    /**
     * @param id Feature ID.
     */
    JdbcThinFeature(int id) {
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
    public static EnumSet<JdbcThinFeature> enumSet(byte[] bytes) {
        return AbstractThinProtocolFeature.enumSet(bytes, values());
    }

    /**
     * @return Byte array representing all supported features by current node.
     */
    public static byte[] allFeatures() {
        return AbstractThinProtocolFeature.features(values());
    }
}
