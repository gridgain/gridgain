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

    /** Compute tasks (execute by task name). */
    EXECUTE_TASK_BY_NAME(1),

    /** Cluster operations (state and WAL). */
    CLUSTER_API(2),

    /** Client discovery. */
    CLUSTER_GROUP_GET_NODES_ENDPOINTS(3),

    /** Cluster groups. */
    CLUSTER_GROUPS(4),

    /** Service invocation. This flag is not necessary and exists for legacy reasons. */
    SERVICE_INVOKE(5),

    /** Feature for use default query timeout if the qry timeout isn't set explicitly. */
    DEFAULT_QRY_TIMEOUT(6),

    /** Additional SqlFieldsQuery properties: partitions, updateBatchSize. */
    QRY_PARTITIONS_BATCH_SIZE(7),

    /** Binary configuration retrieval. */
    BINARY_CONFIGURATION(8),

    /** Service descriptors retrieval. */
    GET_SERVICE_DESCRIPTORS(9),

    /** Invoke service methods with caller context. */
    SERVICE_INVOKE_CALLCTX(10),

    /** Handle OP_HEARTBEAT and OP_GET_IDLE_TIMEOUT. */
    HEARTBEAT(11),

    // 12, 13 - reserved, used in Ignite.

    /** IndexQuery. */
    INDEX_QUERY(14),

    /** IndexQuery limit. */
    INDEX_QUERY_LIMIT(15),

    /** Custom Query label. */
    QRY_LABEL(16),

    /** Cache plugin configurations. GG-specific, use higher id to avoid conflicts. */
    CACHE_PLUGIN_CONFIGURATIONS(32),

    /** Vector Similarty function for VECTOR INDEX. */
    QUERY_INDEX_VECTOR_SIMILARITY(33);

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
