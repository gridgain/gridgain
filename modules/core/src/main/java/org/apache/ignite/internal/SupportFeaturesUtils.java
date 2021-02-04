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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteSystemProperties;

/**
 * Only for internal usage.
 */
public class SupportFeaturesUtils {
    /**
     * Undocumented experimental internal API that must not be touched by regular users.
     */
    public static final String IGNITE_DISTRIBUTED_META_STORAGE_FEATURE = "IGNITE_DISTRIBUTED_META_STORAGE_FEATURE";

    /**
     * Flag to turn on and off support of baseline topology for in-memory caches feature.
     *
     * For internal use only, must not be exposed to end users.
     */
    public static final String IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE =
        "IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE";

    /**
     * Flag to enable baseline auto-adjust feature.
     */
    public static final String IGNITE_BASELINE_AUTO_ADJUST_FEATURE = "IGNITE_BASELINE_AUTO_ADJUST_FEATURE";

    /**
     * Disables Cluster ID and Tag feature. Default value is <code>true</code>.
     */
    public static final String IGNITE_CLUSTER_ID_AND_TAG_FEATURE = "IGNITE_CLUSTER_ID_AND_TAG_FEATURE";

    /**
     * Disables cache configuration splitting.
     */
    public static final String IGNITE_USE_BACKWARD_COMPATIBLE_CONFIGURATION_SPLITTER =
        "IGNITE_USE_BACKWARD_COMPATIBLE_CONFIGURATION_SPLITTER";

    /**
     * PME-free switch explicitly disabled.
     */
    public static final String IGNITE_PME_FREE_SWITCH_DISABLED = "IGNITE_PME_FREE_SWITCH_DISABLED";

    /**
     * Disables keys order preserving for compound .
     */
    public static final String IGNITE_SPECIFIED_SEQ_PK_KEYS_DISABLED = "IGNITE_SPECIFIED_SEQ_PK_KEYS_DISABLED";

    /**
     * @param featureName System property feature name.
     * @return {@code true} If given feature is enabled.
     */
    public static boolean isFeatureEnabled(String featureName) {
        if (IGNITE_DISTRIBUTED_META_STORAGE_FEATURE.equals(featureName) ||
            IGNITE_CLUSTER_ID_AND_TAG_FEATURE.equals(featureName) ||
            IGNITE_BASELINE_AUTO_ADJUST_FEATURE.equals(featureName) ||
            IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE.equals(featureName)
        )
            return IgniteSystemProperties.getBoolean(featureName, true);

        return IgniteSystemProperties.getBoolean(featureName, false);
    }
}
