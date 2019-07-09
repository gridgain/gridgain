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
package org.apache.ignite.internal.processors.cluster;

import java.util.Random;

/**
 *
 */
public class ClusterTagGenerator {
    public static String[] IN_MEMORY_CLUSTER_TAGS = new String[] {
        "in_memory_grid",
        "in_memory_cache",
        "fast_as_RAM"
    };

    public static String[] PERSISTENT_CLUSTER_TAGS = new String[] {
        "fast_and_durable",
        "distributed_database",
        "distributed_and_reliable"
    };

    /**
     * Generates cluster tag.
     *
     * @param persistenceEnabled Flag showing if persistence is enabled in the cluster.
     * @return String to use as default cluster tag.
     */
    public static String generateTag(boolean persistenceEnabled) {
        if (persistenceEnabled)
            return PERSISTENT_CLUSTER_TAGS[new Random().nextInt(PERSISTENT_CLUSTER_TAGS.length)];
        else
            return IN_MEMORY_CLUSTER_TAGS[new Random().nextInt(IN_MEMORY_CLUSTER_TAGS.length)];
    }
}
