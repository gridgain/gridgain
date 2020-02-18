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

package org.apache.ignite.internal.processors.query.schema;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXTRA_INDEX_REBUILD_LOGGING;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;

/**
 * Class for accumulation of record types and number of indexed records in index tree.
 */
public class SchemaIndexCacheStat {
    /**
     * Indexed types.
     */
    public final Map<String, QueryTypeDescriptorImpl> types = new HashMap<>();

    /**
     * Indexed keys.
     */
    public int scanned;

    /**
     * Return is extra index create/rebuild logging enabled.
     *
     * @return Is extra index create/rebuild logging enabled.
     */
    public static boolean extraIndexBuildLogging() {
        return getBoolean(IGNITE_ENABLE_EXTRA_INDEX_REBUILD_LOGGING, false);
    }
}
