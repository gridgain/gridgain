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

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.ThinClientFeatures;

/**
 * Defines supported features for JDBC thin client.
 */
public class JdbcThinFeatures extends ThinClientFeatures {
    /** All features. */
    private static final Set<Feature> ALL_FEATURES = new HashSet<>();

    static {
        // Register all features: register(ALL_FEATURES, <feature>);
    }

    /**
     * @param features Supported features.
     */
    public JdbcThinFeatures(byte [] features) {
        super(features);
    }

    /**
     * @return All supported features.
     */
    public static byte[] allFeatures() {
        return features(ALL_FEATURES);
    }

    /** */
    public static class Feature extends ThinClientFeatures.Feature {
        /**
         * @param featureId Feature Id.
         * @param name Feature name.
         */
        public Feature(int featureId, String name) {
            super(featureId, name);
        }
    }
}
