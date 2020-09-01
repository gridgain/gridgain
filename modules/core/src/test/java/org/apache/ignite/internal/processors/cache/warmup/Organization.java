/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.warmup;

import java.util.Arrays;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;

/**
 * Organization.
 */
class Organization {
    /** Id. */
    final long id;

    /** Name. */
    final String name;

    /**
     * Constructor.
     *
     * @param id Id.
     * @param name Name.
     */
    Organization(long id, String name) {
        this.id = id;
        this.name = name;
    }

    /**
     * Create query entity.
     *
     * @return New query entity.
     */
    static QueryEntity queryEntity() {
        return new QueryEntity(String.class, Organization.class)
            .addQueryField("id", Long.class.getName(), null)
            .addQueryField("name", String.class.getName(), null)
            .setIndexes(Arrays.asList(
                new QueryIndex("id"),
                new QueryIndex("name")
            ));
    }
}
