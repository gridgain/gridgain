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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * GridGain page IO interface.
 */
public interface PagePartitionMetaIOGG {
    /**
     * Upgrades a page to PagePartitionMetaIOGG.
     *
     * @param pageAddr Page address.
     */
    void upgradePage(long pageAddr);

    /**
     * Adds GridGain specific parameters to a string builder.
     *
     * @param pageAddr Page address.
     * @param sb String builder.
     */
    void specificFields(long pageAddr, GridStringBuilder sb);

    /**
     * Initializes GridGain specific fields.
     *
     * @param pageAddr Page address.
     * @param pageId Page id.
     * @param pageSize Page size.
     */
    void initSpecificFields(long pageAddr, long pageId, int pageSize);
}
