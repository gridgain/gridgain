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

package org.apache.ignite.internal.processors.cache.persistence.metastorage;

import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageLayout;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.SimpleDataPageIO;

/**
 * Data pages IO for using with meta store.
 */
public class MetastoreDataPageIO extends SimpleDataPageIO {
    /** */
    public static final IOVersions<MetastoreDataPageIO> SMALL_LAYOUT_VERSIONS = new IOVersions<>(
        new MetastoreDataPageIO(1, new PageLayout(false))
    );

    /** */
    public static final IOVersions<MetastoreDataPageIO> BIG_LAYOUT_VERSIONS = new IOVersions<>(
        new MetastoreDataPageIO(1, new PageLayout(true))
    );

    public static IOVersions<MetastoreDataPageIO> metastorageIOVersions(boolean bigPages) {
        return bigPages ? BIG_LAYOUT_VERSIONS : SMALL_LAYOUT_VERSIONS;
    }

    /**
     * @param ver Version.
     * @param pageLayout Page layout.
     */
    public MetastoreDataPageIO(int ver, PageLayout pageLayout) {
        super(T_DATA_METASTORAGE, ver, pageLayout);
    }
}
