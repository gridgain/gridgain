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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.internal.pagemem.store.PageStore;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class CacheStore {
    /** Index store. */
    private final PageStore idxStore;

    /** Partition stores. */
    private final PageStore[] partStores;

    /** */
    private final Collection<PageStore> cacheStores;

    /**
     */
    CacheStore(PageStore idxStore, PageStore[] partStores) {
        this.idxStore = requireNonNull(idxStore);
        this.partStores = requireNonNull(partStores);

        cacheStores = new ArrayList<>(partStores.length + 1);
        cacheStores.addAll(Arrays.asList(partStores));
        cacheStores.add(idxStore);
    }

    /** */
    public PageStore idxStore() {
        return idxStore;
    }

    /** */
    public PageStore[] partStores() {
        return partStores;
    }

    /** */
    public Collection<PageStore> cacheStores() {
        return cacheStores;
    }
}
