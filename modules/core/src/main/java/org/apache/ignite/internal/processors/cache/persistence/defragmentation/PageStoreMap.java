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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation;

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.store.PageStoreCollection;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.collection.IntRWHashMap;
import org.apache.ignite.internal.util.typedef.internal.S;

/** */
class PageStoreMap implements PageStoreCollection {
    /** GroupId -> PartId -> PageStore */
    private final IntMap<IntMap<PageStore>> grpPageStoresMap = new IntRWHashMap<>();

    /** */
    public void addPageStore(
        int grpId,
        int partId,
        PageStore pageStore
    ) {
        IntMap<PageStore> pageStoresMap = grpPageStoresMap.get(grpId);

        if (pageStoresMap == null) {
            grpPageStoresMap.putIfAbsent(grpId, new IntRWHashMap<>());

            pageStoresMap = grpPageStoresMap.get(grpId);
        }

        pageStoresMap.put(partId, pageStore);
    }

    /** */
    public void removePageStore(
        int grpId,
        int partId
    ) {
        IntMap<PageStore> pageStoresMap = grpPageStoresMap.get(grpId);

        if (pageStoresMap != null)
            pageStoresMap.remove(partId);
    }

    /** */
    public void clear(int grpId) {
        grpPageStoresMap.remove(grpId);
    }

    /** {@inheritDoc} */
    @Override public PageStore getStore(int grpId, int partId) throws IgniteCheckedException {
        IntMap<PageStore> partPageStoresMap = grpPageStoresMap.get(grpId);

        if (partPageStoresMap == null) {
            throw new IgniteCheckedException(S.toString("Page store map not found. ",
                "grpId", grpId, false,
                "partId", partId, false,
                "keys", Arrays.toString(grpPageStoresMap.keys()), false,
                "this", hashCode(), false
            ));
        }

        PageStore pageStore = partPageStoresMap.get(partId);

        if (pageStore == null) {
            throw new IgniteCheckedException(S.toString("Page store not found. ",
                "grpId", grpId, false,
                "partId", partId, false,
                "keys", Arrays.toString(partPageStoresMap.keys()), false,
                "this", hashCode(), false
            ));
        }

        return pageStore;
    }

    /** {@inheritDoc} */
    @Override public Collection<PageStore> getStores(int grpId) {
        IntMap<PageStore> partPageStoresMap = grpPageStoresMap.get(grpId);

        return partPageStoresMap == null ? null : partPageStoresMap.values();
    }
}
