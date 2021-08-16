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

package org.apache.ignite.internal.pagemem.store;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;

/**
 * A collection that contains {@link PageStore} elements.
 */
public interface PageStoreCollection {
    /**
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @return Page store for the corresponding parameters.
     * @throws IgniteCheckedException If cache or partition with the given ID was not created.
     */
    public PageStore getStore(int grpId, int partId) throws IgniteCheckedException;

    /**
     * @param grpId Cache group ID.
     * @return Collection of related page stores.
     * @throws IgniteCheckedException If failed.
     */
    public Collection<PageStore> getStores(int grpId) throws IgniteCheckedException;
}
