/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.checker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.resources.IgniteInstanceResource;

// TODO: 21.11.19 Tmp class only for test purposes, will be subsituted with Max's class.
public class JobProvidedByMaxThatDoAllWork extends ComputeJobAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Ignite instance. */
    @IgniteInstanceResource
    private IgniteEx ignite;

    // TODO: 19.11.19 parametrize with cache name, partition and key start.
    @Override public Object execute() throws IgniteException {
        // cacheId -> partition -> key with grid cache version
        Map<String, List<KeyCacheObject>> res = new HashMap<>();
        for (String cacheName : ignite.cacheNames()) {

            GridCacheContext<Object, Object> context = ignite.context().cache().cache(cacheName).context();

            List<KeyCacheObject> keys = new ArrayList<>();

            try {
                for (GridDhtLocalPartition localPartition: context.topology().localPartitions()){
                    GridCursor<? extends CacheDataRow> cursor = context.offheap().dataStore(localPartition).cursor();

                    if (cursor != null) {
                        while (cursor.next()) {
                            CacheDataRow row = cursor.get();

                            KeyCacheObject keyObj = row.key();
                            keyObj.partition(localPartition.id());

                            keys.add(keyObj);
                        }
                    }
                }
            }

            catch (IgniteCheckedException e) {
                // TODO: 19.11.19 consider some sort of fail-over here.
                // TODO: 19.11.19 Use proper exception and message here.
                throw new IllegalStateException();
            }

            res.put(cacheName, keys);
        }

        return res;
    }
}
