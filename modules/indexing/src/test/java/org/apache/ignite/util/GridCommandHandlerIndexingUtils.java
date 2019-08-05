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

package org.apache.ignite.util;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/** Utility class for tests. */
public class GridCommandHandlerIndexingUtils {
    /** Test cache name. */
    public static final String CACHE_NAME = "persons-cache-vi";

    /** Test group name. */
    public static final String GROUP_NAME = "group1";

    /** Private constructor */
    private GridCommandHandlerIndexingUtils() {
        throw new IllegalArgumentException("don't create");
    }

    /**
     * Create and fill cache.
     *
     * @param ignite node
     * @param cacheName cache name
     * @param grpName group name
     * */
    public static void createAndFillCache(final Ignite ignite, final String cacheName, final String grpName) {
        ignite.createCache(new CacheConfiguration<Integer, Person>()
            .setName(cacheName)
            .setGroupName(grpName)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1)
            .setQueryEntities(F.asList(personEntity(true, true)))
            .setAffinity(new RendezvousAffinityFunction(false, 32)));

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        try (IgniteDataStreamer<Integer, Person> streamer = ignite.dataStreamer(cacheName)) {
            for (int i = 0; i < 10_000; i++)
                streamer.addData(i, new Person(rand.nextInt(), String.valueOf(rand.nextLong())));
        }
    }

    /**
     * @param idxName Index name.
     * @param idxOrgId Index org id.
     */
    private static QueryEntity personEntity(boolean idxName, boolean idxOrgId) {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Person.class.getName());

        entity.addQueryField("orgId", Integer.class.getName(), null);
        entity.addQueryField("name", String.class.getName(), null);

        List<QueryIndex> idxs = new ArrayList<>();

        if (idxName) {
            QueryIndex idx = new QueryIndex("name");

            idxs.add(idx);
        }

        if (idxOrgId) {
            QueryIndex idx = new QueryIndex("orgId");

            idxs.add(idx);
        }

        entity.setIndexes(idxs);

        return entity;
    }
}
