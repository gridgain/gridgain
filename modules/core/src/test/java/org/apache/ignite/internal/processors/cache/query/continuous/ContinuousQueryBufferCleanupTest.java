/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

public class ContinuousQueryBufferCleanupTest extends GridCommonAbstractTest {

    /** */
    private final static int RECORDS_CNT = 10000;

    /** */
    private final static int ACK_THRESHOLD = 100;

    /** */
    private final static int BUFFER_SIZE = 32;

    /** */
    private final static String REMOTE_ROUTINE_INFO_CLASS_NAME = "org.apache.ignite.internal.processors.continuous.GridContinuousProcessor$RemoteRoutineInfo";

    /** */
    private final static String BATCH_CLASS_NAME = "org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryEventBuffer$Batch";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testSingleNode() throws Exception {
        IgniteEx srv = startGrid("srv");

        IgniteConfiguration clientCfg = optimize(getConfiguration("client").setClientMode(true));

        Ignite client = startGrid(clientCfg);

        CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<>("testCache");

        IgniteCache<Integer, String> cache = client.getOrCreateCache(cacheCfg);

        ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();

        qry.setLocalListener((evts) -> evts.forEach(e -> System.out.println("key=" + e.getKey() + ", val=" + e.getValue())));

        cache.query(qry);

        for (int i = 0; i < RECORDS_CNT; i++)
            cache.put(i, Integer.toString(i));

        waitForCondition((PA)() -> false, 2000);

        validateBuffer(srv);
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testMultipleNodes() throws Exception {
        IgniteEx srv = startGrid("srv");

        IgniteEx srv2 = startGrid("srv2");

        IgniteConfiguration clientCfg = optimize(getConfiguration("client").setClientMode(true));

        Ignite client = startGrid(clientCfg);

        CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<Integer, String>("testCache")
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(32, null));

        IgniteCache<Integer, String> cache = client.getOrCreateCache(cacheCfg);

        ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();

        qry.setLocalListener((evts) -> evts.forEach(e -> System.out.println("key=" + e.getKey() + ", val=" + e.getValue())));

        cache.query(qry);

        for (int i = 0; i < RECORDS_CNT; i++)
            cache.put(i, Integer.toString(i));

        waitForCondition((PA)() -> false, 2000);

        validateBuffer(srv);

        validateBuffer(srv2);
    }

    private void validateBuffer(IgniteEx srv) throws ClassNotFoundException {
        GridContinuousProcessor contProc = srv.context().continuous();

        ConcurrentMap<UUID, Object> rmtInfos = GridTestUtils.getFieldValue(contProc, GridContinuousProcessor.class, "rmtInfos");

        Object rmtRoutineInfo = rmtInfos.values().toArray()[0];

        CacheContinuousQueryHandler hnd = GridTestUtils.getFieldValue(rmtRoutineInfo, Class.forName(REMOTE_ROUTINE_INFO_CLASS_NAME), "hnd");

        ConcurrentMap<Integer, CacheContinuousQueryEventBuffer> entryBufs = GridTestUtils.getFieldValue(hnd, CacheContinuousQueryHandler.class, "entryBufs");

        int notNullCnt = 0;

        for (CacheContinuousQueryEventBuffer evtBuf : entryBufs.values()) {

            AtomicReference<Object> curBatch = GridTestUtils.getFieldValue(evtBuf, CacheContinuousQueryEventBuffer.class, "curBatch");

            if (curBatch.get() != null) {
                CacheContinuousQueryEntry[] entries = GridTestUtils.getFieldValue(curBatch.get(), Class.forName(BATCH_CLASS_NAME), "entries");

                for (CacheContinuousQueryEntry entry : entries) {
                    if (entry != null)
                        notNullCnt++;
                }
            }
        }

        assertTrue(notNullCnt < ACK_THRESHOLD + BUFFER_SIZE);
    }
}
