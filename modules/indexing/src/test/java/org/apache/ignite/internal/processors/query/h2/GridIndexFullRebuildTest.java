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

package org.apache.ignite.internal.processors.query.h2;

import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.ComputeTaskInternalFuture;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTask;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskArg;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskResult;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
@RunWith(JUnit4.class)
public class GridIndexFullRebuildTest extends GridCommonAbstractTest {
    public static final String FIRST_CACHE = "cache1";
    public static final String SECOND_CACHE = "cache2";
    public static final String THIRD_CACHE = "cache3";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);

        configuration.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(1024 * 1024 * 1024)
            .setPersistenceEnabled(true)
        );

        dsCfg.setCheckpointFrequency(300_000);

        configuration.setDataStorageConfiguration(dsCfg);

        CacheConfiguration ccfgFirst = new CacheConfiguration();

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("updateDate", "java.lang.Date");
        fields.put("amount", "java.lang.Long");
        fields.put("name", "java.lang.String");

        Set<QueryIndex> indices = Collections.singleton(new QueryIndex("name", QueryIndexType.SORTED));

        ccfgFirst.setName(FIRST_CACHE)
            .setBackups(2)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setGroupName("group")
            .setCacheMode(CacheMode.PARTITIONED)
            .setQueryEntities(Collections.singletonList(
                new QueryEntity(Long.class, Account.class)
                    .setFields(fields)
                    .setIndexes(indices)
            ));

        LinkedHashMap<String, String> fields2 = new LinkedHashMap<>();


        fields2.put("id", "java.lang.Long");
        fields2.put("updateDate", "java.lang.Date");
        fields2.put("amount", "java.lang.Long");
        fields2.put("name", "java.lang.String");
        fields2.put("type", "java.lang.Integer");

        Set<QueryIndex> indices2 = new HashSet<>();

        indices2.add(new QueryIndex("amount", QueryIndexType.SORTED));
        indices2.add(new QueryIndex("type", QueryIndexType.SORTED));
        indices2.add(new QueryIndex("updateDate", QueryIndexType.SORTED));

        CacheConfiguration ccfgSecond = new CacheConfiguration()
            .setName(SECOND_CACHE)
            .setGroupName("cache_group_4")
            .setCacheMode(CacheMode.REPLICATED)
            .setBackups(2)
            .setQueryEntities(Collections.singleton(
                new QueryEntity(Long.class, Account.class)
                .setFields(fields2)
                .setIndexes(indices2)
            ))
            ;

        configuration.setCacheConfiguration(ccfgFirst, ccfgSecond, new CacheConfiguration(ccfgFirst).setName(THIRD_CACHE));

        return configuration;
    }

    /**
     * We start several nodes, populate caches, then start replacing values.
     * After that one node is killed, their index.bin files would be removed.
     * Finally, we restart the node, index rebuild starting after recovery.
     * And we checke indexes by "validate indexes" task.
     */
    @Test
    public void test() throws Exception {
        long start = System.currentTimeMillis();

        IgniteEx grid1 = startGrids(4);

        grid1.cluster().active(true);

        final int accountCount = 4096;

        try (IgniteDataStreamer streamer = grid1.dataStreamer(FIRST_CACHE)) {
            for (long i = 0; i < accountCount; i++) {
                streamer.addData(i, new Account(i));
            }

            streamer.flush();
        }

        try (IgniteDataStreamer streamer = grid1.dataStreamer(SECOND_CACHE)) {
            for (long i = 0; i < accountCount; i++) {
                streamer.addData(i, new Account(i));
            }

            streamer.flush();
        }

        try (IgniteDataStreamer streamer = grid1.dataStreamer(THIRD_CACHE)) {
            for (long i = 0; i < accountCount; i++) {
                streamer.addData(i, new Account(i));
            }

            streamer.flush();
        }

        for (int i = 0; i < 3; i++) {
            AtomicBoolean stop = new AtomicBoolean();

            IgniteCache<Object, Object> cache1 = grid1.cache(FIRST_CACHE);
            IgniteCache<Object, Object> cache2 = grid1.cache(SECOND_CACHE);
            IgniteCache<Object, Object> cache3 = grid1.cache(THIRD_CACHE);

            new Thread(new Runnable() {
                @Override public void run() {
                    ThreadLocalRandom current = ThreadLocalRandom.current();
                    while (!stop.get()) {
                        try {
                            long i = current.nextInt(0, accountCount * 2);
                            cache1.put(i, new Account(i));

                            i = current.nextInt(0, accountCount * 2);
                            cache2.put(i, new Account(i));

                            i = current.nextInt(0, accountCount * 2);
                            cache3.put(i, new Account(i));
                        }
                        catch (Throwable e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();

            long diff = System.currentTimeMillis() - start;

            U.sleep(7500 - (diff % 5000));

            IgniteProcessProxy.kill(getTestIgniteInstanceName(3));

            stop.set(true);

            File workDirectory = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

            for (File grp : new File(workDirectory, U.maskForFileName(getTestIgniteInstanceName(3))).listFiles()) {
                new File(grp, "index.bin").delete();
            }

            startGrid(3);

            awaitPartitionMapExchange();

            U.sleep(3_000);

            ImmutableSet<UUID> nodes = ImmutableSet.of(((IgniteProcessProxy)grid(3)).getId(),
                ((IgniteProcessProxy)grid(2)).getId());

            VisorValidateIndexesTaskArg arg = new VisorValidateIndexesTaskArg(null,
                null, 10000, 1);

            VisorTaskArgument<VisorValidateIndexesTaskArg> argument = new VisorTaskArgument<>(nodes, arg, true);

            ComputeTaskInternalFuture<VisorValidateIndexesTaskResult> execute = grid1.context().task().execute(new VisorValidateIndexesTask(), argument);

            VisorValidateIndexesTaskResult result = execute.get();

            Map<UUID, VisorValidateIndexesJobResult> results = result.results();

            boolean hasIssue = false;

            for (VisorValidateIndexesJobResult jobResult : results.values()) {
                System.err.println(jobResult);

                hasIssue |= jobResult.hasIssues();
            }

            assertFalse(hasIssue);
        }
    }

    /** */
    private void cleanPersistenceFiles(String igName) throws Exception {
        String ig1DbPath = Paths.get(DFLT_STORE_DIR, igName).toString();

        File igDbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), ig1DbPath, false);

        U.delete(igDbDir);

        Files.createDirectory(igDbDir.toPath());

        String ig1DbWalPath = Paths.get(DFLT_STORE_DIR, "wal", igName).toString();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), ig1DbWalPath, false));

        ig1DbWalPath = Paths.get(DFLT_STORE_DIR, "wal", "archive", igName).toString();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), ig1DbWalPath, false));
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** */
    public class Account {
        /** */
        private Long id;
        /** */
        private String name;
        /** */
        private Long amount;
        /** */
        private Date updateDate;

        private int type;

        /** */
        public Account(Long id) {
            this.id = id;

            name = "Account" + id;
            amount = id * 1000;
            updateDate = new Date();
            type = (int)(id % 10);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Account account = (Account)o;
            return Objects.equals(id, account.id);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id);
        }
    }
}

//<!--TX REPLICATED-->
//<bean class="org.apache.ignite.configuration.CacheConfiguration">
//<property name="name"value="cache_group_4_091"/>
//<property name="groupName"value="cache_group_4"/>
//<property name="backups"value="2"/>
//<property name="atomicityMode"value="TRANSACTIONAL"/>
//<property name="cacheMode"value="REPLICATED"/>
//<property name="writeSynchronizationMode"value="FULL_SYNC"/>
//<property name="queryEntities"ref="entities_Long_Account"/>
//</bean>
//<bean class="org.apache.ignite.configuration.CacheConfiguration">
//<property name="name"value="cache_group_4_098"/>
//<property name="groupName"value="cache_group_5"/>
//<property name="atomicityMode"value="ATOMIC"/>
//<property name="backups"value="2"/>
//<property name="cacheMode"value="REPLICATED"/>
//<property name="writeSynchronizationMode"value="FULL_SYNC"/>
//<property name="onheapCacheEnabled"value="true"/>
//<property name="affinity">
//<bean class="org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction">
//<constructor-arg value="false"/><constructor-arg value="128"/>
//</bean>
//</property>
//<property name="queryEntities"ref="entities_Long_Account"/>
//</bean>
//<bean class="org.apache.ignite.configuration.CacheConfiguration">
//<property name="name"value="cache_group_4_105"/>
//<property name="groupName"value="cache_group_6"/>
//<property name="backups"value="2"/>
//<property name="atomicityMode"value="TRANSACTIONAL"/>
//<property name="cacheMode"value="REPLICATED"/>
//<property name="writeSynchronizationMode"value="FULL_SYNC"/>
//<property name="evictionPolicy">
//<bean class="org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy">
//<property name="maxSize"value="1000"/>
//</bean>
//</property>
//<property name="onheapCacheEnabled"value="true"/>
//<property name="affinity">
//<bean class="org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction">
//<constructor-arg value="false"/>
//<constructor-arg value="64"/>
//</bean>
//</property>
//<property name="queryEntities"ref="entities_Long_Account"/>
//</bean>
//<bean class="org.apache.ignite.configuration.CacheConfiguration">
//<property name="name"value="cache_group_4_118"/>
//<property name="atomicityMode"value="TRANSACTIONAL"/>
//<property name="cacheMode"value="REPLICATED"/>
//<property name="backups"value="2"/>
//<property name="writeSynchronizationMode"value="FULL_SYNC"/>
//<property name="evictionPolicy">
//<bean class="org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy">
//<property name="maxSize"value="1000"/>
//</bean>
//</property>
//<property name="onheapCacheEnabled"value="true"/>
//<property name="affinity">
//<bean class="org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction">
//<constructor-arg value="false"/>
//<constructor-arg value="32"/>
//</bean>
//</property>
//<property name="queryEntities"ref="entities_Long_Account"/>
//</bean>