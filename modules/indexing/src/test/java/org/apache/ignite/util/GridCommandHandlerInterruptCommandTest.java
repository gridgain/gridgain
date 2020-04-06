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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.verify.ValidateIndexesClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;

/**
 * Checks cancel of execution validate_indexes command.
 */
public class GridCommandHandlerInterruptCommandTest extends GridCommandHandlerAbstractTest {
    /** Load loop cycles. */
    private static final int LOAD_LOOP = 500_000;

    /** Log listener. */
    private ListeningTestLogger lnsrLog;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(lnsrLog)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setInitialSize(200L * 1024 * 1024)
                    .setMaxSize(200L * 1024 * 1024)
                )
            )
            .setCacheConfiguration(new CacheConfiguration<Integer, UserValue>(DEFAULT_CACHE_NAME)
                .setName(DEFAULT_CACHE_NAME)
                .setQueryEntities(Collections.singleton(createQueryEntity())));
    }

    /**
     * Creates predifened query entity.
     *
     * @return Query entity.
     */
    private QueryEntity createQueryEntity() {
        QueryEntity qryEntity = new QueryEntity();
        qryEntity.setKeyType(Integer.class.getTypeName());
        qryEntity.setValueType(UserValue.class.getName());
        qryEntity.setTableName("USER_TEST_TABLE");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("x", "java.lang.Integer");
        fields.put("y", "java.lang.Integer");
        fields.put("z", "java.lang.Integer");
        qryEntity.setFields(fields);

        LinkedHashMap<String, Boolean> idxFields = new LinkedHashMap<>();

        QueryIndex idx2 = new QueryIndex();
        idx2.setName("IDX_2");
        idx2.setIndexType(QueryIndexType.SORTED);
        idxFields = new LinkedHashMap<>();
        idxFields.put("x", false);
        idx2.setFields(idxFields);

        QueryIndex idx3 = new QueryIndex();
        idx3.setName("IDX_3");
        idx3.setIndexType(QueryIndexType.SORTED);
        idxFields = new LinkedHashMap<>();
        idxFields.put("y", false);
        idx3.setFields(idxFields);

        QueryIndex idx4 = new QueryIndex();
        idx4.setName("IDX_4");
        idx4.setIndexType(QueryIndexType.SORTED);
        idxFields = new LinkedHashMap<>();
        idxFields.put("z", false);
        idx4.setFields(idxFields);

        qryEntity.setIndexes(Arrays.asList(idx2, idx3, idx4));
        return qryEntity;
    }

    /**
     * User value.
     */
    private static class UserValue {
        /** X. */
        private int x;

        /** Y. */
        private int y;

        /** Z. */
        private int z;

        /**
         * @param x X.
         * @param y Y.
         * @param z Z.
         */
        public UserValue(int x, int y, int z) {
            this.x = x;
            this.y = y;
            this.z = z;
        }

        /**
         * @param seed Seed.
         */
        public UserValue(long seed) {
            x = (int)(seed % 6991);
            y = (int)(seed % 18679);
            z = (int)(seed % 31721);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(UserValue.class, this);
        }
    }

    /**
     * Checks that validate_indexes command will cancel after it interrupted.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testValidateIndexesCommand() throws Exception {
        lnsrLog = new ListeningTestLogger(false, log);

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        preloadeData(ignite);

        LogListener lnsrValidationStarted = LogListener.matches("Current progress of ValidateIndexesClosure").build();
        LogListener lnsrValidationCancelled = LogListener.matches("Index validation was cancelled.").build();

        lnsrLog.registerListener(lnsrValidationStarted);
        lnsrLog.registerListener(lnsrValidationCancelled);

        IgniteInternalFuture fut = GridTestUtils.runAsync(() ->
            assertSame(EXIT_CODE_UNEXPECTED_ERROR, execute("--cache", "validate_indexes")));

        assertTrue(GridTestUtils.waitForCondition(lnsrValidationStarted::check, 10_000));

        fut.cancel();

        fut.get();

        assertTrue(GridTestUtils.waitForCondition(lnsrValidationCancelled::check, 10_000));
    }

    /**
     * Preload data to default cache.
     *
     * @param ignite Ignite.
     */
    private void preloadeData(IgniteEx ignite) {
        try (IgniteDataStreamer streamr = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < LOAD_LOOP; i++)
                streamr.addData(i, new UserValue(i));
        }
    }

    /**
     * Test invokes index validation closure and canceling it after started.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancelValidateIndexesClosure() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        preloadeData(ignite0);

        AtomicBoolean cancelled = new AtomicBoolean(false);

        ValidateIndexesClosure clo = new ValidateIndexesClosure(cancelled::get, Collections.singleton(DEFAULT_CACHE_NAME),
            0, 0, false, true);

        ListeningTestLogger listeningLogger = new ListeningTestLogger(false, log);

        GridTestUtils.setFieldValue(clo, "ignite", ignite0);
        GridTestUtils.setFieldValue(clo, "log", listeningLogger);

        LogListener lnsrValidationStarted = LogListener.matches("Current progress of ValidateIndexesClosure").build();

        listeningLogger.registerListener(lnsrValidationStarted);

        IgniteInternalFuture fut = GridTestUtils.runAsync(() ->
            GridTestUtils.assertThrows(log, clo::call, IgniteException.class, ValidateIndexesClosure.CANCELLED_MSG));

        assertTrue(GridTestUtils.waitForCondition(lnsrValidationStarted::check, 10_000));

        assertFalse(fut.isDone());

        cancelled.set(true);

        fut.get(10_000);
    }
}
