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

package org.apache.ignite.internal.processors.query;

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

/**
 * Check consistency between cache data and SQL indexes on remove cache entry (prevent index tree corruption).
 */
public class CheckIndexConsistencyOnRemoveTest extends AbstractIndexingCommonTest {
    /** Test logger. */
    private static ListeningTestLogger srvLog;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(srvLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srvLog = new ListeningTestLogger(log);

        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (String cacheName : grid(0).cacheNames())
            grid(0).cache(cacheName).destroy();
    }

    /**
     * Make data inconsistency with two PK which are equal logically and differ in binary representation.
     * Remove keys from cache, checks:
     * - warning message is printed;
     * - secondary tree index isn't corrupted.
     */
    @Test
    @WithSystemProperty(key = "IGNITE_SENSITIVE_DATA_LOGGING", value = "none")
    public void checkWarningMessage() {
        sql("CREATE TABLE TEST (ID0 INTEGER, ID1 INTEGER, VAL VARCHAR(100), PRIMARY KEY(ID0, ID1))" +
            "WITH \"KEY_TYPE=TEST_KEY,CACHE_NAME=test,VALUE_TYPE=TEST_VAL\"");

        sql("CREATE INDEX IDX_VAL ON TEST (VAL)");

        IgniteCache cache = grid(0).cache("test");
        BinaryObject key0, key1;

        {
            BinaryObjectBuilder bobKey = grid(0).binary().builder("TEST_KEY");
            bobKey.setField("ID0", 0);
            bobKey.setField("ID1", 0);

            BinaryObjectBuilder bobVal = grid(0).binary().builder("TEST_VAL");
            bobVal.setField("VAL", "val0");

            key0 = bobKey.build();
            cache.put(key0, bobVal.build());
        }

        {
            BinaryObjectBuilder bobKey = grid(0).binary().builder("TEST_KEY");
            bobKey.setField("ID1", 0);
            bobKey.setField("ID0", 0);

            BinaryObjectBuilder bobVal = grid(0).binary().builder("TEST_VAL");
            bobVal.setField("VAL", "val1");

            key1 = bobKey.build();
            cache.put(key1, bobVal.build());
        }

        // Consistency is broken
        assertEquals(2, cache.size());
        assertEquals(1, sql("SELECT * from TEST USE INDEX (\"_key_PK\")").getAll().size());

        cache.remove(key1);

        LogListener lsnr1 = LogListener
            .matches("SQL index inconsistency detected")
            .andMatches("secIdxName=IDX_VAL")
            .build();
        LogListener lsnr2 = LogListener
            .matches("hidden data")
            // Key, value, 3 column's values, key hex and value hex.
            .times(7)
            .build();
        srvLog.registerListener(lsnr1);
        srvLog.registerListener(lsnr2);

        cache.remove(key0);

        assertTrue(lsnr1.check());
        assertTrue(lsnr2.check());

        // Check that IDX_VAL tree is not corrupted.
        assertEquals(0, sql("SELECT * from TEST USE INDEX (IDX_VAL)").getAll().size());
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setArgs(args), false);
    }
}
