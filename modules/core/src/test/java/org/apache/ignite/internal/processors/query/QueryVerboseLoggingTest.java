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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.JUnitAssertAware;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class QueryVerboseLoggingTest extends JUnitAssertAware {

    private static final UUID LOCAL_NODE_ID = UUID.randomUUID();

    private static final String LABEL_PREFIX = "lbl";

    private final AtomicLong ID_GEN = new AtomicLong(0);

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.setProperty(QueryVerboseLogging.Settings.QVL_TARGET_QUERY_MIN_DURATION_PROP, String.valueOf(0));
    }

    @Before
    public void setUp() throws Exception {
        ID_GEN.set(0);
    }

    @Test
    public void assertClearsRedundantQueries() throws IgniteCheckedException {
        final int MAX_SIZE = 16;

        System.setProperty(QueryVerboseLogging.Settings.QVL_ENABLED_PROP, "true");
        System.setProperty(QueryVerboseLogging.Settings.QVL_TARGET_QUERY_LABEL_PROP, LABEL_PREFIX + ".*");
        System.setProperty(QueryVerboseLogging.Settings.QVL_FORCE_FINISH_MAX_SIZE_PROP, String.valueOf(MAX_SIZE));

        ConcurrentMap<String, QueryVerboseLogging.TargetQueryDescriptor> queries = U.staticField(QueryVerboseLogging.class, "queries");

        QVL.refreshCfg();

        for (int i = 0; i < MAX_SIZE * 2; i++) {
            push();
        }

        assertEquals(MAX_SIZE, queries.size());
    }

    @Test
    public void assertClearsTooOldQueriesFirst() throws IgniteCheckedException {
        final int MAX_SIZE = 15;
        final int MAX_TTL_SECONDS = 5;
        final int MIN_DURATION = 20_000;

        System.setProperty(QueryVerboseLogging.Settings.QVL_ENABLED_PROP, "true");
        System.setProperty(QueryVerboseLogging.Settings.QVL_TARGET_QUERY_LABEL_PROP, LABEL_PREFIX + ".*");
        System.setProperty(QueryVerboseLogging.Settings.QVL_FORCE_FINISH_MAX_SIZE_PROP, String.valueOf(MAX_SIZE));
        System.setProperty(QueryVerboseLogging.Settings.QVL_FORCE_FINISH_TTL_PROP, String.valueOf(MAX_TTL_SECONDS * 1000));
        System.setProperty(QueryVerboseLogging.Settings.QVL_TARGET_QUERY_MIN_DURATION_PROP, String.valueOf(MIN_DURATION));

        ConcurrentMap<String, QueryVerboseLogging.TargetQueryDescriptor> queries = U.field(QueryVerboseLogging.class, "queries");

        QVL.refreshCfg();

        final int INITIAL = 10;
        for (int i = 0; i < INITIAL; i++)
            push();

        U.sleep(15_000);

        for (int i = 0; i < MAX_SIZE - INITIAL; i++) {
            push();
        }

        for (int s = 0; s < MAX_TTL_SECONDS; s++) {
            for (int i = 0; i < INITIAL / 2; i++)
                log(i, s);

            U.sleep(1000);
        }

        final int ADDITION = 7;
        for (int i = 0; i < ADDITION; i++) {
            push();
        }

        final int EXPECTED_SURVIVED = 12;

        // 0-4: Preserved (updated in the last MAX_TTL_SECONDS=5s)
        // 5-9: Cleaned up (not updated in the last MAX_TTL_SECONDS=5s) and logged (duration > MIN_DURATION=20s)
        // 10-14: Cleaned up (not updated in the last MAX_TTL_SECONDS=5s) and NOT logged (duration < MIN_DURATION=20s)
        // 15-21: Preserved (previous cleanup released enough space)
        assertEquals(EXPECTED_SURVIVED, queries.size());

        for (int i = EXPECTED_SURVIVED; i < EXPECTED_SURVIVED + ADDITION; i++) {
            push();
        }

        // push(22: size=12 newSize=12)
        // ...
        // push(26: size=16 > MAX_SIZE -> cleanup newSize=9)
        // push(27: size=10)
        // push(28: size=11)
        assertEquals(11, queries.size());
    }

    private void push() {
        long id = ID_GEN.getAndIncrement();
        String label = LABEL_PREFIX + "-" + id;
        String qryString = "SELECT * FROM foo";
        String globalQryId = QueryUtils.globalQueryId(LOCAL_NODE_ID, id);
        GridCacheQueryType qryType = null;
        boolean lazy = true;
        boolean distrJoins = true;
        boolean enforceJoinOrder = true;

        QVL.register(label, qryString, globalQryId, qryType, lazy, distrJoins, enforceJoinOrder);
    }

    private void log(int id, int spanId) {
        QVL.logSpan(LOCAL_NODE_ID, (long) id, () -> "span-" + spanId);
    }
}
