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
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

public class QueryVerboseLoggingTest extends JUnitAssertAware {

    private static final UUID LOCAL_NODE_ID = UUID.randomUUID();

    private static final String LABEL_PREFIX = "lbl";

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.setProperty(QueryVerboseLogging.Settings.QVL_TARGET_QUERY_MIN_DURATION_PROP, String.valueOf(0));
    }

    @Test
    public void assertClearsRedundantQueries() throws IgniteCheckedException {
        final int MAX_SIZE = 16;

        System.setProperty(QueryVerboseLogging.Settings.QVL_ENABLED_PROP, "true");
        System.setProperty(QueryVerboseLogging.Settings.QVL_TARGET_QUERY_LABEL_PROP, LABEL_PREFIX + ".*");
        System.setProperty(QueryVerboseLogging.Settings.QVL_FORCE_FINISH_MAX_SIZE_PROP, String.valueOf(MAX_SIZE));

        ConcurrentMap<String, QueryVerboseLogging.TargetQueryDescriptor> queries = U.field(QueryVerboseLogging.class, "queries");

        QVL.refreshCfg();

        for (int i = 0; i < MAX_SIZE * 2; i++) {
            push(i);
        }

        assertEquals(MAX_SIZE, queries.size());
    }

    @Test
    public void assertClearsTooOldQueriesFirst() throws IgniteCheckedException {
        final int MAX_SIZE = 16;
        final int MAX_TTL = 10_000;

        System.setProperty(QueryVerboseLogging.Settings.QVL_ENABLED_PROP, "true");
        System.setProperty(QueryVerboseLogging.Settings.QVL_TARGET_QUERY_LABEL_PROP, LABEL_PREFIX + ".*");
        System.setProperty(QueryVerboseLogging.Settings.QVL_FORCE_FINISH_MAX_SIZE_PROP, String.valueOf(MAX_SIZE));
        System.setProperty(QueryVerboseLogging.Settings.QVL_FORCE_FINISH_TTL_PROP, String.valueOf(MAX_TTL));

        ConcurrentMap<String, QueryVerboseLogging.TargetQueryDescriptor> queries = U.field(QueryVerboseLogging.class, "queries");

        QVL.refreshCfg();

        for (int i = 0; i < MAX_SIZE; i++) {
            push(i);
        }

        U.sleep(MAX_TTL + 100);

        final int ADDITION = 7;

        for (int i = 0; i < ADDITION; i++) {
            push(MAX_TTL + i);
        }

        assertEquals(ADDITION, queries.size());
    }

    private static void push(int id) {
        String label = LABEL_PREFIX + "-" + id;
        String qryString = "SELECT * FROM foo";
        String globalQryId = QueryUtils.globalQueryId(LOCAL_NODE_ID, id);
        GridCacheQueryType qryType = null;
        boolean lazy = true;
        boolean distrJoins = true;
        boolean enforceJoinOrder = true;

        QVL.register(label, qryString, globalQryId, qryType, lazy, distrJoins, enforceJoinOrder);
    }
}
