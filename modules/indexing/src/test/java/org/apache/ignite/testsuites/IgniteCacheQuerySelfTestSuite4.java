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

package org.apache.ignite.testsuites;

import org.apache.ignite.cache.query.IndexQueryAliasTest;
import org.apache.ignite.cache.query.IndexQueryAllTypesTest;
import org.apache.ignite.cache.query.IndexQueryCacheKeyValueEscapedFieldsTest;
import org.apache.ignite.cache.query.IndexQueryCacheKeyValueFieldsTest;
import org.apache.ignite.cache.query.IndexQueryFailoverTest;
import org.apache.ignite.cache.query.IndexQueryFilterTest;
import org.apache.ignite.cache.query.IndexQueryInCriterionDescTest;
import org.apache.ignite.cache.query.IndexQueryInCriterionTest;
import org.apache.ignite.cache.query.IndexQueryKeepBinaryTest;
import org.apache.ignite.cache.query.IndexQueryLimitTest;
import org.apache.ignite.cache.query.IndexQueryLocalTest;
import org.apache.ignite.cache.query.IndexQueryPartitionTest;
import org.apache.ignite.cache.query.IndexQueryQueryEntityTest;
import org.apache.ignite.cache.query.IndexQueryRangeTest;
import org.apache.ignite.cache.query.IndexQuerySqlIndexTest;
import org.apache.ignite.cache.query.IndexQueryWrongIndexTest;
import org.apache.ignite.cache.query.MultiTableIndexQuery;
import org.apache.ignite.cache.query.MultifieldIndexQueryTest;
import org.apache.ignite.cache.query.RepeatedFieldIndexQueryTest;
import org.apache.ignite.cache.query.ThinClientIndexQueryTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryAsyncFailoverAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryAsyncFailoverTxReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryAsyncFailoverTxSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverAtomicNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverAtomicReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverTxReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverTxSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Continuous queries failover tests.
    CacheContinuousQueryFailoverAtomicSelfTest.class,
    CacheContinuousQueryFailoverAtomicNearEnabledSelfTest.class,
    CacheContinuousQueryFailoverAtomicReplicatedSelfTest.class,
    CacheContinuousQueryFailoverTxSelfTest.class,
    CacheContinuousQueryFailoverTxReplicatedSelfTest.class,

    CacheContinuousQueryAsyncFailoverAtomicSelfTest.class,
    CacheContinuousQueryAsyncFailoverTxReplicatedSelfTest.class,
    CacheContinuousQueryAsyncFailoverTxSelfTest.class,

    // Index queries.
    // TODO Remove after https://ggsystems.atlassian.net/browse/GG-38392
    IndexQueryAllTypesTest.class,
    IndexQueryFailoverTest.class,
    IndexQueryFilterTest.class,
    IndexQueryKeepBinaryTest.class,
    IndexQueryLocalTest.class,
    IndexQueryQueryEntityTest.class,
    IndexQueryAliasTest.class,
    IndexQuerySqlIndexTest.class,
    IndexQueryRangeTest.class,
    IndexQueryPartitionTest.class,
    IndexQueryCacheKeyValueFieldsTest.class,
    IndexQueryCacheKeyValueEscapedFieldsTest.class,
    IndexQueryWrongIndexTest.class,
    MultifieldIndexQueryTest.class,
    MultiTableIndexQuery.class,
    RepeatedFieldIndexQueryTest.class,
    ThinClientIndexQueryTest.class,
    RepeatedFieldIndexQueryTest.class,
    IndexQueryInCriterionTest.class,
    IndexQueryInCriterionDescTest.class,
    IndexQueryLimitTest.class,
})
public class IgniteCacheQuerySelfTestSuite4 {
}
