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

package org.apache.ignite.cache.query;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Suite with tests for {@link IndexQuery}.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
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
    IndexQueryInCriterionDescTest.class
})
public class IndexQueryTestSuite {
}
