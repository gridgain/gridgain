/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

import org.apache.ignite.internal.processors.query.h2.RowCountTableStatisticsSurvivesNodeRestartTest;
import org.apache.ignite.internal.processors.query.h2.RowCountTableStatisticsUsageTest;
import org.apache.ignite.internal.processors.query.stat.ColumnStatisticsCollectorAggregationTest;
import org.apache.ignite.internal.processors.query.stat.ColumnStatisticsCollectorTest;
import org.apache.ignite.internal.processors.query.stat.HasherSelfTest;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsRepositoryStaticTest;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsRepositoryTest;
import org.apache.ignite.internal.processors.query.stat.ManagerStatisticsTypesTest;
import org.apache.ignite.internal.processors.query.stat.PSUBasicValueDistributionTableStatisticsUsageTest;
import org.apache.ignite.internal.processors.query.stat.PSUCompositeIndexTableStatisticsUsageTest;
import org.apache.ignite.internal.processors.query.stat.PSUStatisticPartialGatheringTest;
import org.apache.ignite.internal.processors.query.stat.PSUStatisticsStorageTest;
import org.apache.ignite.internal.processors.query.stat.PSUStatisticsTypesTest;
import org.apache.ignite.internal.processors.query.stat.PSUValueDistributionTableStatisticsUsageTest;
import org.apache.ignite.internal.processors.query.stat.SqlStatisticsCommandTests;
import org.apache.ignite.internal.processors.query.stat.StatisticsClearTest;
import org.apache.ignite.internal.processors.query.stat.StatisticsConfigurationTest;
import org.apache.ignite.internal.processors.query.stat.StatisticsGatheringTest;
import org.apache.ignite.internal.processors.query.stat.StatisticsObsolescenceTest;
import org.apache.ignite.internal.processors.query.stat.StatisticsStorageInMemoryTest;
import org.apache.ignite.internal.processors.query.stat.StatisticsStoragePersistenceTest;
import org.apache.ignite.internal.processors.query.stat.StatisticsStorageRestartTest;
import org.apache.ignite.internal.processors.query.stat.StatisticsStorageUnitTest;
import org.apache.ignite.internal.sql.SqlParserAnalyzeSelfTest;
import org.apache.ignite.internal.sql.SqlParserDropStatisticsSelfTest;
import org.apache.ignite.internal.sql.SqlParserRefreshStatisticsSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Tests for statistics.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Table statistics collection.
    HasherSelfTest.class,
    ColumnStatisticsCollectorAggregationTest.class,
    ColumnStatisticsCollectorTest.class,
    ManagerStatisticsTypesTest.class,
    IgniteStatisticsRepositoryTest.class,
    IgniteStatisticsRepositoryStaticTest.class,
    StatisticsStorageRestartTest.class,
    StatisticsGatheringTest.class,
    StatisticsClearTest.class,

    // Table statistics usage.
    RowCountTableStatisticsUsageTest.class,
    RowCountTableStatisticsSurvivesNodeRestartTest.class,
    PSUStatisticsTypesTest.class,
    PSUStatisticPartialGatheringTest.class,
    PSUBasicValueDistributionTableStatisticsUsageTest.class,
    PSUValueDistributionTableStatisticsUsageTest.class,
    PSUCompositeIndexTableStatisticsUsageTest.class,
    PSUStatisticsStorageTest.class,

    // Statistics collection components tests
    StatisticsStorageInMemoryTest.class,
    StatisticsStoragePersistenceTest.class,
    StatisticsStorageUnitTest.class,

    // Statistics SQL commands
    SqlParserAnalyzeSelfTest.class,
    SqlParserRefreshStatisticsSelfTest.class,
    SqlParserDropStatisticsSelfTest.class,
    SqlStatisticsCommandTests.class,
    StatisticsConfigurationTest.class,

    // Obsolescence
    StatisticsObsolescenceTest.class,
})
public class IgniteStatisticsTestSuite {
}
