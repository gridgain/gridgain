/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class GridMapQueryExecutorTest {

    /**
     * Helper method for testing log output based on sensitivity config and parameters.
     *
     * @param sensitivity      The system property value for IGNITE_SENSITIVE_DATA_LOGGING.
     * @param params           The parameters passed to the log method.
     * @param expectedParams   Expected formatted output in the log.
     */
    private static void testBuildQueryLogDetailsWithSampleParams(String sensitivity, Object[] params, String expectedParams) {
        System.setProperty(IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING, sensitivity);

        Collection<GridCacheSqlQuery> queries = Collections.singletonList(
                new GridCacheSqlQuery("SELECT * FROM test")
        );

        String log = GridMapQueryExecutor.buildQueryLogDetails(
                123L,
                "TestLabel",
                "TestSchema",
                queries,
                params,
                new RuntimeException("Test exception"),
                UUID.randomUUID(),
                UUID.randomUUID()
        );

        assertTrue("Log does not contain expected param string", log.contains("params=" + expectedParams));
    }

    @Test
    public void testBuildQueryLogDetailsWithNullParams() {
        Collection<GridCacheSqlQuery> queries = Collections.singletonList(
                new GridCacheSqlQuery("SELECT * FROM test")
        );

        String log = GridMapQueryExecutor.buildQueryLogDetails(
                123L,
                "TestLabel",
                "TestSchema",
                queries,
                null,
                new RuntimeException("Test exception"),
                UUID.randomUUID(),
                UUID.randomUUID()
        );

        assertTrue(log.contains("params=N/A"));
    }

    @Test
    public void testBuildQueryLogDetailsWithEmptyParams() {
        Collection<GridCacheSqlQuery> queries = Collections.singletonList(
                new GridCacheSqlQuery("SELECT * FROM test")
        );

        String log = GridMapQueryExecutor.buildQueryLogDetails(
                123L,
                "TestLabel",
                "TestSchema",
                queries,
                new Object[]{},
                new RuntimeException("Test exception"),
                UUID.randomUUID(),
                UUID.randomUUID()
        );

        assertTrue(log.contains("params=N/A"));
    }

    @Test
    public void testBuildQueryLogDetailsHandlesNullFields() {
        String log = GridMapQueryExecutor.buildQueryLogDetails(
                999L,
                null,
                null,
                null,
                null,
                null,
                UUID.randomUUID(),
                UUID.randomUUID()
        );

        assertTrue(log.contains("label=N/A"));
        assertTrue(log.contains("schema=N/A"));
        assertTrue(log.contains("queries=N/A"));
        assertTrue(log.contains("params=N/A"));
    }

    @Test
    public void testBuildQueryLogDetailsWithPlainSensitivity() {
        Object[] givenParams = new Object[]{42, "john", 345};

        testBuildQueryLogDetailsWithSampleParams("plain", givenParams, "[42, john, 345]");
    }

    @Test
    public void testBuildQueryLogDetailsWithHashSensitivity() {
        Object[] givenParams = new Object[]{42, "john", 345};
        int hash = Arrays.hashCode(givenParams);
        String expected = String.valueOf(IgniteUtils.hash(hash));

        testBuildQueryLogDetailsWithSampleParams("hash", givenParams, expected);
    }

    @Test
    public void testBuildQueryLogDetailsWithNoneSensitivity() {
        Object[] givenParams = new Object[]{42, "john", 345};

        testBuildQueryLogDetailsWithSampleParams("none", givenParams, "HIDDEN");
    }

}
