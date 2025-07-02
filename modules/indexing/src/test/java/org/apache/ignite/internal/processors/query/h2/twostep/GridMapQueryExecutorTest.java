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

    public void testBuildQueryLogDetailsWithSampleParams(String sensitivity, String expectedParams) {
        System.setProperty(IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING, sensitivity);

        Collection<GridCacheSqlQuery> queries = Collections.singletonList(
                new GridCacheSqlQuery("SELECT * FROM test")
        );

        String log = GridMapQueryExecutor.buildQueryLogDetails(
                123L,
                "TestLabel",
                "TestSchema",
                queries,
                new Object[]{42, "john", 345},
                new RuntimeException("Test exception"),
                UUID.randomUUID(),
                UUID.randomUUID()
        );

        assertTrue(log.contains("params=" + expectedParams));
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
        testBuildQueryLogDetailsWithSampleParams("plain", "[42, john, 345]");
    }

    @Test
    public void testBuildQueryLogDetailsWithHashSensitivity() {
        int hash = Arrays.hashCode(new Object[]{42, "john", 345});
        testBuildQueryLogDetailsWithSampleParams("hash", String.valueOf(IgniteUtils.hash(hash)));
    }

    @Test
    public void testBuildQueryLogDetailsWithNoneSensitivity() {
        testBuildQueryLogDetailsWithSampleParams("none", "HIDDEN");
    }

}
