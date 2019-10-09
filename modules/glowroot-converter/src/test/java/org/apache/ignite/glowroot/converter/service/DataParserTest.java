/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.glowroot.converter.service;

import org.apache.ignite.glowroot.converter.model.CacheTraceItem;
import org.apache.ignite.glowroot.converter.model.GlowrootTransactionMeta;
import org.apache.ignite.glowroot.converter.model.TraceItem;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;

public class DataParserTest {

    @Test
    public void testParseCacheApiData() {
        TraceItem tti = DataParser.parse(
            new GlowrootTransactionMeta(UUID.randomUUID().toString(), 0, 0),
            0,
            0,
            "trace_type=cache_ops cache_name=CacheQueryExampleOrganizations op=put args=Long,Organization");

        assertTrue(tti instanceof CacheTraceItem);

        CacheTraceItem tcti = (CacheTraceItem)tti;

        assertEquals("CacheQueryExampleOrganizations", tcti.cacheName());
        assertEquals("put", tcti.operation());
        assertEquals("Long,Organization", tcti.args());
    }
}