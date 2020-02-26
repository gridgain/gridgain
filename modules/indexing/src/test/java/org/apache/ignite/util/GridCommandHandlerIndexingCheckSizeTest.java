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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.visor.verify.ValidateIndexesCheckSizeResult;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskResult;
import org.apache.ignite.util.GridCommandHandlerIndexingUtils.Organization;
import org.apache.ignite.util.GridCommandHandlerIndexingUtils.Person;
import org.junit.Test;

import static java.lang.String.valueOf;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CACHE_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.GROUP_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.breakCacheDataTree;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillCache;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.organizationEntity;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.personEntity;

public class GridCommandHandlerIndexingCheckSizeTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        createAndFillCache(client, CACHE_NAME, GROUP_NAME, qryEntities(), 100);
    }

    @Test
    public void test0() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", CACHE_NAME));

        assertContains(log, testOut.toString(), "no issues found");
    }

    @Test
    public void test1() {
        Map<String, AtomicInteger> rmvEntries = new HashMap<>();

        breakCacheDataTree(log, crd.cachex(CACHE_NAME), 1, (i, entry) -> {
            String typeName = ((BinaryObjectImpl)entry.getValue()).type().typeName();
            rmvEntries.computeIfAbsent(typeName, s -> new AtomicInteger()).incrementAndGet();
            return true;
        });

        assertEquals(rmvEntries.size(), 2);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", CACHE_NAME));

        String out = testOut.toString();
        assertContains(log, out, "issues found (listed above)");
        assertContains(log, out, "Size check");

        Map<String, ValidateIndexesCheckSizeResult> checkSizeRes =
            ((VisorValidateIndexesTaskResult)lastOperationResult).results().get(crd.localNode().id())
                .checkSizeResult();

        assertEquals(rmvEntries.size(), checkSizeRes.size());

        // TODO: 26.02.2020 add check
    }

    private Map<QueryEntity, Function<Random, Object>> qryEntities(){
        Map<QueryEntity, Function<Random, Object>> qryEntities = new HashMap<>();

        qryEntities.put(personEntity(), rand -> new Person(rand.nextInt(), valueOf(rand.nextLong())));
        qryEntities.put(organizationEntity(), rand -> new Organization(rand.nextInt(), valueOf(rand.nextLong())));

        return qryEntities;
    }
}
