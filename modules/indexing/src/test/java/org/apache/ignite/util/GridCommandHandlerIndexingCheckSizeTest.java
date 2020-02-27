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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.cache.CacheSubcommands;
import org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.visor.verify.ValidateIndexesCheckSizeIssue;
import org.apache.ignite.internal.visor.verify.ValidateIndexesCheckSizeResult;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskResult;
import org.apache.ignite.util.GridCommandHandlerIndexingUtils.Organization;
import org.apache.ignite.util.GridCommandHandlerIndexingUtils.Person;
import org.junit.Test;

import static java.lang.String.valueOf;
import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandList.CACHE;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.VALIDATE_INDEXES;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.NO_CHECK_SIZES;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CACHE_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.GROUP_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.breakCacheDataTree;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.breakSqlIndex;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillCache;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.organizationEntity;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.personEntity;

/**
 * Class for testing function of checking size of index and cache in
 * {@link CacheSubcommands#VALIDATE_INDEXES}.
 */
public class GridCommandHandlerIndexingCheckSizeTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** Entry count for entity. */
    private static final int ENTRY_CNT = 100;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        Map<QueryEntity, Function<Random, Object>> qryEntities = new HashMap<>();

        qryEntities.put(personEntity(), rand -> new Person(rand.nextInt(), valueOf(rand.nextLong())));
        qryEntities.put(organizationEntity(), rand -> new Organization(rand.nextInt(), valueOf(rand.nextLong())));

        createAndFillCache(client, CACHE_NAME, GROUP_NAME, qryEntities, ENTRY_CNT);
    }

    /**
     * Test checks that an error will be displayed checking cache size and
     * index when cache is broken.
     */
    @Test
    public void testCheckCacheSizeWhenBrokenCache() {
        Map<String, AtomicInteger> rmvEntryByTbl = new HashMap<>();

        String cacheName = CACHE_NAME;
        IgniteEx node = crd;

        breakCacheDataTree(log, node.cachex(cacheName), 1, (i, entry) -> {
            rmvEntryByTbl.computeIfAbsent(tableName(cacheName, entry), s -> new AtomicInteger()).incrementAndGet();
            return true;
        });

        assertEquals(rmvEntryByTbl.size(), 2);
        validateCheckSizes(node, cacheName, rmvEntryByTbl, ai -> ENTRY_CNT - ai.get(), ai -> ENTRY_CNT);
    }

    /**
     * Test checks that cache size and index validation error will not be
     * displayed if cache is broken, because argument
     * {@link ValidateIndexesCommandArg#NO_CHECK_SIZES} be used.
     */
    @Test
    public void testNoCheckCacheSizeWhenBrokenCache() {
        String cacheName = CACHE_NAME;

        breakCacheDataTree(log, crd.cachex(cacheName), 1, null);

        checkNoCheckSizeInCaseBrokenData(cacheName);
    }

    /**
     * Test checks that an error will be displayed checking cache size and
     * index when index is broken.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCheckCacheSizeWhenBrokenIdx() throws Exception {
        Map<String, AtomicInteger> rmvIdxByTbl = new HashMap<>();

        String cacheName = CACHE_NAME;

        breakSqlIndex(crd.cachex(cacheName), 0, row -> {
            rmvIdxByTbl.computeIfAbsent(tableName(cacheName, row), s -> new AtomicInteger()).incrementAndGet();
            return true;
        });

        assertEquals(rmvIdxByTbl.size(), 2);
        validateCheckSizes(crd, cacheName, rmvIdxByTbl, ai -> ENTRY_CNT, ai -> ENTRY_CNT - ai.get());
    }

    /**
     * Test checks that cache size and index validation error will not be
     * displayed if index is broken, because argument
     * {@link ValidateIndexesCommandArg#NO_CHECK_SIZES} be used.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNoCheckCacheSizeWhenBrokenIdx() throws Exception {
        String cacheName = CACHE_NAME;

        breakSqlIndex(crd.cachex(cacheName), 1, null);

        checkNoCheckSizeInCaseBrokenData(cacheName);
    }

    /**
     * Check that if data is broken and option
     * {@link ValidateIndexesCommandArg#NO_CHECK_SIZES} is enabled, size check
     * will not take place.
     *
     * @param cacheName Cache size.
     */
    private void checkNoCheckSizeInCaseBrokenData(String cacheName) {
        injectTestSystemOut();

        assertEquals(
            EXIT_CODE_OK,
            execute(CACHE.text(), VALIDATE_INDEXES.text(), NO_CHECK_SIZES.argName(), cacheName)
        );

        String out = testOut.toString();
        assertContains(log, out, "issues found (listed above)");
        assertNotContains(log, out, "Size check");
    }

    /**
     * Checking whether cache and index size check is correct.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param rmvByTbl Number of deleted items per table.
     * @param cacheSizeExp Function for getting expected cache size.
     * @param idxSizeExp Function for getting expected index size.
     */
    private void validateCheckSizes(
        IgniteEx node,
        String cacheName,
        Map<String, AtomicInteger> rmvByTbl,
        Function<AtomicInteger, Integer> cacheSizeExp,
        Function<AtomicInteger, Integer> idxSizeExp
    ) {
        requireNonNull(node);
        requireNonNull(cacheName);
        requireNonNull(rmvByTbl);
        requireNonNull(cacheSizeExp);
        requireNonNull(idxSizeExp);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute(CACHE.text(), VALIDATE_INDEXES.text(), cacheName));

        String out = testOut.toString();
        assertContains(log, out, "issues found (listed above)");
        assertContains(log, out, "Size check");

        Map<String, ValidateIndexesCheckSizeResult> valIdxCheckSizeResults =
            ((VisorValidateIndexesTaskResult)lastOperationResult).results().get(node.localNode().id())
                .checkSizeResult();

        assertEquals(rmvByTbl.size(), valIdxCheckSizeResults.size());

        for (Map.Entry<String, AtomicInteger> rmvByTblEntry : rmvByTbl.entrySet()) {
            ValidateIndexesCheckSizeResult checkSizeRes = valIdxCheckSizeResults.entrySet().stream()
                .filter(e -> e.getKey().contains(rmvByTblEntry.getKey()))
                .map(Map.Entry::getValue)
                .findAny()
                .orElse(null);

            assertNotNull(checkSizeRes);
            assertEquals((int)cacheSizeExp.apply(rmvByTblEntry.getValue()), checkSizeRes.cacheSize());

            Collection<ValidateIndexesCheckSizeIssue> issues = checkSizeRes.issues();
            assertFalse(issues.isEmpty());

            issues.forEach(issue -> {
                assertEquals((int)idxSizeExp.apply(rmvByTblEntry.getValue()), issue.idxSize());

                Throwable err = issue.err();
                assertNotNull(err);
                assertEquals("Cache and index size not same.", err.getMessage());
            });
        }
    }

    /**
     * Get table name for cache row.
     *
     * @param cacheName  Cache name.
     * @param cacheDataRow Cache row.
     */
    private String tableName(String cacheName, CacheDataRow cacheDataRow) {
        requireNonNull(cacheName);
        requireNonNull(cacheDataRow);

        try {
            return crd.context().query().typeByValue(
                cacheName,
                crd.cachex(cacheName).context().cacheObjectContext(),
                cacheDataRow.key(),
                cacheDataRow.value(),
                false
            ).tableName();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Get table name for cache entry.
     *
     * @param cacheName  Cache name.
     * @param cacheEntry Cache entry.
     */
    private <K, V> String tableName(String cacheName, Cache.Entry<K, V> cacheEntry) {
        requireNonNull(cacheName);
        requireNonNull(cacheEntry);

        try {
            return crd.context().query().typeByValue(
                cacheName,
                crd.cachex(cacheName).context().cacheObjectContext(),
                null,
                ((CacheObject)cacheEntry.getValue()),
                false
            ).tableName();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
