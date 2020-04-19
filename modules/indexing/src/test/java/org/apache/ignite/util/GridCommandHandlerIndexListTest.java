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

import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.visor.cache.index.IndexListInfoContainer;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EMPTY_GROUP_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CACHE_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CACHE_NAME_SECOND;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.GROUP_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.GROUP_NAME_SECOND;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.THREE_ENTRIES_CACHE_NAME_COMMON_PART;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillSeveralCaches;

/**
 * Test for --cache index_list command. Uses single cluster per suite.
 */
public class GridCommandHandlerIndexListTest extends GridCommandHandlerAbstractTest {
    /** Grids number. */
    public static final int GRIDS_NUM = 2;

    /** Ignite instance. */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        ignite = startGrids(GRIDS_NUM);

        ignite.cluster().active(true);

        createAndFillSeveralCaches(ignite);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /**
     * Tests --index name option and output correctness.
     */
    @Test
    public void testCacheIndexList() {
        final String idxName = "PERSON_ORGID_ASC_IDX";
        final int expectedLinesNum = 15;
        final int expectedIndexDescrLinesNum = 2;

        injectTestSystemOut();

        final CommandHandler handler = new CommandHandler(createTestLogger());

        execute(handler, "--cache", "indexes_list", "--index-name", idxName);

        String outStr = testOut.toString();

        assertTrue(outStr.contains("grpName=" + GROUP_NAME +", cacheName=" + CACHE_NAME + ", idxName=PERSON_ORGID_ASC_IDX, " +
                                   "colsNames=ArrayList [ORGID, _KEY], tblName=PERSON"));
        assertTrue(outStr.contains("grpName=" + GROUP_NAME_SECOND + ", cacheName=" + CACHE_NAME_SECOND + ", idxName=PERSON_ORGID_ASC_IDX, " +
                                   "colsNames=ArrayList [ORGID, _KEY], tblName=PERSON"));

        final String[] outputLines = outStr.split("\n");

        int outputLinesNum = outputLines.length;

        assertEquals("Unexpected number of lines: " + outputLinesNum, outputLinesNum, expectedLinesNum);

        long indexDescrLinesNum = Arrays.stream(outputLines)
                                        .filter(s -> s.contains("grpName="))
                                        .count();

        assertEquals("Unexpected number of index desctiption lines: " + indexDescrLinesNum,
                     indexDescrLinesNum, expectedIndexDescrLinesNum);

        Set<IndexListInfoContainer> cmdResult = handler.getLastOperationResult();
        assertNotNull(cmdResult);

        final int resSetSize = cmdResult.size();
        assertEquals("Unexpected result set size: " + resSetSize, resSetSize, 2);

        boolean isResSetCorrect = cmdResult.stream()
                                           .map(IndexListInfoContainer::indexName)
                                           .allMatch((name) -> name.equals(idxName));

        assertTrue("Unexpected result set", isResSetCorrect);
    }

    /** */
    @Test
    public void testGroupNameFilter1() {
        checkGroup("^" + GROUP_NAME + "$", GROUP_NAME, 7);
    }

    /** */
    @Test
    public void testGroupNameFilter2() {
        checkGroup("^" + GROUP_NAME, (name) -> name.equals(GROUP_NAME) || name.equals(GROUP_NAME_SECOND), 10);
    }

    /** */
    @Test
    public void testEmptyGroupFilter() {
        checkGroup(EMPTY_GROUP_NAME, EMPTY_GROUP_NAME, 4);
    }

    /** */
    private void checkGroup(String grpRegEx, String grpName, int expectedResNum) {
        checkGroup(grpRegEx, (name) -> name.equals(grpName), expectedResNum);
    }

    /** */
    private void checkGroup(String grpRegEx, Predicate<String> predicate, int expectedResNum) {
        final CommandHandler handler = new CommandHandler(createTestLogger());

        execute(handler, "--cache", "indexes_list", "--group-name", grpRegEx);

        Set<IndexListInfoContainer> cmdResult = handler.getLastOperationResult();
        assertNotNull(cmdResult);

        boolean isResCorrect = cmdResult.stream()
            .map(IndexListInfoContainer::groupName)
            .allMatch(predicate);

        assertTrue("Unexpected command result", isResCorrect);

        int indexesNum = cmdResult.size();
        assertEquals("Unexpected number of indexes: " + indexesNum, indexesNum, expectedResNum);
    }

    /** */
    @Test
    public void testCacheNameFilter1() {
        checkCacheNameFilter(THREE_ENTRIES_CACHE_NAME_COMMON_PART,
            cacheName -> cacheName.contains(THREE_ENTRIES_CACHE_NAME_COMMON_PART),
            8);
    }

    /** */
    @Test
    public void testCacheNameFilter2() {
        checkCacheNameFilter("^" + THREE_ENTRIES_CACHE_NAME_COMMON_PART,
            cacheName -> cacheName.startsWith(THREE_ENTRIES_CACHE_NAME_COMMON_PART),
            4);
    }

    /** */
    private void checkCacheNameFilter(String cacheRegEx, Predicate<String> predicate, int expectedResNum) {
        final CommandHandler handler = new CommandHandler(createTestLogger());

        execute(handler, "--cache", "indexes_list", "--cache-name", cacheRegEx);

        Set<IndexListInfoContainer> cmdResult = handler.getLastOperationResult();
        assertNotNull(cmdResult);

        boolean isResCorrect = cmdResult.stream()
            .map(IndexListInfoContainer::cacheName)
            .allMatch(predicate);

        assertTrue("Unexpected command result", isResCorrect);

        int indexesNum = cmdResult.size();
        assertEquals("Unexpected number of indexes: " + indexesNum, indexesNum, expectedResNum);
    }
}
