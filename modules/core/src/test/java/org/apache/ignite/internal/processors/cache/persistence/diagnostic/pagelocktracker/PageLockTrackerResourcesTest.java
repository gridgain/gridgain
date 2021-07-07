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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Tests for memory leakage in the {@link SharedPageLockTracker}.
 *
 * @see <a href="https://ggsystems.atlassian.net/browse/GG-33352">GG-33352</a>
 */
public class PageLockTrackerResourcesTest extends GridCommonAbstractTest {
    /**
     * Tests that all data structures get unregistered on node stop.
     */
    @Test
    public void testStructureIdLeakOnNodeDestroy() throws Exception {
        PageLockTrackerManager lockTracker;

        try (IgniteEx ignite = startGrid()) {
            lockTracker = getPageLockTrackerManager(ignite);

            ignite.createCache("foobar").put("foo", "bar");

            assertThat(lockTracker.dumpLocks().structureIdToStructureName, is(not(anEmptyMap())));
        }

        assertThat(lockTracker.dumpLocks().structureIdToStructureName, is(anEmptyMap()));
    }

    /**
     * Tests that all data structures get unregistered on cache destroy.
     */
    @Test
    public void testStructureIdLeakOnCacheDestroy() throws Exception {
        try (IgniteEx ignite = startGrid()) {
            PageLockTrackerManager lockTracker = getPageLockTrackerManager(ignite);

            int preCreateSize = lockTracker.dumpLocks().structureIdToStructureName.size();

            ignite.createCache("foobar").put("foo", "bar");

            int afterCreateSize = lockTracker.dumpLocks().structureIdToStructureName.size();

            assertThat(afterCreateSize, is(greaterThan(preCreateSize)));

            ignite.destroyCache("foobar");

            int afterDestroySize = lockTracker.dumpLocks().structureIdToStructureName.size();

            assertThat(afterDestroySize, is(equalTo(preCreateSize)));
        }
    }

    /**
     * Extracts the {@link PageLockTrackerManager} from the given Ignite node.
     */
    private static PageLockTrackerManager getPageLockTrackerManager(IgniteEx ignite) {
        return ignite.context().cache().context().diagnostic().pageLockTracker();
    }
}
