/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query.h2.maintenance;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.INDEX_REBUILD_MNTC_TASK_NAME;
import static org.apache.ignite.internal.processors.query.h2.maintenance.MaintenanceRebuildIndexUtils.INDEX_REBUILD_PARAMETER_SEPARATOR;
import static org.apache.ignite.internal.processors.query.h2.maintenance.MaintenanceRebuildIndexUtils.mergeTasks;
import static org.apache.ignite.internal.processors.query.h2.maintenance.MaintenanceRebuildIndexUtils.parseMaintenanceTaskParameters;
import static org.apache.ignite.internal.processors.query.h2.maintenance.MaintenanceRebuildIndexUtils.toMaintenanceTask;
import static org.junit.Assert.assertEquals;

/** Tests for {@link MaintenanceRebuildIndexTarget}. */
public class MaintenanceRebuildIndexUtilsSelfTest {
    /**
     * Tests that maintenance task's parameters can be stringified and parsed back.
     */
    @Test
    public void testSerializeAndParse() {
        int cacheId = 1;
        String idxName = "test";

        MaintenanceTask task = toMaintenanceTask(cacheId, idxName);

        assertEquals(INDEX_REBUILD_MNTC_TASK_NAME, task.name());

        List<MaintenanceRebuildIndexTarget> targets = parseMaintenanceTaskParameters(task.parameters());

        assertEquals(1, targets.size());

        MaintenanceRebuildIndexTarget target = targets.get(0);

        assertEquals(cacheId, target.cacheId());
        assertEquals(idxName, target.idxName());
    }

    /**
     * Tests that maintenance task's parameters can be merged correctly.
     */
    @Test
    public void testMerge() {
        List<MaintenanceRebuildIndexTarget> targets = IntStream.range(0, 100)
            .mapToObj(i -> new MaintenanceRebuildIndexTarget(i, "idx" + i)).collect(toList());

        MaintenanceRebuildIndexTarget first = targets.get(0);

        // Create initial task
        MaintenanceTask task = toMaintenanceTask(first.cacheId(), first.idxName());

        // Merge all tasks in one task
        for (MaintenanceRebuildIndexTarget target : targets)
            task = mergeTasks(task, toMaintenanceTask(target.cacheId(), target.idxName()));

        assertEquals(INDEX_REBUILD_MNTC_TASK_NAME, task.name());

        List<MaintenanceRebuildIndexTarget> parsedTargets = parseMaintenanceTaskParameters(task.parameters());

        assertEquals(targets, parsedTargets);
    }

    /**
     * Tests that merging same tasks yields a correct task without duplication of its parameters.
     */
    @Test
    public void testMergeSame() {
        int cacheId = 1;
        String idxName = "test";

        MaintenanceTask task1 = toMaintenanceTask(cacheId, idxName);
        MaintenanceTask task2 = toMaintenanceTask(cacheId, idxName);

        MaintenanceTask mergedTask = mergeTasks(task1, task2);

        assertEquals(INDEX_REBUILD_MNTC_TASK_NAME, mergedTask.name());

        assertEquals(task1.parameters(), mergedTask.parameters());
    }

    /**
     * Tests that {@link MaintenanceRebuildIndexUtils#INDEX_REBUILD_PARAMETER_SEPARATOR} can be used in the index name.
     */
    @Test
    public void testIndexNameWithSeparatorCharacter() {
        int cacheId = 1;
        String idxName = "test" + INDEX_REBUILD_PARAMETER_SEPARATOR + "test";

        MaintenanceTask task = toMaintenanceTask(cacheId, idxName);

        List<MaintenanceRebuildIndexTarget> targets = parseMaintenanceTaskParameters(task.parameters());

        assertEquals(1, targets.size());

        MaintenanceRebuildIndexTarget target = targets.get(0);

        assertEquals(cacheId, target.cacheId());
        assertEquals(idxName, target.idxName());
    }
}
