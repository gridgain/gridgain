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

package org.apache.ignite.internal.visor.dr;

import java.io.Serializable;
import java.util.Set;

/**
 * Validate cache entry job result.
 */
public class VisorDrRepairPartitionCountersJobResult extends VisorDrPartitionCountersJobResult {
    /** */
    private static final long serialVersionUID = 0L;

    private long entriesFixed;

    private long tombstoneCleared;

    public VisorDrRepairPartitionCountersJobResult(String cacheOrGroupName, long size,
            Set<Integer> affectedCaches, Set<Integer> affectedPartitions, long entriesProcessed,
            long brokenEntriesFound, long entriesFixed, long tombstoneCleared) {
        super(cacheOrGroupName, size, affectedCaches, affectedPartitions, entriesProcessed,
                brokenEntriesFound);
        this.entriesFixed = entriesFixed;
        this.tombstoneCleared = tombstoneCleared;
    }
}