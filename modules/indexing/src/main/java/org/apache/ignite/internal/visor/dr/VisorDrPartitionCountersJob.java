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

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.visor.VisorJob;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract dr partition counters job class.
 */
public abstract class VisorDrPartitionCountersJob<K, V> extends VisorJob<K, V> {
    protected VisorDrPartitionCountersJob(@Nullable K arg, boolean debug) {
        super(arg, debug);
    }

    /**
     * Locks partition for specific cache.
     *
     * @param part Partition.
     * @param grpCtx Cache group context.
     * @param cache Cache name.
     * @return Local partition.
     */
    protected GridDhtLocalPartition reservePartition(int part, @Nullable CacheGroupContext grpCtx, String cache) {
        if (grpCtx == null)
            throw new IllegalArgumentException("Cache group not found: " + cache);

        GridDhtLocalPartition locPart = grpCtx.topology().localPartition(part);

        if (locPart == null || !locPart.reserve())
            throw new IllegalArgumentException("Failed to reserve partition for group: groupName=" + cache +
                    ", part=" + part);

        if (locPart.state() != OWNING || grpCtx.topology().stopping()) {
            locPart.release();

            throw new IllegalArgumentException("Failed to reserve non-owned partition for group: groupName=" +
                    cache + ", part=" + part);
        }

        return locPart;
    }
}
