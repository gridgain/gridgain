package org.apache.ignite.internal.visor.dr;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.visor.VisorJob;
import org.jetbrains.annotations.Nullable;

public abstract class VisorDrPartitionCountersJob<K, V> extends VisorJob<K, V> {
    protected VisorDrPartitionCountersJob(@Nullable K arg, boolean debug) {
        super(arg, debug);
    }

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
