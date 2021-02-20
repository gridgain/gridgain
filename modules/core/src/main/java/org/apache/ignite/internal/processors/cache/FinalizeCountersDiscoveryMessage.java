package org.apache.ignite.internal.processors.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

public class FinalizeCountersDiscoveryMessage implements DiscoveryCustomMessage {

    /** Custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    public Map<Integer, Map<Integer, Map<UUID, Long>>> partSizesMap = new HashMap();

    @Override public IgniteUuid id() {
        return id;
    }

    @Override public @Nullable DiscoveryCustomMessage ackMessage() {
        return null;
    }

    @Override public boolean isMutable() {
        return false;
    }

    @Override public boolean stopProcess() {
        return false;
    }

    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        return discoCache.copy(topVer, null);
    }
}
