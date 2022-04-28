package org.apache.ignite.internal.visor.dr;

import java.util.Map;
import java.util.Set;

public class VisorDrPartitionCountersJobArgs {
    /** */
    private final Map<String, Set<Integer>> cachesWithPartitions;

    /** */
    private final boolean debug;

    public VisorDrPartitionCountersJobArgs(
            Map<String, Set<Integer>> cachesWithPartitions, boolean debug) {
        this.cachesWithPartitions = cachesWithPartitions;
        this.debug = debug;
    }

    public Map<String, Set<Integer>> getCachesWithPartitions() {
        return cachesWithPartitions;
    }

    public boolean isDebug() {
        return debug;
    }
}
