package org.apache.ignite.internal.visor.dr;

import java.util.Map;
import java.util.Set;

public class DrFixPartitionCountersJobArgs extends DrPartitionCountersJobArgs {
    /** */
    private final int batchSize;

    /** */
    private final boolean keepBinary;

    public DrFixPartitionCountersJobArgs(
            Map<String, Set<Integer>> cachesWithPartitions, boolean debug, int batchSize,
            boolean keepBinary) {
        super(cachesWithPartitions, debug);

        this.batchSize = batchSize;
        this.keepBinary = keepBinary;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isKeepBinary() {
        return keepBinary;
    }
}
