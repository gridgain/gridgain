/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.checker;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Partition reconciliation task arguments.
 */
public class VisorPartitionReconciliationTaskArg extends IgniteDataTransferObject {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** Caches. */
    private Set<String> caches;

    /** If {@code true} - Partition Reconciliation&Fix: update from Primary partition. */
    private boolean fixMode;

    /** Print result to console. */
    private boolean console;

    /** If {@code true} - print data to result with sensitive information: keys and values. */
    private boolean verbose;

    /** Percent of system loading from 0 to 1. */
    private double loadFactor;

    /** Amount of keys to retrieve within one job. */
    private int batchSize;

    /** Amount of potentially inconsistent keys recheck attempts. */
    private int recheckAttempts;

    /**
     * Specifies which fix algorithm to use: options {@code PartitionReconciliationRepairMeta.RepairAlg} while repairing
     * doubtful keys.
     */
    private RepairAlgorithm repairAlg;

    /** Recheck delay seconds. */
    private int recheckDelay;

    /**
     * Default constructor.
     */
    public VisorPartitionReconciliationTaskArg() {
        // No-op.
    }

    /**
     * Constructor.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public VisorPartitionReconciliationTaskArg(Set<String> caches, boolean fixMode, boolean verbose, boolean console,
        double loadFactor, int batchSize, int recheckAttempts, RepairAlgorithm repairAlg, int recheckDelay) {
        this.caches = caches;
        this.verbose = verbose;
        this.console = console;
        this.fixMode = fixMode;
        this.loadFactor = loadFactor;
        this.batchSize = batchSize;
        this.recheckAttempts = recheckAttempts;
        this.repairAlg = repairAlg;
        this.recheckDelay = recheckDelay;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, caches);

        out.writeBoolean(fixMode);

        out.writeBoolean(verbose);

        out.writeBoolean(console);

        out.writeDouble(loadFactor);

        out.writeInt(batchSize);

        out.writeInt(recheckAttempts);

        U.writeEnum(out, repairAlg);

        out.writeInt(recheckDelay);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in)
        throws IOException, ClassNotFoundException {

        caches = U.readSet(in);

        fixMode = in.readBoolean();

        verbose = in.readBoolean();

        console = in.readBoolean();

        loadFactor = in.readDouble();

        batchSize = in.readInt();

        recheckAttempts = in.readInt();

        repairAlg = RepairAlgorithm.fromOrdinal(in.readByte());

        recheckDelay = in.readInt();
    }

    /**
     * @return Caches.
     */
    public Set<String> caches() {
        return caches;
    }

    /**
     * @return If  - Partition Reconciliation&Fix: update from Primary partition.
     */
    public boolean fixMode() {
        return fixMode;
    }

    /**
     * @return Amount of keys to retrieve within one job.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * @return Amount of potentially inconsistent keys recheck attempts.
     */
    public int recheckAttempts() {
        return recheckAttempts;
    }

    /**
     * @return If  - print data to result with sensitive information: keys and values.
     */
    public boolean verbose() {
        return verbose;
    }

    /**
     * @return {@code true} if print result to console.
     */
    public boolean console() {
        return console;
    }

    /**
     * @return Specifies which fix algorithm to use: options  while repairing doubtful keys.
     */
    public RepairAlgorithm repairAlg() {
        return repairAlg;
    }

    /**
     *
     */
    public double loadFactor() {
        return loadFactor;
    }

    /**
     * @return Recheck delay seconds.
     */
    public int recheckDelay() {
        return recheckDelay;
    }

    /**
     * Builder class for test purposes.
     */
    public static class Builder {
        /** Caches. */
        private Set<String> caches;

        /** If {@code true} - Partition Reconciliation&Fix: update from Primary partition. */
        private boolean fixMode;

        /** Print result to console. */
        private boolean console;

        /** If {@code true} - print data to result with sensitive information: keys and values. */
        private boolean verbose;

        /** Percent of system loading from 0 to 1. */
        private double loadFactor;

        /** Amount of keys to retrieve within one job. */
        private int batchSize;

        /** Amount of potentially inconsistent keys recheck attempts. */
        private int recheckAttempts;

        /**
         * Specifies which fix algorithm to use: options {@code PartitionReconciliationRepairMeta.RepairAlg} while
         * repairing doubtful keys.
         */
        private RepairAlgorithm repairAlg;

        /** Recheck delay seconds. */
        private int recheckDelay;

        /**
         * Default constructor.
         */
        public Builder() {
            caches = null;
            fixMode = false;
            console = true;
            verbose = true;
            loadFactor = 1.0;
            batchSize = 100;
            recheckAttempts = 2;
            recheckDelay = 0;
        }

        /**
         * Build metod.
         */
        public VisorPartitionReconciliationTaskArg build() {
            return new VisorPartitionReconciliationTaskArg(
                caches, fixMode, verbose, console, loadFactor, batchSize, recheckAttempts, repairAlg, recheckDelay);
        }

        /**
         * @param caches New caches.
         */
        public Builder caches(Set<String> caches) {
            this.caches = caches;

            return this;
        }

        /**
         * @param fixMode New if  - Partition Reconciliation&Fix: update from Primary partition.
         */
        public Builder fixMode(boolean fixMode) {
            this.fixMode = fixMode;

            return this;
        }

        /**
         * @param console New print result to console.
         */
        public Builder console(boolean console) {
            this.console = console;

            return this;
        }

        /**
         * @param verbose New if  - print data to result with sensitive information: keys and values.
         */
        public Builder verbose(boolean verbose) {
            this.verbose = verbose;

            return this;
        }

        /**
         * @param loadFactor New percent of system loading from 0 to 1.
         */
        public Builder loadFactor(double loadFactor) {
            this.loadFactor = loadFactor;

            return this;
        }

        /**
         * @param batchSize New amount of keys to retrieve within one job.
         */
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;

            return this;
        }

        /**
         * @param recheckAttempts New amount of potentially inconsistent keys recheck attempts.
         */
        public Builder recheckAttempts(int recheckAttempts) {
            this.recheckAttempts = recheckAttempts;

            return this;
        }

        /**
         * @param repairAlg New specifies which fix algorithm to use: options  while repairing doubtful keys.
         */
        public Builder repairAlg(RepairAlgorithm repairAlg) {
            this.repairAlg = repairAlg;

            return this;
        }

        /**
         * @param recheckDelay Recheck delay seconds.
         */
        public Builder recheckDelay(int recheckDelay) {
            this.recheckDelay = recheckDelay;

            return this;
        }
    }
}
