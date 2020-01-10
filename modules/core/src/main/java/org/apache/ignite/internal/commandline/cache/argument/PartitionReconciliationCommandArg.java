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

package org.apache.ignite.internal.commandline.cache.argument;

import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.cache.CacheSubcommands;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;

/**
 * {@link CacheSubcommands#PARTITION_RECONCILIATION} command arguments.
 */
public enum PartitionReconciliationCommandArg implements CommandArg {
    /** If {@code true} - Partition Reconciliation&Fix: update from Primary partition. */
    FIX_MODE("--fix-mode", Boolean.FALSE),

    /**
     * Specifies which fix algorithm to use while repairing doubtful keys: options {@code
     * PartitionReconciliationRepairMeta.RepairAlg}.
     */
    FIX_ALG("--fix-alg", RepairAlgorithm.defaultValue()),

    /** If {@code true} - print data to result with sensitive information: keys and values. */
    VERBOSE("--verbose", Boolean.FALSE),

    /** Percent of system loading between 0 and 1. */
    LOAD_FACTOR("--load-factor", 1d),

    /** Amount of keys to retrieve within one job. */
    BATCH_SIZE("--batch-size", 1000),

    /** Amount of potentially inconsistent keys recheck attempts. */
    RECHECK_ATTEMPTS("--recheck-attempts", 2),

    /** Print result to console. */
    CONSOLE("--console", Boolean.FALSE),

    /** Recheck delay seconds. */
    RECHECK_DELAY("--recheck-delay", 10);

    /** Option name. */
    private final String name;

    /** Default value. */
    private Object dfltVal;

    /** */
    PartitionReconciliationCommandArg(String name, Object dfltVal) {
        this.name = name;
        this.dfltVal = dfltVal;
    }

    /** {@inheritDoc} */
    @Override public String argName() {
        return name;
    }

    /**
     * @return Default value.
     */
    public Object defaultValue() {
        return dfltVal;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
