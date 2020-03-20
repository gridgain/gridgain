/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.transactions.TransactionConcurrency;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;

/**
 */
public class TxPartitionCounterStateConsistencyOptimisticTest extends TxPartitionCounterStateConsistencyTest {
    /** {@inheritDoc} */
    @Override protected TransactionConcurrency concurrency() {
        return OPTIMISTIC;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testPartitionConsistencyDuringRebalanceAndConcurrentUpdates_LateAffinitySwitch() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testPartitionConsistencyDuringRebalanceAndConcurrentUpdates_TxDuringPME() throws Exception {
        // No-op.
    }

    @Override
    @Test
    public void testPrimaryLeftUnderLoadToSwitchingPartitions_3() throws Exception {
        super.testPrimaryLeftUnderLoadToSwitchingPartitions_3();
    }
}
