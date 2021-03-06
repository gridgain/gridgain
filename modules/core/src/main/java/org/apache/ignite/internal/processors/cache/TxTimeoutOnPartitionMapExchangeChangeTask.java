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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * The task for changing transaction timeout on partition map exchange.
 */
public class TxTimeoutOnPartitionMapExchangeChangeTask implements CachePartitionExchangeWorkerTask {
    /** Discovery message. */
    private final TxTimeoutOnPartitionMapExchangeChangeMessage msg;

    /**
     * Constructor.
     *
     * @param msg Discovery message.
     */
    public TxTimeoutOnPartitionMapExchangeChangeTask(TxTimeoutOnPartitionMapExchangeChangeMessage msg) {
        assert msg != null;
        this.msg = msg;
    }

    /**
     * Gets discovery message.
     *
     * @return Discovery message.
     */
    public TxTimeoutOnPartitionMapExchangeChangeMessage message() {
        return msg;
    }

    /** {@inheritDoc} */
    @Override public boolean skipForExchangeMerge() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxTimeoutOnPartitionMapExchangeChangeTask.class, this);
    }
}
