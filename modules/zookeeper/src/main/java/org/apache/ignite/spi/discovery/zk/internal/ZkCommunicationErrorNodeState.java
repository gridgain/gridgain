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

package org.apache.ignite.spi.discovery.zk.internal;

import java.io.Serializable;
import java.util.BitSet;

/**
 *
 */
class ZkCommunicationErrorNodeState implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    final BitSet commState;

    /** */
    final Exception err;

    /**
     * @param commState Communication state.
     * @param err Error if failed get communication state..
     */
    ZkCommunicationErrorNodeState(BitSet commState, Exception err) {
        assert commState != null || err != null;

        this.commState = commState;
        this.err = err;
    }
}
