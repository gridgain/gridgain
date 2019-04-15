/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.internal.util.typedef.internal.U;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.RandomAccess;

/**
 *
 */
public class ReduceBlockList<Z> extends AbstractList<Z> implements RandomAccess {
    /** */
    private final List<List<Z>> blocks;

    /** */
    private int size;

    /** */
    private final int maxBlockSize;

    /** */
    private final int shift;

    /** */
    private final int mask;

    /**
     * @param maxBlockSize Max block size.
     */
    public ReduceBlockList(int maxBlockSize) {
        assert U.isPow2(maxBlockSize);

        this.maxBlockSize = maxBlockSize;

        shift = Integer.numberOfTrailingZeros(maxBlockSize);
        mask = maxBlockSize - 1;

        blocks = new ArrayList<>();
        blocks.add(new ArrayList<Z>());
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public boolean add(Z z) {
        size++;

        List<Z> lastBlock = lastBlock();

        lastBlock.add(z);

        if (lastBlock.size() == maxBlockSize)
            blocks.add(new ArrayList<Z>());

        return true;
    }

    /** {@inheritDoc} */
    @Override public Z get(int idx) {
        return blocks.get(idx >>> shift).get(idx & mask);
    }

    /**
     * @return Last block.
     */
    public List<Z> lastBlock() {
        return ReduceIndex.last(blocks);
    }

    /**
     * @return Evicted block.
     */
    public List<Z> evictFirstBlock() {
        // Remove head block.
        List<Z> res = blocks.remove(0);

        size -= res.size();

        return res;
    }
}
