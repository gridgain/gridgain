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

package org.apache.ignite.internal.sql.optimizer.affinity;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Node with partition which should be extracted from argument.
 */
public class PartitionParameterNode extends PartitionSingleNode {
    /** Indexing. */
    @GridToStringExclude
    private final PartitionResolver partResolver;

    /** Index. */
    private final int idx;

    /** Parameter data type. */
    private final int type;

    /** Mapped parameter type. */
    private final PartitionParameterType mappedType;

    /**
     * Constructor.
     *
     * @param tbl Table descriptor.
     * @param partResolver Partition resolver.
     * @param idx Parameter index.
     * @param type Parameter data type.
     * @param mappedType Mapped parameter type to be used by thin clients.
     */
    public PartitionParameterNode(PartitionTable tbl, PartitionResolver partResolver, int idx, int type,
        PartitionParameterType mappedType) {
        super(tbl);

        this.partResolver = partResolver;
        this.idx = idx;
        this.type = type;
        this.mappedType = mappedType;
    }

    /** {@inheritDoc} */
    @Override public Integer applySingle(PartitionClientContext cliCtx, Object... args) throws IgniteCheckedException {
        assert args != null;
        assert idx < args.length;

        Object arg = args[idx];

        if (cliCtx != null)
            return cliCtx.partition(arg, mappedType, tbl.cacheName());
        else {
            assert partResolver != null;

            return partResolver.partition(
                arg,
                type,
                tbl.cacheName()
            );
        }
    }

    /** {@inheritDoc} */
    @Override public boolean constant() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int value() {
        return idx;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionParameterNode.class, this);
    }
}
