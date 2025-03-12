/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.tree.updatelog;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Factory;
import org.apache.ignite.internal.util.lang.GridCursor;

/**
 * UpdateLog structure backed by BTree.
 */
public class UpdateLogImpl implements UpdateLog {
    private final Factory<PartitionLogTree> factory;

    private volatile PartitionLogTree logTree;

    /**
     * Creates an UpdateLog with initialized storage.
     */
    public UpdateLogImpl(PartitionLogTree logTree) {
        this.logTree = logTree;
        factory = null;
    }

    /**
     * Creates an UpdateLog with lazy storage initialization.
     */
    public UpdateLogImpl(Factory<PartitionLogTree> factory) {
        this.factory = factory;
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws IgniteCheckedException {
        if (hasTree()) {
            logTree.destroy();
        }
    }

    /** {@inheritDoc} */
    @Override public void put(UpdateLogRow row) throws IgniteCheckedException {
        init();

        logTree.putx(row);
    }

    /** {@inheritDoc} */
    @Override public PartitionLogTree tree() throws IgniteCheckedException {
        init();

        return logTree;
    }

    /** {@inheritDoc} */
    @Override public boolean hasTree() {
        return logTree != null;
    }

    /** {@inheritDoc} */
    @Override public GridCursor<UpdateLogRow> find(UpdateLogRow lower, UpdateLogRow upper) throws IgniteCheckedException {
        if (hasTree()) {
            return logTree.find(lower, upper);
        }

        return GridCursor.EMPTY_CURSOR;
    }

    /** {@inheritDoc} */
    @Override public void remove(UpdateLogRow row) throws IgniteCheckedException {
        if (hasTree()) {
            logTree.removex(row);
        }
    }

    /**
     * Initialize the storage if needed.
     */
    private void init() throws IgniteCheckedException {
        if (hasTree()) {
            return;
        }

        synchronized (this) {
            if (!hasTree()) {
                assert factory != null;

                logTree = factory.create();
            }
        }
    }
}
