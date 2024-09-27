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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.Factory;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
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
        if (logTree != null) {
            logTree.destroy();
        }
    }

    /** {@inheritDoc} */
    @Override public void put(UpdateLogRow row) throws IgniteCheckedException {
        init();

        logTree.putx(row);
    }

    /** {@inheritDoc} */
    @Override
    public void put(UpdateLogRow row, IgniteLogger log, CacheObjectContext cctx) throws IgniteCheckedException {
        init();

        boolean treeUpdated = logTree.putx(row);

        if (log.isDebugEnabled()) {
            String rowKey = row.key() == null ? "null" : row.key().value(cctx, false);
            log.debug("Attempt to update log tree for key " + rowKey + ", success="
                    + treeUpdated);
        }
    }

    /** {@inheritDoc} */
    @Override public PartitionLogTree tree() {
        return logTree;
    }

    /** {@inheritDoc} */
    @Override public boolean hasTree() {
        return logTree != null;
    }

    /** {@inheritDoc} */
    @Override public GridCursor<UpdateLogRow> find(UpdateLogRow lower, UpdateLogRow upper) throws IgniteCheckedException {
        if (logTree != null) {
            return logTree.find(lower, upper);
        }

        return GridCursor.EMPTY_CURSOR;
    }

    /** {@inheritDoc} */
    @Override public void remove(UpdateLogRow row) throws IgniteCheckedException {
        if (logTree != null) {
            logTree.removex(row);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void remove(UpdateLogRow row, IgniteLogger log, CacheObjectContext cctx) throws IgniteCheckedException {
        if (logTree != null) {
            boolean removeUpdated = logTree.removex(row);

            if (log.isDebugEnabled()) {
                String rowKey = row.key() == null ? "null" : row.key().value(cctx, false);
                log.debug("Attempt to remove from log tree for key " + rowKey + ", success="
                        + removeUpdated);
            }
        }
    }

    /**
     * Initialize the storage if needed.
     */
    private void init() throws IgniteCheckedException {
        if (logTree != null) {
            return;
        }

        synchronized (this) {
            if (logTree == null) {
                assert factory != null;

                logTree = factory.create();
            }
        }
    }
}
