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

package org.apache.ignite.internal.processors.query.h2.disk;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.apache.ignite.internal.processors.query.h2.QueryMemoryManager;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.engine.SessionInterface;
import org.gridgain.internal.h2.result.ResultInterface;

/**
 * Basic class for external result.
 */
@SuppressWarnings({"WeakerAccess", "ForLoopReplaceableByForEach"})
public abstract class AbstractExternalResult<T> implements AutoCloseable {
    /** Logger. */
    protected final IgniteLogger log;

    /** Current size in rows. */
    protected int size;

    /** Memory tracker. */
    protected final H2MemoryTracker memTracker;

    /** Memory manager. */
    protected final QueryMemoryManager memMgr;

    /** Parent result. */
    protected final AbstractExternalResult parent;

    /** Child results count. Parent result is closed only when all children are closed. */
    private int childCnt;

    /** */
    private boolean closed;

    /** File with spilled rows data. */
    protected final ExternalResultData<T> data;

    /**
     * @param useHashIdx Whether to use hash index.
     * @param initSize Initial size.
     * @param cls Class of stored data.
     * @param useHashIdx Flag whether to use hash index.
     * @param initSize Initial result set size.
     */
    protected AbstractExternalResult(Session ses,
        boolean useHashIdx,
        long initSize,
        Class<T> cls) {
        memMgr = (QueryMemoryManager)ses.groupByDataFactory();
        assert memMgr != null;
        this.log = memMgr.log();
        this.data = memMgr.createExternalData(ses, useHashIdx, initSize, cls);
        this.parent = null;
        this.memTracker = ses.memoryTracker().createChildTracker();
    }

    /**
     * Used for {@link ResultInterface#createShallowCopy(SessionInterface)} only.
     * @param parent Parent result.
     */
    protected AbstractExternalResult(AbstractExternalResult parent) {
        memMgr = parent.memMgr;
        log = parent.log;
        size = parent.size;
        data = parent.data.createShallowCopy();
        this.parent = parent;
        memTracker = null;
    }

    /** */
    protected boolean needToSpill() {
        return !memTracker.reserve(0);
    }

    /** */
    protected synchronized void onChildCreated() {
        childCnt++;
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() {
        if (closed)
            return;

        closed = true;

        if (parent == null) {
            if (childCnt == 0)
                onClose();
        }
        else
            parent.closeChild();
    }

    /** */
    protected synchronized void closeChild() {
        if (--childCnt == 0 && closed)
            onClose();
    }

    /** */
    protected void onClose() {
        data.close();
    }

    /** */
    public int size() {
        return size;
    }
}
