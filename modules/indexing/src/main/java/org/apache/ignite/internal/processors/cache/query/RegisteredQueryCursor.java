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

package org.apache.ignite.internal.processors.cache.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.RunningQueryManager;

/**
 * Query cursor for registered as running queries.
 *
 * Running query will be unregistered during close of cursor.
 */
public class RegisteredQueryCursor<T> extends QueryCursorImpl<T> {
    /** */
    private final AtomicBoolean unregistered = new AtomicBoolean(false);

    /** */
    private RunningQueryManager runningQryMgr;

    /** */
    private Long qryId;

    /** Exception caused query failed or {@code null} if it succeded. */
    private Exception failReason;

    /**
     * @param iterExec Query executor.
     * @param cancel Cancellation closure.
     * @param runningQryMgr Running query manager.
     * @param qryId Registered running query id.
     */
    public RegisteredQueryCursor(Iterable<T> iterExec, GridQueryCancel cancel, RunningQueryManager runningQryMgr,
        Long qryId) {
        super(iterExec, cancel);

        assert runningQryMgr != null;
        assert qryId != null;

        this.runningQryMgr = runningQryMgr;
        this.qryId = qryId;
    }

    /** {@inheritDoc} */
    @Override protected Iterator<T> iter() {
        try {
            return super.iter();
        }
        catch (Exception e) {
            failReason = e;

            if (QueryUtils.wasCancelled(failReason))
                unregisterQuery();

            throw e;
        }
    }

    /** Re-implements to track thrown exceptions on query error / cancel. */
    @Override public List<T> getAll() {
        List<T> all = new ArrayList<>();

        try {
            Iterator<T> iter = iter(); // Implicitly calls iterator() to do all checks.

            while (iter.hasNext())
                all.add(iter.next());

            return all;
        }
        catch (Exception e) {
            failReason = e;

            if (QueryUtils.wasCancelled(failReason))
                unregisterQuery();

            throw e;
        }
        finally {
            // Update state if the results is read to end
            STATE_UPDATER.compareAndSet(this, State.RESULT_READY, State.NO_DATA);

            close();
        }
    }

    /** {@inheritDoc}
     * NB: if the method changed please change also RegisteredQueryCursor#getAll(QueryCursorEx.Consumer<T>)
     * because it re-implements this logic to track exceptions on query registry.
     */
    @Override public void getAll(QueryCursorEx.Consumer<T> clo) throws IgniteCheckedException {
        try {
            Iterator<T> iter = iter(); // Implicitly calls iterator() to do all checks.

            while (iter.hasNext())
                clo.consume(iter.next());
        }
        catch (Exception e) {
            failReason = e;

            if (QueryUtils.wasCancelled(failReason))
                unregisterQuery();

            throw e;
        }
        finally {
            // Update state if the results is read to end
            STATE_UPDATER.compareAndSet(this, State.RESULT_READY, State.NO_DATA);

            close();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        unregisterQuery();

        super.close();
    }

    /**
     * Unregister query.
     */
    private void unregisterQuery(){
        if (unregistered.compareAndSet(false, true))
            runningQryMgr.unregister(qryId, failReason);
    }
}
