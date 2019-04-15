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

package org.apache.ignite.internal.processors.cache.query;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
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

    /** Flag to indicate error. */
    private boolean failed;

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

    @Override protected Iterator<T> iter() {
        try {
            return super.iter();
        }
        catch (Exception e) {
            failed = true;

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (unregistered.compareAndSet(false, true))
            runningQryMgr.unregister(qryId, failed);

        super.close();
    }
}
