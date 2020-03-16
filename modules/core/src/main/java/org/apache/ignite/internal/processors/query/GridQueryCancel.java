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

package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.QueryCancelledException;

/**
 * Holds query cancel state.
 */
public class GridQueryCancel {
    /** */
    private final List<QueryCancellable> cancelActions = new ArrayList<>(3);

    /** */
    private boolean canceled;

    /**
     * Adds a cancel action.
     *
     * @param clo Clo.
     */
    public synchronized void add(QueryCancellable clo) throws QueryCancelledException {
        assert clo != null;

        if (canceled)
            throw new QueryCancelledException();

        cancelActions.add(clo);
    }

    /**
     * Executes cancel closure.
     */
    public synchronized void cancel() {
        if (canceled)
            return;

        canceled = true;

        IgniteException ex = null;

        // Run actions in the reverse order.
        for (int i = cancelActions.size() - 1; i >= 0; i--) {
            try {
                QueryCancellable act = cancelActions.get(i);

                act.doCancel();
            }
            catch (Exception e) {
                if (ex == null)
                    ex = new IgniteException(e);
                else
                    ex.addSuppressed(e);
            }
        }

        if (ex != null)
            throw ex;
    }

    /**
     * Stops query execution if a user requested cancel.
     */
    public synchronized void checkCancelled() throws QueryCancelledException {
        if (canceled)
            throw new QueryCancelledException();
    }
}
