package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Cancellable future adapter. TODO: global statistics? void? just statistics?
 */
public class StatsCollectionFutureAdapter extends GridFutureAdapter {
    /** {@inheritDoc} */
    @Override public boolean cancel() {
        boolean res = onDone(null, null, true);

        if (res)
            onCancelled();

        return res;
    }
}
