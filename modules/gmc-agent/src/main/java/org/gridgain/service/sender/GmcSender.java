package org.gridgain.service.sender;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.gridgain.agent.WebSocketManager;

import java.util.List;

/**
 * Sender to GMC.
 */
public class GmcSender<T> extends RetryableSender<T> {
    /** Max sleep time seconds. */
    private static final int MAX_SLEEP_TIME_SECONDS = 10;

    /** Manager. */
    private final WebSocketManager mgr;

    /** Topic name. */
    private final String dest;

    /** Logger. */
    private final IgniteLogger log;

    /** Retry count. */
    private int retryCnt;

    /**
     * @param ctx Context.
     * @param mgr Manager.
     * @param cap Capacity.
     * @param dest Destination.
     */
    public GmcSender(GridKernalContext ctx, WebSocketManager mgr, int cap, String dest) {
        super(cap);
        this.mgr = mgr;
        this.dest = dest;
        this.log = ctx.log(GmcSender.class);
    }

    /** {@inheritDoc} */
    @Override protected void sendInternal(List<T> elements) throws Exception {
        Thread.sleep(Math.min(MAX_SLEEP_TIME_SECONDS, retryCnt) * 1000);

        if (!mgr.send(dest, elements)) {
            retryCnt++;

            if (retryCnt == 1)
                log.warning("Failed to send message to GMC, will retry in " + retryCnt * 1000 + " ms");

            throw new IgniteException("Failed to send message to GMC");
        } else
            retryCnt = 0;
    }
}
