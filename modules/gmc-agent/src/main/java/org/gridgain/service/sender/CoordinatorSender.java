package org.gridgain.service.sender;

import org.apache.ignite.internal.GridKernalContext;

import java.util.List;

/**
 * Sender to coordinator.
 */
public class CoordinatorSender<T> extends RetryableSender<T> {
    /** Context. */
    private final GridKernalContext ctx;

    /** Topic name. */
    private final String topicName;

    /**
     * @param ctx Grid kernal context.
     * @param cap Capacity.
     */
    public CoordinatorSender(GridKernalContext ctx, int cap, String topicName) {
        super(cap);
        this.ctx = ctx;
        this.topicName = topicName;
    }

    /** {@inheritDoc} */
    @Override protected void sendInternal(List<T> elements) {
        ctx.grid().message(ctx.grid().cluster().forOldest()).send(topicName, (Object) elements);
    }
}
