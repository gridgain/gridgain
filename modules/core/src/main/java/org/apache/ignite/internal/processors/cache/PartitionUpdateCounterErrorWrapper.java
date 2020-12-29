package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

/**
 * Update counter wrapper with logging capabilities for exceptions.
 */
public class PartitionUpdateCounterErrorWrapper implements PartitionUpdateCounter  {
    /** */
    private final IgniteLogger log;

    /** */
    private final int partId;

    /** */
    private final CacheGroupContext grp;

    /** */
    private final PartitionUpdateCounter delegate;

    /**
     * @param partId Part id.
     * @param delegate Delegate.
     */
    public PartitionUpdateCounterErrorWrapper(int partId, PartitionUpdateCounter delegate) {
        this.partId = partId;
        this.grp = delegate.context();
        this.log = grp.shared().logger(getClass());
        this.delegate = delegate;
    }

    @Override
    public long reserve(long delta) {
        SB sb = new SB();

        sb.a("[op=reserve" +
                ", grpId=" + grp.groupId() +
                ", grp=" + grp.cacheOrGroupName() +
                ", partId=" + partId +
                ", delta=" + delta +
                ", before=" + toString());

        try {
            return delegate.reserve(delta);
        } catch (AssertionError e) {
            U.error(log, sb.a(", after=" + toString() +
                    ']').toString());

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void init(long initUpdCntr, @Nullable byte[] cntrUpdData) {
        delegate.init(initUpdCntr, cntrUpdData);
    }

    /** {@inheritDoc} */
    @Override public long next() {
        return delegate.next();
    }

    /** {@inheritDoc} */
    @Override public long next(long delta) {
        return delegate.next(delta);
    }

    /** {@inheritDoc} */
    @Override
    public void update(long val) throws IgniteCheckedException {
        delegate.update(val);
    }

    /** {@inheritDoc} */
    @Override public boolean update(long start, long delta) {
        return delegate.update(start, delta);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        delegate.reset();
    }

    /** {@inheritDoc} */
    @Override public void updateInitial(long start, long delta) {
        delegate.updateInitial(start, delta);
    }

    /** {@inheritDoc} */
    @Override public GridLongList finalizeUpdateCounters() {
        return delegate.finalizeUpdateCounters();
    }

    /** {@inheritDoc} */
    @Override public void resetInitialCounter() {
        delegate.resetInitialCounter();
    }

    /** {@inheritDoc} */
    @Override public long initial() {
        return delegate.initial();
    }

    /** {@inheritDoc} */
    @Override public long get() {
        return delegate.get();
    }

    /** {@inheritDoc} */
    @Override public long reserved() {
        return delegate.reserved();
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] getBytes() {
        return delegate.getBytes();
    }

    /** {@inheritDoc} */
    @Override public boolean sequential() {
        return delegate.sequential();
    }

    /** {@inheritDoc} */
    @Override public boolean empty() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Iterator<long[]> iterator() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CacheGroupContext context() {
        return delegate.context();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return delegate.toString();
    }
}
