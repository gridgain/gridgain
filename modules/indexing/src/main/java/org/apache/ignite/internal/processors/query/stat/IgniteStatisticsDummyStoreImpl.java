package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.IgniteLogger;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

/**
 * Statistics store implementation to log unexpected calls. Returns nulls to any request.
 */
public class IgniteStatisticsDummyStoreImpl implements IgniteStatisticsStore {
    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param logSupplier Log supplier.
     */
    public IgniteStatisticsDummyStoreImpl(Function<Class<?>, IgniteLogger> logSupplier) {
        this.log = logSupplier.apply(IgniteStatisticsDummyStoreImpl.class);
    }

    /** {@inheritDoc} */
    @Override public void clearAllStatistics() {
        if (log.isInfoEnabled())
            log.info("Unable to clear all partition level statistics on non server node.");
    }

    /** {@inheritDoc} */
    @Override public void replaceLocalPartitionsStatistics(
        StatisticsKey key,
        Collection<ObjectPartitionStatisticsImpl> statistics
    ) {
        if (log.isInfoEnabled())
            log.info("Unable to replace partition level statistics on non server node.");
    }

    /** {@inheritDoc} */
    @Override public Collection<ObjectPartitionStatisticsImpl> getLocalPartitionsStatistics(StatisticsKey key) {
        if (log.isInfoEnabled())
            log.info("Unable to get partition level statistics on non server node.");

        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionsStatistics(StatisticsKey key) {
        if (log.isInfoEnabled())
            log.info("Unable to clear partition level statistics on non server node.");
    }

    /** {@inheritDoc} */
    @Override public ObjectPartitionStatisticsImpl getLocalPartitionStatistics(StatisticsKey key, int partId) {
        if (log.isInfoEnabled())
            log.info("Unable to get partition level statistics on non server node.");

        return null;
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionStatistics(StatisticsKey key, int partId) {
        if (log.isInfoEnabled())
            log.info("Unable to clear partition level statistics on non server node.");
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionsStatistics(StatisticsKey key, Collection<Integer> partIds) {
        if (log.isInfoEnabled())
            log.info("Unable to clear partition level statistics on non server node.");
    }

    /** {@inheritDoc} */
    @Override public void saveLocalPartitionStatistics(StatisticsKey key, ObjectPartitionStatisticsImpl statistics) {
        if (log.isInfoEnabled())
            log.info("Unable to save partition level statistics on non server node.");
    }
}
