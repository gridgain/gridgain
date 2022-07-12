/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedLongProperty;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.spi.ExponentialBackoffTimeoutStrategy;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty.detachedBooleanProperty;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedLongProperty.detachedLongProperty;

/**
 * Periodically removes expired entities from caches with {@link CacheConfiguration#isEagerTtl()} flag set.
 */
public class GridCacheSharedTtlCleanupManager extends GridCacheSharedManagerAdapter implements PartitionsExchangeAware {
    /** Ttl cleanup worker thread sleep interval, ms. */
    private final long cleanupWorkerSleepInterval =
        IgniteSystemProperties.getLong("CLEANUP_WORKER_SLEEP_INTERVAL", 500);

    /** Limit of expired entries processed by worker for certain cache in one pass. */
    private static final int CLEANUP_WORKER_ENTRIES_PROCESS_LIMIT = 1000;

    /** Default tombstone limit per cache group. */
    public static final long DEFAULT_TOMBSTONE_LIMIT = Long.MAX_VALUE;

    /** Default minimum tombstone TTL. */
    public static final long DEFAULT_MIN_TOMBSTONE_TTL = 30_000;

    /** */
    public static final String TS_LIMIT = "tombstones.limit";

    /** */
    public static final String TS_TTL = "tombstones.ttl";

    /** */
    public static final String TS_CLEANUP = "tombstones.suspended.cleanup";

    /** */
    public static final String DEFAULT_TOMBSTONE_TTL_PROP = "DEFAULT_TOMBSTONE_TTL";

    /** Cleanup worker. */
    private CleanupWorker cleanupWorker;

    /** Lock on worker thread creation. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Map of registered ttl managers, where the cache id is used as the key. */
    private final Map<Integer, GridCacheTtlManager> mgrs = new ConcurrentHashMap<>();

    /** Tombstones limit per cache group. */
    private DistributedLongProperty tsLimit = detachedLongProperty(TS_LIMIT);

    /** Tombstones TTL. */
    private DistributedLongProperty tsTtl = detachedLongProperty(TS_TTL);

    /** Tombstones suspended cleanup state. */
    private DistributedBooleanProperty tsSuspendedCleanup = detachedBooleanProperty(TS_CLEANUP);

    /** Default tombstone TTL. */
    private long dfltTombstoneTtl;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        dfltTombstoneTtl = IgniteSystemProperties.getLong(DEFAULT_TOMBSTONE_TTL_PROP,
            calcDfltTombstoneTTL(cctx.kernalContext().config()));

        cctx.kernalContext().internalSubscriptionProcessor().registerDistributedConfigurationListener(dispatcher -> {
            tsLimit.addListener((name, oldVal, newVal) -> {
                if (oldVal == null && newVal == null)
                    return;

                U.log(log, "Tombstones limit has been updated [oldVal=" + oldVal + ", newVal=" + newVal + ']');
            });

            dispatcher.registerProperty(tsLimit);
        });

        cctx.kernalContext().internalSubscriptionProcessor().registerDistributedConfigurationListener(dispatcher -> {
            tsTtl.addListener((name, oldVal, newVal) -> {
                if (oldVal == null && newVal == null)
                    return;

                U.log(log, "Tombstones time to live has been updated [oldVal=" + oldVal + ", newVal=" + newVal + ']');
            });

            dispatcher.registerProperty(tsTtl);
        });

        cctx.kernalContext().internalSubscriptionProcessor().registerDistributedConfigurationListener(dispatcher -> {
            tsSuspendedCleanup.addListener((name, oldVal, newVal) -> {
                if (oldVal == null && newVal == null)
                    return;

                if (oldVal == null)
                    oldVal = false;

                if (!oldVal && newVal)
                    U.log(log, "Tombstones cleanup has been disabled");
                else if (oldVal && !newVal)
                    U.log(log, "Tombstones cleanup has been enabled");
            });

            dispatcher.registerProperty(tsSuspendedCleanup);
        });

        cctx.exchange().registerExchangeAwareComponent(this);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        stopCleanupWorker();
    }

    /**
     * Register ttl manager of cache for periodical check on expired entries.
     *
     * @param mgr ttl manager of cache.
     * */
    public void register(GridCacheTtlManager mgr) {
        if (mgrs.isEmpty())
            startCleanupWorker();

        mgrs.put(mgr.context().cacheId(), mgr);
    }

    /**
     * Unregister ttl manager of cache from periodical check on expired entries.
     *
     * @param mgr ttl manager of cache.
     * */
    public void unregister(GridCacheTtlManager mgr) {
        mgrs.remove(mgr.context().cacheId());

        if (mgrs.isEmpty())
            stopCleanupWorker();
    }

    /**
     * @return {@code True} if eager TTL is enabled.
     */
    public boolean eagerTtlEnabled() {
        assert cctx != null : "Manager is not started";

        lock.lock();

        try {
            return cleanupWorker != null;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @return Tombstones limit per cache group.
     */
    public final long tombstonesLimit() {
        return tsLimit.getOrDefault(DEFAULT_TOMBSTONE_LIMIT);
    }

    /**
     * @return Tombstone expiration time.
     */
    public final long tombstoneExpireTime() {
        return U.currentTimeMillis() + tombstoneTTL();
    }

    /**
     * @return Tombstone time to live.
     */
    public final long tombstoneTTL() {
        return tsTtl.getOrDefault(dfltTombstoneTtl);
    }

    /**
     * @return Tombstones cleanup suspension status.
     */
    public final boolean tombstoneCleanupSuspended() {
        return tsSuspendedCleanup.getOrDefault(false);
    }

    /**
     * @return Tombstone TTL distibuted property.
     */
    public DistributedLongProperty tobmstoneTtlProperty() {
        return tsTtl;
    }

    /**
     * @return Tombstone TTL in millisecond, based on failure detection timeout.
     */
    private static long calcDfltTombstoneTTL(IgniteConfiguration cfg) {
        CommunicationSpi commSpi = cfg.getCommunicationSpi();

        long totalTimeout = 0;

        if (commSpi instanceof TcpCommunicationSpi) {
            TcpCommunicationSpi cfg0 = (TcpCommunicationSpi) commSpi;

            totalTimeout = cfg0.failureDetectionTimeoutEnabled() ? cfg0.failureDetectionTimeout() :
                ExponentialBackoffTimeoutStrategy.totalBackoffTimeout(
                    cfg0.getConnectTimeout(),
                    cfg0.getMaxConnectTimeout(),
                    cfg0.getReconnectCount()
                );
        }

        return Math.max((long)(totalTimeout * 1.5), DEFAULT_MIN_TOMBSTONE_TTL);
    }

    /**
     *
     */
    private void startCleanupWorker() {
        lock.lock();

        try {
            if (cleanupWorker != null)
                return;

            cleanupWorker = new CleanupWorker();

            new IgniteThread(cleanupWorker).start();
        }
        finally {
            lock.unlock();
        }
    }

    /**
     *
     */
    private void stopCleanupWorker() {
        lock.lock();

        try {
            if (null != cleanupWorker) {
                U.cancel(cleanupWorker);
                U.join(cleanupWorker, log);

                cleanupWorker = null;
            }
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
        for (GridCacheTtlManager mgr : mgrs.values())
            mgr.blockExpire(fut);
    }

    /** {@inheritDoc} */
    @Override public void onInitAfterTopologyLock(GridDhtPartitionsExchangeFuture fut) {
        for (GridCacheTtlManager mgr : mgrs.values())
            mgr.unblockExpire(fut);
    }

    /**
     * Entry cleanup worker.
     */
    private class CleanupWorker extends GridWorker {
        /**
         * Creates cleanup worker.
         */
        CleanupWorker() {
            super(cctx.igniteInstanceName(), "ttl-cleanup-worker", cctx.logger(GridCacheSharedTtlCleanupManager.class),
                cctx.kernalContext().workersRegistry());
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            try {
                blockingSectionBegin();

                try {
                    cctx.discovery().localJoin();

                    try {
                        cctx.exchange().affinityReadyFuture(AffinityTopologyVersion.ZERO).get();
                    }
                    catch (IgniteCheckedException ex) {
                        if (cctx.kernalContext().isStopping()) {
                            isCancelled.set(true);

                            return; // Node is stopped before affinity has prepared.
                        }

                        throw new IgniteException("Failed to wait for initialization topology [err="
                            + ex.getMessage() + ']', ex);
                    }
                }
                finally {
                    blockingSectionEnd();
                }

                assert !cctx.kernalContext().recoveryMode();

                final AtomicBoolean expiredRemains = new AtomicBoolean();

                while (!isCancelled()) {
                    expiredRemains.set(false);

                    for (Map.Entry<Integer, GridCacheTtlManager> mgr : mgrs.entrySet()) {
                        updateHeartbeat();

                        Integer processedCacheID = mgr.getKey();

                        cctx.database().checkpointReadLock();

                        try {
                            // Need to be sure that the cache to be processed will not be unregistered and,
                            // therefore, stopped during the process of expiration is in progress.
                            mgrs.computeIfPresent(processedCacheID, (id, m) -> {
                                if (m.expire(CLEANUP_WORKER_ENTRIES_PROCESS_LIMIT))
                                    expiredRemains.set(true);

                                return m;
                            });
                        }
                        finally {
                            cctx.database().checkpointReadUnlock();
                        }

                        if (isCancelled())
                            return;
                    }

                    updateHeartbeat();

                    if (!expiredRemains.get())
                        U.sleep(cleanupWorkerSleepInterval);

                    onIdle();
                }
            }
            catch (Throwable t) {
                if (X.hasCause(t, NodeStoppingException.class)) {
                    isCancelled.set(true); // Treat node stopping as valid worker cancellation.

                    return;
                }

                if (!(t instanceof IgniteInterruptedCheckedException || t instanceof InterruptedException)) {
                    if (isCancelled.get())
                        return;

                    err = t;
                }

                throw t;
            }
            finally {
                if (err == null && !isCancelled.get())
                    err = new IllegalStateException("Thread " + name() + " is terminated unexpectedly");

                if (err instanceof OutOfMemoryError)
                    cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    cctx.kernalContext().failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
            }
        }
    }
}
