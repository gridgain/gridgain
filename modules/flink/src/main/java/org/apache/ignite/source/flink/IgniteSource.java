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

package org.apache.ignite.source.flink;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Flink Ignite source implemented as a RichParallelSourceFunction.
 */
public class IgniteSource extends RichParallelSourceFunction<CacheEvent> {
    /** Serial version uid. */
    private static final long serialVersionUID = 1L;

    /** Logger. */
    private static final Logger log = LoggerFactory.getLogger(IgniteSource.class);

    /** Default max number of events taken from the buffer at once. */
    private static final int DFLT_EVT_BATCH_SIZE = 1;

    /** Default number of milliseconds timeout for event buffer queue operation. */
    private static final int DFLT_EVT_BUFFER_TIMEOUT = 10;

    /** Event buffer. */
    private BlockingQueue<CacheEvent> evtBuf = new LinkedBlockingQueue<>();

    /** Remote Listener id. */
    private UUID rmtLsnrId;

    /** Flag for isRunning state. */
    private volatile boolean isRunning;

    /** Max number of events taken from the buffer at once. */
    private int evtBatchSize = DFLT_EVT_BATCH_SIZE;

    /** Number of milliseconds timeout for event buffer queue operation. */
    private int evtBufTimeout = DFLT_EVT_BUFFER_TIMEOUT;

    /** Local listener. */
    private final TaskLocalListener locLsnr = new TaskLocalListener();

    /** Ignite instance. */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /** Cache name. */
    private final String cacheName;

    /**
     * Sets Ignite instance.
     *
     * @param ignite Ignite instance.
     */
    public void setIgnite(Ignite ignite) {
        this.ignite = ignite;
    }

    /**
     * Sets Event Batch Size.
     *
     * @param evtBatchSize Event Batch Size.
     */
    public void setEvtBatchSize(int evtBatchSize) {
        this.evtBatchSize = evtBatchSize;
    }

    /**
     * Sets Event Buffer timeout.
     *
     * @param evtBufTimeout Event Buffer timeout.
     */
    public void setEvtBufTimeout(int evtBufTimeout) {
        this.evtBufTimeout = evtBufTimeout;
    }

    /**
     * @return Local Task Listener
     */
    TaskLocalListener getLocLsnr() {
        return locLsnr;
    }

    /**
     * Default IgniteSource constructor.
     *
     * @param cacheName Cache name.
     */
    public IgniteSource(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * Starts Ignite source.
     *
     * @param filter User defined filter.
     * @param cacheEvts Converts comma-delimited cache events strings to Ignite internal representation.
     */
    @SuppressWarnings("unchecked")
    public void start(IgnitePredicate<CacheEvent> filter, int... cacheEvts) {
        A.notNull(cacheName, "Cache name");

        TaskRemoteFilter rmtLsnr = new TaskRemoteFilter(cacheName, filter);

        try {
            synchronized (this) {
                if (isRunning)
                    return;

                isRunning = true;

                rmtLsnrId = ignite.events(ignite.cluster().forCacheNodes(cacheName))
                    .remoteListen(locLsnr, rmtLsnr, cacheEvts);
            }
        }
        catch (IgniteException e) {
            log.error("Failed to register event listener!", e);

            throw e;
        }
    }

    /**
     * Transfers data from grid.
     *
     * @param ctx SourceContext.
     */
    @Override public void run(SourceContext<CacheEvent> ctx) {
        List<CacheEvent> evts = new ArrayList<>(evtBatchSize);

        try {
            while (isRunning) {
                // block here for some time if there is no events from source
                CacheEvent firstEvt = evtBuf.poll(1, TimeUnit.SECONDS);

                if (firstEvt != null)
                    evts.add(firstEvt);

                if (evtBuf.drainTo(evts, evtBatchSize) > 0) {
                    synchronized (ctx.getCheckpointLock()) {
                        for (CacheEvent evt : evts)
                            ctx.collect(evt);

                        evts.clear();
                    }
                }
            }
        }
        catch (Exception e) {
            if (X.hasCause(e, InterruptedException.class))
                return; // Executing thread can be interrupted see cancel() javadoc.

            log.error("Error while processing cache event of " + cacheName, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        synchronized (this) {
            if (!isRunning)
                return;

            isRunning = false;

            if (rmtLsnrId != null && ignite != null) {
                ignite.events(ignite.cluster().forCacheNodes(cacheName))
                    .stopRemoteListen(rmtLsnrId);

                rmtLsnrId = null;
            }
        }
    }

    /**
     * Local listener buffering cache events to be further sent to Flink.
     */
    private class TaskLocalListener implements IgniteBiPredicate<UUID, CacheEvent> {
        /** {@inheritDoc} */
        @Override public boolean apply(UUID id, CacheEvent evt) {
            try {
                if (!evtBuf.offer(evt, evtBufTimeout, TimeUnit.MILLISECONDS))
                    log.error("Failed to buffer event {}", evt.name());
            }
            catch (InterruptedException ignored) {
                log.error("Failed to buffer event using local task listener {}", evt.name());

                Thread.currentThread().interrupt(); // Restore interrupt flag.
            }

            return true;
        }
    }
}

