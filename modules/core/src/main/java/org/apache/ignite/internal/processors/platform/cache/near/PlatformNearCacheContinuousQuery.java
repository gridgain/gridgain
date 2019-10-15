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

package org.apache.ignite.internal.processors.platform.cache.near;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformTarget;
import org.apache.ignite.internal.processors.platform.cache.query.PlatformContinuousQuery;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.EventType;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Platform Near Cache update listener query.
 */
public class PlatformNearCacheContinuousQuery {
    /** */
    private static final long serialVersionUID = 0L;

    /** Context. */
    protected final PlatformContext platformCtx;

    /** Pointer to native counterpart; zero if closed. */
    private long ptr;

    /** Cursor to handle filter close. */
    private QueryCursor cursor;

    /** Lock for concurrency control. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param ptr Pointer to native counterpart.
     */
    public PlatformNearCacheContinuousQuery(PlatformContext platformCtx, long ptr) {
        assert ptr != 0L;

        this.platformCtx = platformCtx;
        this.ptr = ptr;
    }

    /**
     * Start query execution.
     *
     * @param cache Cache.
     */
    @SuppressWarnings("unchecked")
    public void start(IgniteCacheProxy cache) throws IgniteCheckedException {
        lock.writeLock().lock();

        try {
            try {
                ContinuousQueryWithTransformer qry = new ContinuousQueryWithTransformer();

                qry.setLocalListener(this);
                qry.setRemoteFilterFactory(this);
                qry.setRemoteTransformerFactory(this);

                cursor = cache.query(qry);
            }
            catch (Exception e) {
                try
                {
                    close0();
                }
                catch (Exception ignored)
                {
                    // Ignore
                }

                throw PlatformUtils.unwrapQueryException(e);
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    public void onUpdated(Iterable evts) throws CacheEntryListenerException {
        lock.readLock().lock();

        try {
            if (ptr == 0)
                throw new CacheEntryListenerException("Failed to notify listener because it has been closed.");

            // TODO: Key-only events
            PlatformUtils.applyContinuousQueryEvents(platformCtx, ptr, evts);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    public boolean evaluate(CacheEntryEvent evt) throws CacheEntryListenerException {
        // New entries (CREATED) do not matter for near cache invalidation.
        return evt.getEventType() != EventType.CREATED;
    }

    /** {@inheritDoc} */
    public void onQueryUnregister() {
        // TODO: Propagate this call similar to the old class
        close();
    }

    /** {@inheritDoc} */
    public void close() {
        lock.writeLock().lock();

        try {
            close0();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Internal close routine.
     */
    private void close0() {
        if (ptr != 0) {
            long ptr0 = ptr;

            ptr = 0;

            if (cursor != null)
                cursor.close();

            platformCtx.gateway().continuousQueryFilterRelease(ptr0);
        }
    }
}