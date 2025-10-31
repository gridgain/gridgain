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

package org.apache.ignite.spi.systemview.view;

import java.util.UUID;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryHandler;
import org.apache.ignite.internal.processors.continuous.GridContinuousHandler;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.RoutineInfo;

import static org.apache.ignite.internal.util.IgniteUtils.toStringSafe;

/**
 * Continuous query representation for a {@link SystemView}.
 */
public class ContinuousQueryView {
    /** Routine info. */
    private final RoutineInfo qry;

    /** Continuous query handler. */
    private final GridContinuousHandler hnd;

    /** Routine id. */
    private final UUID routineId;

    /** Node consistent ID */
    private final String nodeConsistentId;

    /** Class name of the local listener */
    private final String locLsnr;

    /** Class name of the local transformed listener */
    private String localTransformedListener;

    /**
     * @param routineId Routine id.
     * @param qry Query info.
     */
    public ContinuousQueryView(UUID routineId, RoutineInfo qry, String nodeConsistentId) {
        this.qry = qry;
        this.hnd = qry.handler();
        this.routineId = routineId;
        this.nodeConsistentId = nodeConsistentId;

        CacheContinuousQueryHandler hnd0 = cacheHandler();

        if (hnd0 == null || hnd0.localListener() == null)
            this.locLsnr = null;
        else
            this.locLsnr = toStringSafe(hnd0.localListener());

        if (hnd0 == null || hnd0.localTransformedEventListener() == null)
            this.localTransformedListener = null;
        else
            this.localTransformedListener = toStringSafe(hnd0.localTransformedEventListener());
    }

    /** @return Continuous query id. */
    public UUID routineId() {
        return routineId;
    }

    /** @return Node id. */
    public UUID nodeId() {
        return qry.nodeId();
    }

    /** @return Cache name. */
    @Order
    public String cacheName() {
        return hnd.cacheName();
    }

    /** @return Topic for continuous query messages. */
    public String topic() {
        return toStringSafe(hnd.orderedTopic());
    }

    /** @return Buffer size. */
    public int bufferSize() {
        return qry.bufferSize();
    }

    /** @return Notify interval. */
    public long interval() {
        return qry.interval();
    }

    /** @return Auto unsubscribe flag value. */
    public boolean autoUnsubscribe() {
        return qry.autoUnsubscribe();
    }

    /** @return {@code True} if continuous query registered to receive events. */
    public boolean isEvents() {
        return hnd.isEvents();
    }

    /** @return {@code True} if continuous query registered for messaging. */
    public boolean isMessaging() {
        return hnd.isMessaging();
    }

    /** @return {@code True} if regular continuous query. */
    public boolean isQuery() {
        return hnd.isQuery();
    }

    /**
     * @return {@code True} if {@code keepBinary} mode enabled.
     * @see IgniteCache#withKeepBinary()
     */
    public boolean keepBinary() {
        return hnd.keepBinary();
    }

    /** @return {@code True} if continuous query should receive notification for existing entries. */
    public boolean notifyExisting() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        return hnd0 != null && hnd0.notifyExisting();
    }

    /** @return {@code True} if old value required for listener. */
    public boolean oldValueRequired() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        return hnd0 != null && hnd0.oldValueRequired();
    }

    /** @return Last send time. */
    @Order(5)
    public long lastSendTime() {
        return qry.lastSendTime();
    }

    /** @return Delayed register flag. */
    public boolean delayedRegister() {
        return qry.delayedRegister();
    }

    /**
     * @return Class name of the local transformed listener.
     * @see ContinuousQuery#setLocalListener(CacheEntryUpdatedListener)
     */
    @Order(1)
    public String localListener() {
        return locLsnr;
    }

    /**
     * @return String representation of remote filter.
     * @see ContinuousQuery#setRemoteFilter(CacheEntryEventSerializableFilter)
     * @see ContinuousQuery#setRemoteFilterFactory(Factory)
     */
    @Order(2)
    public String remoteFilter() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        try {
            if (hnd0 == null || hnd0.getEventFilter() == null)
                return null;

            return toStringSafe(hnd0.getEventFilter());
        }
        catch (IgniteCheckedException e) {
            return null;
        }
    }

    /**
     * @return String representation of remote transformer.
     * @see ContinuousQueryWithTransformer
     * @see ContinuousQueryWithTransformer#setRemoteTransformerFactory(Factory)
     */
    @Order(3)
    public String remoteTransformer() {
        CacheContinuousQueryHandler hnd0 = cacheHandler();

        try {
            if (hnd0 == null || hnd0.getTransformer() == null)
                return null;

            return toStringSafe(hnd0.getTransformer());
        }
        catch (IgniteCheckedException e) {
            return null;
        }
    }

    /**
     * @return String representation of local transformed listener.
     * @see ContinuousQueryWithTransformer
     * @see ContinuousQueryWithTransformer#setLocalListener(ContinuousQueryWithTransformer.EventListener)
     */
    @Order(4)
    public String localTransformedListener() {
        return localTransformedListener;
    }

    /**
     * @return String representation of consistent node ID
     * */
    @Order(5)
    public String nodeConsistentId() {
        return nodeConsistentId;
    }

    /** */
    private CacheContinuousQueryHandler cacheHandler() {
        if (!(hnd instanceof CacheContinuousQueryHandler))
            return null;

        return (CacheContinuousQueryHandler)hnd;
    }
}
