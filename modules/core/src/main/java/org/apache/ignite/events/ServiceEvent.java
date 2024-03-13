/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.events;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * Grid service event.
 * <p>
 * Grid events are used for notification about what happens within the grid. Note that by design Ignite keeps all events
 * generated on the local node locally and it provides APIs for performing a distributed queries across multiple nodes:
 * <ul>
 * <li>
 * {@link org.apache.ignite.IgniteEvents#remoteQuery(org.apache.ignite.lang.IgnitePredicate, long, int...)} -
 * asynchronously querying events occurred on the nodes specified, including remote nodes.
 * </li>
 * <li>
 * {@link org.apache.ignite.IgniteEvents#localQuery(org.apache.ignite.lang.IgnitePredicate, int...)} - querying only
 * local events stored on this local node.
 * </li>
 * <li>
 * {@link org.apache.ignite.IgniteEvents#localListen(org.apache.ignite.lang.IgnitePredicate, int...)} - listening to
 * local grid events (events from remote nodes not included).
 * </li>
 * </ul>
 * User can also wait for events using method {@link org.apache.ignite.IgniteEvents#waitForLocal(org.apache.ignite.lang.IgnitePredicate,
 * int...)}.
 * <h1 class="header">Events and Performance</h1>
 * Note that by default all events in Ignite are enabled and therefore generated and stored by whatever event storage
 * SPI is configured. Ignite can and often does generate thousands events per seconds under the load and therefore it
 * creates a significant additional load on the system. If these events are not needed by the application this load is
 * unnecessary and leads to significant performance degradation.
 * <p>
 * It is <b>highly recommended</b> to enable only those events that your application logic requires by using {@link
 * org.apache.ignite.configuration.IgniteConfiguration#getIncludeEventTypes()} method in Ignite configuration. Note that
 * certain events are required for Ignite's internal operations and such events will still be generated but not stored
 * by event storage SPI if they are disabled in Ignite configuration.
 *
 * @see EventType#EVT_SERVICE_METHOD_EXECUTION_STARTED
 * @see EventType#EVT_SERVICE_METHOD_EXECUTION_FINISHED
 * @see EventType#EVT_SERVICE_METHOD_EXECUTION_FAILED
 */
public class ServiceEvent extends EventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final String svcName;

    /** */
    private final String mtdName;

    /**  */
    private final UUID subjId;

    private final UUID requestId;

    /** {@inheritDoc} */
    @Override public String shortDisplay() {
        return name() + ": svcName=" + svcName;
    }

    /**
     * Creates service event with given parameters.
     *
     * @param node Node that raised this event.
     * @param msg Optional message.
     * @param type Event type.
     * @param svcName Service name.
     * @param mtdName Service method name.
     * @param subjId Security subject ID.
     */
    public ServiceEvent(ClusterNode node, String msg, int type, String svcName,
        @Nullable String mtdName, @Nullable UUID subjId, UUID requestId) {
        super(node, msg, type);
        this.svcName = svcName;
        this.mtdName = mtdName;
        this.subjId = subjId;
        this.requestId = requestId;
    }

    /**
     * Gets name of the service that triggered this event.
     *
     * @return Name of the service that triggered the event.
     */
    public String serviceName() {
        return svcName;
    }

    /**
     * Gets name of service method that triggered this event.
     *
     * @return Name of service method that triggered the event.
     */
    public String methodName() {
        return mtdName;
    }

    /**
     * Gets security subject ID initiated this service event.
     * <p>
     * Subject ID will be set either to node ID or client ID initiated
     * service execution.
     *
     * @return Subject ID.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * Gets service request ID initiated this service event.
     *
     * @return Request ID.
     */
    public UUID requestId() {
        return requestId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceEvent.class, this,
                "nodeId8", U.id8(node().id()),
                "msg", message(),
                "type", name(),
                "tstamp", timestamp());
    }
}

