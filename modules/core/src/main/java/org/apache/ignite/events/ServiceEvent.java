package org.apache.ignite.events;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

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
 * @see EventType#EVT_SERVICE_STARTED
 * @see EventType#EVT_SERVICE_METHOD_INVOKED
 * @see EventType#EVT_SERVICE_METHOD_INVOCATION_FAILED
 * @see EventType#EVT_SERVICE_METHOD_INVOCATION_FAILED_OVER
 * @see EventType#EVT_SERVICE_CANCELLED
 */
public class ServiceEvent extends EventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String svcName;

    /** */
    private String mtdName;


    /** {@inheritDoc} */
    @Override public String shortDisplay() {
        return name() + ": svcName=" + svcName;
    }

    /**
     * No-arg constructor.
     */
    public ServiceEvent() {
        // No-op.
    }

    /**
     * Creates job event with given parameters.
     *
     * @param node Node.
     * @param msg Optional message.
     * @param type Event type.
     */
    public ServiceEvent(ClusterNode node, String msg, int type) {
        super(node, msg, type);
    }

    /**
     * Gets name of the task that triggered the event.
     *
     * @return Name of the task that triggered the event.
     */
    public String svcName() {
        return svcName;
    }

    /**
     * Gets name of task class that triggered this event.
     *
     * @return Name of task class that triggered the event.
     */
    public String mtdName() {
        return mtdName;
    }

    /**
     * Sets name of the task that triggered this event.
     *
     * @param svcName Task name to set.
     */
    public void svcName(String svcName) {
        assert svcName != null;

        this.svcName = svcName;
    }

    /**
     * Sets name of the task class that triggered this event.
     *
     * @param mtdName Task class name to set.
     */
    public void mtdName(String mtdName) {
        this.mtdName = mtdName;
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
