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

package org.apache.ignite.mxbean;

import java.util.List;

/**
 * MXBean interface that provides access to ODBC\JDBC\Thin client connections.
 */
@MXBeanDescription("MBean that provides information about client connections.")
public interface ClientProcessorMXBean {
    /**
     * Returns list of active connections.
     *
     * @return Sessions.
     */
    @MXBeanDescription("List of client connections.")
    List<String> getConnections();

    /**
     * Drop all active connections.
     */
    @MXBeanDescription("Drop all client connections.")
    void dropAllConnections();

    /**
     * Drops client connection by {@code id}, if exists.
     *
     * @param id connection id.
     * @return {@code True} if connection has been dropped successfully, {@code false} otherwise.
     */
    @MXBeanDescription("Drop client connection by ID.")
    boolean dropConnection(
        @MXBeanParameter(name = "id", description = "Client connection ID.") long id
    );

    /**
     * If sets to {@code true} shows full stack trace otherwise highlevel short error message.
     *
     * @param show Show flag.
     */
    @MXBeanDescription("Show error full stack.")
    void showFullStackOnClientSide(
        @MXBeanParameter(name = "show", description = "Show error full stack.") boolean show
    );

    /**
     * Sets maximum allowed number of active client connections per node.
     * <p>
     * This applies to any connections that use thin client protocol: thin clients, ODBC, thin JDBC connections.
     * The total number of all connections (ODBC+JDBC+thin client) can not exceed this limit.
     * Zero or negative values mean no limit.
     *
     * @param maxConnectionsPerNode Maximum allowed number of active connections per node.
     */
    @MXBeanDescription("Maximum allowed number of active client connections per node. This applies to any " +
        "connection that use thin client protocol: thin clients, ODBC, thin JDBC connections. The total number " +
        "of all connections (ODBC + JDBC + thin client) can not exceed this limit. Zero or negative values mean " +
        "no limit.")
    void setMaxConnectionsPerNode(
        @MXBeanParameter(
            name = "maxConnectionsPerNode",
            description = "Maximum allowed number of active connections per node."
        ) int maxConnectionsPerNode
    );

    /**
     * @return Current maximum allowed number of active client connections per node.
     */
    @MXBeanDescription("Returns a maximum allowed number of active client connections per node. This applies to any " +
        "connection that use thin client protocol: thin clients, ODBC, thin JDBC connections. The total number " +
        "of all connections (ODBC + JDBC + thin client) can not exceed this limit. Zero or negative values mean " +
        "no limit.")
    int getMaxConnectionsPerNode();
}
