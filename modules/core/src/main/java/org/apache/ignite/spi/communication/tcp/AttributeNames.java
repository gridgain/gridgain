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

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.cluster.ClusterNode;

/**
 * This class was created for the refactoring approach. It contains attribute names into a {@link ClusterNode}.
 * It should be remove after global refactoring. The values of these attributes should pass via constructors of appropriate classes.
 * @deprecated fix it in the ticket https://ggsystems.atlassian.net/browse/GG-29546
 */
@Deprecated
public class AttributeNames {
    /** Paired connection. */
    private final String pairedConn;

    /** Shmem port. */
    private final String shmemPort;

    /** Addresses. */
    private final String addrs;

    /** Host names. */
    private final String hostNames;

    /** Externalizable attributes. */
    private final String extAttrs;

    /** Port. */
    private final String port;

    /** Force client to server connections flag. */
    private final String forceClientToSrvConnections;

    /**
     * @param pairedConn Paired connection.
     * @param shmemPort Shmem port.
     * @param addrs Addresses.
     * @param hostNames Host names.
     * @param extAttrs Externalizable attributes.
     * @param port Port.
     * @param forceClientToSrvConnections Force client to server connections flag.
     */
    public AttributeNames(
        String pairedConn,
        String shmemPort,
        String addrs,
        String hostNames,
        String extAttrs,
        String port,
        String forceClientToSrvConnections
    ) {
        this.pairedConn = pairedConn;
        this.shmemPort = shmemPort;
        this.addrs = addrs;
        this.hostNames = hostNames;
        this.extAttrs = extAttrs;
        this.port = port;
        this.forceClientToSrvConnections = forceClientToSrvConnections;
    }

    /**
     * @return Paired connection.
     */
    public String pairedConnection() {
        return pairedConn;
    }

    /**
     * @return Shmem port.
     */
    public String shmemPort() {
        return shmemPort;
    }

    /**
     * @return Externalizable attributes.
     */
    public String externalizableAttributes() {
        return extAttrs;
    }

    /**
     * @return Host names.
     */
    public String hostNames() {
        return hostNames;
    }

    /**
     * @return Addresses.
     */
    public String addresses() {
        return addrs;
    }

    /**
     * @return Port.
     */
    public String port() {
        return port;
    }

    /**
     * @return Force client to server connections flag.
     */
    public String forceClientToServerConnections() {
        return forceClientToSrvConnections;
    }
}
