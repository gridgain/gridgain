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

package org.apache.ignite.internal.processors.platform.client.cluster;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cluster.ClusterGroup;

import java.util.function.Function;

/**
 * Client cluster group projection representation.
 * Decodes a remote projection request from a client node.
 */
public class ClientClusterGroupProjection {
    /** */
    private static final short ATTRIBUTE = 1;

    /** */
    private static final short SERVER_NODES = 2;

    /** Cluster group factory method. */
    private final Function<ClusterGroup, ClusterGroup> clusterGrpFactory;

    /**
     * Constructor.
     *
     * @param clusterGrpFactory Cluster Group builder.
     */
    private ClientClusterGroupProjection(Function<ClusterGroup, ClusterGroup> clusterGrpFactory){
        this.clusterGrpFactory = clusterGrpFactory;
    }

    /**
     * Reads projection from a stream.
     * @param reader Reader.
     * @return Projection.
     */
    public static ClientClusterGroupProjection read(BinaryRawReader reader) {
        if (!reader.readBoolean())
            return new ClientClusterGroupProjection(null);

        Function<ClusterGroup, ClusterGroup> factory = clusterGrp ->
        {
            int cnt = reader.readInt();
            for (int i = 0; i < cnt; i++) {
                short code = reader.readShort();
                switch (code) {
                    case ATTRIBUTE: {
                        String attName = reader.readString();
                        String attrVal = reader.readString();
                        clusterGrp = clusterGrp.forAttribute(attName, attrVal);
                        break;
                    }
                    case SERVER_NODES: {
                        clusterGrp = reader.readBoolean()
                                ? clusterGrp.forServers()
                                : clusterGrp.forClients();
                        break;
                    }
                    default:
                        throw new UnsupportedOperationException("Unknown code: " + code);
                }
            }
            return clusterGrp;
        };
        return new ClientClusterGroupProjection(factory);
    }

    /**
     * Applies projection.
     *
     * @param clusterGrp Source cluster group.
     * @return New cluster group instance with the projection.
     */
    public ClusterGroup apply(ClusterGroup clusterGrp){
        if(clusterGrpFactory == null)
            return clusterGrp;

        return clusterGrpFactory.apply(clusterGrp);
    }
}
