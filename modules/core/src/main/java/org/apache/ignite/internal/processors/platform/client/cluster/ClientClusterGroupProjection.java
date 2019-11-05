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

import java.util.HashMap;
import java.util.Map;

/**
 * Client cluster group projection representation.
 * Decodes a remote client's projection request.
 */
public class ClientClusterGroupProjection {
    /** */
    private static final short ATTRIBUTES = 1;
    /** */
    private static final short SERVER_NODE = 2;

    /** Filter value mappings. */
    private final Map<Short, Object> mappings;

    /**
     * Constructor.
     *
     * @param mappings Mappings.
     */
    private ClientClusterGroupProjection(Map<Short, Object> mappings){
        this.mappings = mappings;
    }

    /**
     * Reads projection from a stream.
     * @param reader Reader.
     * @return Projection.
     */
    public static ClientClusterGroupProjection read(BinaryRawReader reader)
    {
        if(!reader.readBoolean())
            return new ClientClusterGroupProjection(new HashMap<>());

        int cnt = reader.readInt();
        Map<Short, Object> mappings = new HashMap<>(cnt);
        for(int i = 0; i < cnt; i++){
            short code = reader.readShort();
            switch (code){
                case ATTRIBUTES:{
                    HashMap<String, String> attrs = new HashMap<>();
                    int attrsCnt = reader.readInt();
                    for(int j = 0; j < attrsCnt; j++){
                        String attrName = reader.readString();
                        String attrVal = reader.readString();
                        attrs.put(attrName, attrVal);
                    }
                    mappings.put(ATTRIBUTES, attrs);
                    break;
                }
                case SERVER_NODE:{
                    mappings.put(SERVER_NODE, true);
                    break;
                }
                default:
                    throw new IllegalArgumentException("Unknown code: " + code);
            }
        }
        return new ClientClusterGroupProjection(mappings);
    }

    /**
     * Appleis projection.
     *
     * @param clusterGrp Cluster group before projection.
     * @return Cluster group after projection.
     */
    public ClusterGroup Apply(ClusterGroup clusterGrp){
        for (Map.Entry<Short, Object> entry : mappings.entrySet()) {
            short code = entry.getKey();
            switch (code){
                case ATTRIBUTES:{
                    Map<String, String> attrs = (Map<String, String>)entry.getValue();
                    for (Map.Entry<String, String> attr : attrs.entrySet())
                        clusterGrp = clusterGrp.forAttribute(attr.getKey(), attr.getValue());
                    break;
                }
                case SERVER_NODE:{
                    clusterGrp = clusterGrp.forServers();
                    break;
                }
                default:
                    throw new IllegalArgumentException("Unknown code: " + code);
            }
        }
        return clusterGrp;
    }
}
