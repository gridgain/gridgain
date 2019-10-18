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

package org.gridgain.dto.event;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;

/**
 * Node bean.
 */
public class ClusterNodeBean implements ClusterNode, Externalizable {
    /** Node ID */
    private UUID nodeId;

    /** Consistent ID. */
    private Object consistentId;

    /** */
    public ClusterNodeBean(ClusterNode node) {
        nodeId = node.id();
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override public Object consistentId() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T> T attribute(String name) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ClusterMetrics metrics() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> attributes() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> addresses() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> hostNames() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public long order() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDaemon() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isClient() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeUuid(out, nodeId);

        out.writeObject(String.valueOf(consistentId));
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = U.readUuid(in);

        consistentId = in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterNodeBean.class, this);
    }
}
