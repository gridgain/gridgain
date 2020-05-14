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

package org.apache.ignite.internal.cluster;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DAEMON;

/**
 * Representation of cluster node that either isn't currently present in cluster, or semantically detached.
 * For example nodes returned from {@code BaselineTopology.currentBaseline()} are always considered as
 * semantically detached, even if they are currently present in cluster.
 */
public class DetachedClusterNode implements ClusterNode, Externalizable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringExclude
    private UUID uuid = UUID.randomUUID();

    /** Consistent ID. */
    @GridToStringInclude
    private Object consistentId;

    /** Node attributes. */
    @GridToStringInclude
    private Map<String, Object> attributes;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public DetachedClusterNode() {
    }

    /**
     * @param consistentId Consistent ID.
     * @param attributes Node attributes.
     */
    public DetachedClusterNode(Object consistentId, Map<String, Object> attributes) {
        this.consistentId = consistentId;
        this.attributes = attributes;
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return uuid;
    }

    /** {@inheritDoc} */
    @Override public Object consistentId() {
        return consistentId;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T attribute(String name) {
        return (T)attributes.get(name);
    }

    /** {@inheritDoc} */
    @Override public ClusterMetrics metrics() {
        throw new UnsupportedOperationException("Operation is not supported on DetachedClusterNode");
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> attributes() {
        return attributes;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> addresses() {
        throw new UnsupportedOperationException("Operation is not supported on DetachedClusterNode");
    }

    /** {@inheritDoc} */
    @Override public Collection<String> hostNames() {
        throw new UnsupportedOperationException("Operation is not supported on DetachedClusterNode");
    }

    /** {@inheritDoc} */
    @Override public long order() {
        throw new UnsupportedOperationException("Operation is not supported on DetachedClusterNode");
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        throw new UnsupportedOperationException("Operation is not supported on DetachedClusterNode");
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDaemon() {
        return "true".equalsIgnoreCase(attribute(ATTR_DAEMON));
    }

    /** {@inheritDoc} */
    @Override public boolean isClient() {
        return Boolean.TRUE.equals(attribute(IgniteNodeAttributes.ATTR_CLIENT_MODE));
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(consistentId);

        U.writeMap(out, attributes);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        uuid = UUID.randomUUID();

        consistentId = in.readObject();
        attributes = U.sealMap(U.<String, Object>readMap(in));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DetachedClusterNode.class, this);
    }
}
