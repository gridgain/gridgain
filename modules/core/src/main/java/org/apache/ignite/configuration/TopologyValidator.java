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

package org.apache.ignite.configuration;

import java.io.Serializable;
import java.util.Collection;
import javax.cache.CacheException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Topology validator is used to verify that cluster topology is valid for further cache operations.
 * <h1 class="header">Description</h1>
 * The topology validator is invoked every time the cluster topology changes (either a new node joined
 * or an existing node failed or left).
 * <p>
 * If topology validator is not configured, then the cluster topology is always considered to be valid.
 * <p>
 * Whenever the {@link #validate(Collection)} method returns {@code true}, then the topology is considered valid
 * for a certain cache and all operations on this cache will be allowed to proceed. Otherwise, all update operations
 * on the cache are restricted with the following exceptions:
 * <ul>
 * <li>{@link CacheException} will be thrown for all update operations (put, remove, etc) attempt.</li>
 * <li>{@link IgniteException} will be thrown for the transaction commit attempt.</li>
 * </ul>
 * After returning {@code false} and declaring the topology not valid, the topology validator can return
 * to normal state whenever the next topology change happens.
 * <h2 class="header">Example</h2>
 * The example below shows how a validator can be used to allow cache updates only in case if the cluster
 * topology contains exactly 2 nodes:
 * <pre name="code" class="java">
 * new TopologyValidator() {
 *    public boolean validate(Collection<ClusterNode> nodes) {
 *       return nodes.size() == 2;
 *    }
 * }
 * </pre>
 * <h2 class="header">Configuration</h2>
 * The topology validator can be configured either from code or XML via
 * {@link CacheConfiguration#setTopologyValidator(TopologyValidator)} method.
 */
public interface TopologyValidator extends Serializable {
    /**
     * Validates topology.
     *
     * @param nodes Collection of nodes.
     * @return {@code true} in case topology is valid for specific cache, otherwise {@code false}
     */
    public boolean validate(Collection<ClusterNode> nodes);
}