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
package org.apache.ignite.testframework.discovery;

import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Matcher to check if given object is expected node id.
 */
public class IsNode<T> extends BaseMatcher<T> {
    /** Expected value. */
    private final UUID nodeId;

    /**
     * @param nodeId Expected value of.
     */
    private IsNode(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override public boolean matches(Object msg) {
        return msg instanceof ClusterNode && ((ClusterNode)msg).id().equals(nodeId);

    }

    /** {@inheritDoc} */
    @Override public void describeTo(Description desc) {
        desc.appendValue("Node(id=" + nodeId + ")");
    }

    /**
     * Expected value.
     *
     * @param nodeId Node id.
     * @param <T> Type of matcher.
     * @return Matcher.
     */
    @Factory
    public static Matcher<ClusterNode> isNode(UUID nodeId) {
        return new IsNode<>(nodeId);
    }
}
