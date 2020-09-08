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

package org.apache.ignite.internal.processors.rest.model;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.visor.compute.VisorGatewayTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Helper for build {@link VisorGatewayTask} arguments.
 */
public class VisorGatewayArgument extends HashMap<String, String> {
    /** Used to sent request charset. */
    private static final String CHARSET = StandardCharsets.UTF_8.name();

    /** Latest argument index. */
    private int idx = 3;

    /**
     * Construct helper object.
     *
     * @param cls Class of executed task.
     */
    public VisorGatewayArgument(Class cls) {
        super(F.asMap(
            "cmd", GridRestCommand.EXE.key(),
            "name", VisorGatewayTask.class.getName(),
            "p1", "null",
            "p2", cls.getName()
        ));
    }

    /**
     * Execute task on specified node.
     *
     * @param node Node.
     * @return This helper for chaining method calls.
     */
    public VisorGatewayArgument setNode(ClusterNode node) {
        put("p1", node != null ? node.id().toString() : null);

        return this;
    }

    /**
     * Execute task on specified nodes.
     *
     * @param nodes Collection of nodes.
     * @return This helper for chaining method calls.
     */
    public VisorGatewayArgument setNodes(Collection<ClusterNode> nodes) {
        put("p1", concat(F.transform(nodes, new C1<ClusterNode, UUID>() {
            /** {@inheritDoc} */
            @Override public UUID apply(ClusterNode node) {
                return node.id();
            }
        }).toArray(), ";"));

        return this;
    }

    /**
     * Add custom arguments.
     *
     * @param vals Array of values or {@code null}.
     * @return This helper for chaining method calls.
     */
    public VisorGatewayArgument addArguments(@Nullable Object... vals) {
        if (idx == 3)
            throw new IllegalStateException("Task argument class should be declared before adding of additional arguments. " +
                "Use VisorGatewayArgument.setTaskArgument first");

        if (vals != null && F.isEmpty(vals))
            throw new IllegalArgumentException("Additional arguments should be configured as null or not empty array of arguments");

        if (vals != null) {
            for (Object val : vals)
                put("p" + idx++, String.valueOf(val));
        }
        else
            put("p" + idx++, null);

        return this;
    }

    /**
     * Add task argument class with custom arguments.
     *
     * @param cls Class.
     * @param vals Values.
     * @return This helper for chaining method calls.
     */
    public VisorGatewayArgument setTaskArgument(Class cls, @Nullable Object... vals) {
        if (idx != 3)
            throw new IllegalStateException("Task argument class should be declared before adding of additional arguments");

        put("p" + idx++, cls.getName());

        if (vals != null) {
            for (Object val : vals)
                put("p" + idx++, val != null ? val.toString() : null);
        }
        else
            put("p" + idx++, null);

        return this;
    }

    /**
     * Add collection argument.
     *
     * @param cls Class.
     * @param vals Values.
     * @return This helper for chaining method calls.
     */
    public VisorGatewayArgument addCollectionArgument(Class cls, @Nullable Object... vals) {
        if (idx == 3)
            throw new IllegalStateException("Task argument class should be declared before adding of additional arguments. " +
                "Use VisorGatewayArgument.setTaskArgument first");

        put("p" + idx++, Collection.class.getName());
        put("p" + idx++, cls.getName());
        put("p" + idx++, concat(vals, ";"));

        return this;
    }

    /**
     * Add set argument.
     *
     * @param cls Class.
     * @param vals Values.
     * @return This helper for chaining method calls.
     */
    public VisorGatewayArgument addSetArgument(Class cls, @Nullable Object... vals) {
        if (idx == 3)
            throw new IllegalStateException("Task argument class should be declared before adding of additional argument. " +
                "Use VisorGatewayArgument.setTaskArgument first");

        put("p" + idx++, Set.class.getName());
        put("p" + idx++, cls.getName());
        put("p" + idx++, concat(vals, ";"));

        return this;
    }

    /**
     * Add map argument.
     *
     * @param keyCls Key class.
     * @param valCls Value class.
     * @param map Map.
     */
    public VisorGatewayArgument addMapArgument(Class keyCls, Class valCls, @NotNull Map<?, ?> map) throws UnsupportedEncodingException {
        if (idx == 3)
            throw new IllegalStateException("Task argument class should be declared before adding of additional arguments. " +
                "Use VisorGatewayArgument.setTaskArgument first");

        if (map == null)
            throw new IllegalArgumentException("Map argument should be specified");

        put("p" + idx++, Map.class.getName());
        put("p" + idx++, keyCls.getName());
        put("p" + idx++, valCls.getName());

        SB sb = new SB();

        boolean first = true;

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!first)
                sb.a(";");

            sb.a(entry.getKey());

            if (entry.getValue() != null)
                sb.a("=").a(entry.getValue());

            first = false;
        }

        put("p" + idx++, URLEncoder.encode(sb.toString(), CHARSET));

        return this;
    }

    /**
     * Concat object with delimiter.
     *
     * @param vals Values.
     * @param delim Delimiter.
     */
    private static String concat(Object[] vals, String delim) {
        SB sb = new SB();

        boolean first = true;

        if (vals != null) {
            for (Object val : vals) {
                if (!first)
                    sb.a(delim);

                sb.a(val);

                first = false;
            }
        }
        else
            sb.a(vals);

        return sb.toString();
    }
}
