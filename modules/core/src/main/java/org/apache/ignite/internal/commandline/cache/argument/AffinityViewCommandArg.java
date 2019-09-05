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

package org.apache.ignite.internal.commandline.cache.argument;

import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.cache.AffinityViewCommand;

/**
 * Argument for {@link AffinityViewCommand}
 */
public enum AffinityViewCommandArg implements CommandArg {
    /** Current. */
    CURRENT("--current"),

    /** Ideal. */
    IDEAL("--ideal"),

    /** Diff. */
    DIFF("--diff"),

    /** Group name. */
    GROUP_NAME("--group-name"),

    /** Affinity source node. */
    SOURCE_NODE_ID("--src-node-id");

    /** Option name. */
    private final String name;

    /**
     * @param name arg name
     */
    AffinityViewCommandArg(String name) {
        this.name = name;
    }

    /**
     * @return {@code name} without "--"
     */
    public String trimmedArgName() {
        if (name.startsWith("--"))
            return name.substring(2);
        else
            return name;
    }

    /** {@inheritDoc} */
    @Override public String argName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
