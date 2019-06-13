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
package org.apache.ignite.internal.commandline.ru;

import org.apache.ignite.internal.commandline.argument.CommandArg;

/**
 * {@link RollingUpgradeSubCommands#ENABLE} command arguments.
 */
public enum RollingUpgradeCommandArg implements CommandArg {
    /**
     * Enables strict mode of rolling upgrade.
     */
    FORCE("force");

    /** Option name. */
    private final String name;

    /**
     * @param text Command text.
     * @return Command for the text.
     */
    public static RollingUpgradeCommandArg of(String text) {
        for (RollingUpgradeCommandArg cmd : values()) {
            if (cmd.name().equalsIgnoreCase(text))
                return cmd;
        }

        return null;
    }

    /** */
    RollingUpgradeCommandArg(String name) {
        this.name = name;
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
