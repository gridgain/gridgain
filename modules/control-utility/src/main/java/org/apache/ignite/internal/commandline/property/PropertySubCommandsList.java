/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.commandline.property;

import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.property.subcommands.PropertyGetCommand;
import org.apache.ignite.internal.commandline.property.subcommands.PropertyHelpCommand;
import org.apache.ignite.internal.commandline.property.subcommands.PropertyListCommand;
import org.apache.ignite.internal.commandline.property.subcommands.PropertySetCommand;

/** */
public enum PropertySubCommandsList {
    /** */
    HELP("help", new PropertyHelpCommand()),
    /** */
    LIST("list", new PropertyListCommand()),
    /** */
    GET("get", new PropertyGetCommand()),
    /** */
    SET("set", new PropertySetCommand());

    /** */
    private final String name;

    /** */
    private final Command<?> cmd;

    /** */
    PropertySubCommandsList(String name, Command<?> cmd) {
        this.name = name;
        this.cmd = cmd;
    }

    /** */
    public String text() {
        return name;
    }

    /** */
    public Command<?> command() {
        return cmd;
    }

    /** */
    public static PropertySubCommandsList parse(String name) {
        for (PropertySubCommandsList cmd : values()) {
            if (cmd.name.equalsIgnoreCase(name))
                return cmd;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
