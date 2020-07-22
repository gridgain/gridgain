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
package org.apache.ignite.internal.commandline.argument;

import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.commandline.CommandLogger.optional;

public class CommandParameter<E extends Enum<E> & CommandArg> {
    private final E parameter;

    private final Class valueType;

    private final boolean isOptional;

    public CommandParameter(E parameter, boolean isOptional) {
        this(parameter, null, isOptional);
    }

    public CommandParameter(E parameter, Class valueType) {
        this(parameter, valueType, false);
    }

    public CommandParameter(E parameter, Class valueType, boolean isOptional) {
        this.parameter = parameter;
        this.valueType = valueType;
        this.isOptional = isOptional;
    }

    public E commandArgType() {
        return parameter;
    }

    public Class valueType() {
        return valueType;
    }

    public boolean isOptional() {
        return isOptional;
    }

    @Override public String toString() {
        return S.toString(CommandParameter.class, this);
    }

    public String usage() {
        return isOptional ? optional(parameter.argName()) : parameter.argName();
    }
}
