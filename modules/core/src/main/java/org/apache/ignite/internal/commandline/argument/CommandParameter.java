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

import java.util.function.Supplier;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.commandline.CommandLogger.optional;

public class CommandParameter<E extends Enum<E> & CommandArg, T> {
    private final E parameter;

    private final boolean isOptional;

    private final String help;

    private final Class<T> valueType;

    private Supplier<T> dfltValSupplier;

    public CommandParameter(E parameter, String help, boolean isOptional, Class valueType, Supplier<T> dfltValSupplier) {
        this.parameter = parameter;
        this.help = help;
        this.isOptional = isOptional;
        this.valueType = valueType;
        this.dfltValSupplier = dfltValSupplier;
    }

    /** */
    public static <E extends Enum<E> & CommandArg, T> CommandParameter<E, T> optionalArg(
        E parameter, String help, Class<T> type, Supplier<T> dfltValSupplier) {
        return new CommandParameter<E, T>(parameter, help, true, type, dfltValSupplier);
    }

    /** */
    public static <E extends Enum<E> & CommandArg, T> CommandParameter<E, T> mandatoryArg(
        E parameter, String usage, Class<T> type) {
        return new CommandParameter<E, T>(parameter, usage, false, type, null);
    }

    public E parameter() {
        return parameter;
    }

    public String parameterName() {
        return parameter.argName();
    }

    public Class<T> valueType() {
        return valueType;
    }

    public boolean isOptional() {
        return isOptional;
    }

    @Override public String toString() {
        return S.toString(CommandParameter.class, this);
    }

    public String help() {
        return help;
    }

    public String usage() {
        return isOptional ? optional(parameter.argName()) : parameter.argName();
    }

    /** */
    public Supplier<T> defaultValueSupplier() {
        return dfltValSupplier;
    }
}
