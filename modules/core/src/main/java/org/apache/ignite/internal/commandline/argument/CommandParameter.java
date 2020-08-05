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

/**
 * Command line parameter configuration. It is used to build parser configuration.
 *
 * @param <E> Enum type, which parameter belongs to.
 * @param <T> Type of possible parameter's value.
 */
public class CommandParameter<E extends Enum<E> & CommandArg, T> {
    /** */
    private final E parameter;

    /** */
    private final boolean isOptional;

    /** */
    private final String help;

    /** */
    private final Class<T> valueType;

    /** */
    private Supplier<T> dfltValSupplier;

    /** */
    public CommandParameter(E parameter, String help, boolean isOptional, Class valueType, Supplier<T> dfltValSupplier) {
        this.parameter = parameter;
        this.help = help;
        this.isOptional = isOptional;
        this.valueType = valueType;
        this.dfltValSupplier = dfltValSupplier;
    }

    /**
     * Builds configuration of optional parameter.
     *
     * @param parameter Parameter.
     * @param help Help string.
     * @param type Value type.
     * @param dfltValSupplier Supplier of default value for optional parameter.
     * @param <E> Enum type, which parameter belongs to.
     * @param <T> Value type.
     * @return Parameter configuration.
     */
    public static <E extends Enum<E> & CommandArg, T> CommandParameter<E, T> optionalParam(
        E parameter, String help, Class<T> type, Supplier<T> dfltValSupplier) {
        return new CommandParameter<E, T>(parameter, help, true, type, dfltValSupplier);
    }

    /**
     * Builds configuration of mandatory parameter.
     *
     * @param parameter Parameter.
     * @param help Help string.
     * @param type Value type.
     * @param <E> Enum type, which parameter belongs to.
     * @param <T> Value type.
     * @return Parameter configuration.
     */
    public static <E extends Enum<E> & CommandArg, T> CommandParameter<E, T> mandatoryParam(
        E parameter, String help, Class<T> type) {
        return new CommandParameter<E, T>(parameter, help, false, type, null);
    }

    /**
     * @return CLI parameter.
     */
    public E parameter() {
        return parameter;
    }

    /**
     * @return CLI parameter name.
     */
    public String parameterName() {
        return parameter.argName();
    }

    /**
     * @return Value type.
     */
    public Class<T> valueType() {
        return valueType;
    }

    /**
     * @return Whether is optional.
     */
    public boolean isOptional() {
        return isOptional;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CommandParameter.class, this);
    }

    /**
     * @return Help text.
     */
    public String help() {
        return help;
    }

    /**
     * @return String for usage description.
     */
    public String usage() {
        return isOptional ? optional(parameter.argName()) : parameter.argName();
    }

    /**
     * @return Supplier of default value for optional parameter.
     */
    public Supplier<T> defaultValueSupplier() {
        return dfltValSupplier;
    }
}
