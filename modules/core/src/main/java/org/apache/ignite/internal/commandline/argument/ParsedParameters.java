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
package org.apache.ignite.internal.commandline.argument;

import java.util.Map;
import org.apache.ignite.IgniteException;

/**
 * Parsed CLI parameters.
 *
 * @param <E> Enum which parameters belong to.
 */
public class ParsedParameters<E extends Enum<E> & CommandArg> {
    /** */
    private final Map<String, CommandParameter<E, ? extends Object>> parametersMap;

    /** */
    private final Map<E,  ? extends Object> parsedParams;

    /** */
    public ParsedParameters(
        Map<String, CommandParameter<E, ? extends Object>> parametersMap, Map<E, ? extends Object> parsedParams) {
        this.parametersMap = parametersMap;
        this.parsedParams = parsedParams;
    }

    /**
     * Get parsed parameter value.
     *
     * @param parameter Parameter configuration.
     * @param <T> Value type.
     * @return Value.
     */
    public <T> T get(CommandParameter<E, T> parameter) {
        Object val = parsedParams.get(parameter.parameter());

        if (val == null)
            return (T) parameter.defaultValueSupplier().get();
        else
            return (T) val;
    }

    /**
     * Get parsed parameter value.
     *
     * @param name Parameter name.
     * @param <T> Value type.
     * @return Value.
     */
    public <T> T get(String name) {
        CommandParameter<E, ? extends Object> param = parametersMap.get(name);

        if (param == null)
            throw new IgniteException("No such paramter: " + name);

        return get((CommandParameter<E, T>) param);
    }
}