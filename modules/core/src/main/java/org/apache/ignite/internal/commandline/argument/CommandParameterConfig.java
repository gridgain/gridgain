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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.ignite.internal.util.IgniteUtils.capacity;
import static org.apache.ignite.internal.util.lang.GridFunc.transform;

public class CommandParameterConfig<E extends Enum<E> & CommandArg> {
    private final Map<E, CommandParameter<E>> paramsMap;

    private final Set<CommandParameter<E>> obligatoryParameters;

    public CommandParameterConfig(CommandParameter<E>... parameters) {
        Map<E, CommandParameter<E>> paramsMap = new HashMap<>(capacity(parameters.length));

        Set<CommandParameter<E>> obligatoryParameters = new HashSet<>(capacity(parameters.length));

        for (CommandParameter<E> parameter : parameters) {
            paramsMap.put(parameter.commandArgType(), parameter);

            if (!parameter.isOptional())
                obligatoryParameters.add(parameter);
        }

        this.paramsMap = Collections.unmodifiableMap(paramsMap);

        this.obligatoryParameters = Collections.unmodifiableSet(obligatoryParameters);
    }

    public Map<E, CommandParameter<E>> parametersMap() {
        return paramsMap;
    }

    public Set<CommandParameter<E>> obligatoryParameters() {
        return obligatoryParameters;
    }

    public String[] optionsUsage() {
        return transform(paramsMap.values(), CommandParameter::usage).toArray(new String[] { });
    }
}
