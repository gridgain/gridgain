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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.util.GridStringBuilder;

import static org.apache.ignite.internal.commandline.argument.CommandArgUtils.of;
import static org.apache.ignite.internal.commandline.argument.CommandArgUtils.ofString;
import static org.apache.ignite.internal.util.IgniteUtils.EMPTY_STRING_ARRAY;
import static org.apache.ignite.internal.util.lang.GridFunc.transform;

/**
 * Parser for command line parameters.
 *
 * @param <E> Enum which parameters belong to.
 */
public class CommandParametersParser<E extends Enum<E> & CommandArg> {
    /** */
    private final Class<E> parametersEnum;

    /** */
    private final Map<String, CommandParameter<E, ? extends Object>> parametersMap;

    /** */
    private final Set<CommandParameter<E, ? extends Object>> neededObligatoryParams;

    /** */
    public CommandParametersParser(Class<E> parametersEnum, List<CommandParameter<E, ? extends Object>> parametersList) {
        this.parametersEnum = parametersEnum;

        Map<String, CommandParameter<E, ? extends Object>> parametersMap = new LinkedHashMap<>();
        Set<CommandParameter<E, ? extends Object>> neededObligatoryParams = new HashSet<>();

        for (CommandParameter<E, ? extends Object> param : parametersList) {
            parametersMap.put(param.parameterName(), param);

            if (!param.isOptional())
                neededObligatoryParams.add(param);
        }

        this.parametersMap = Collections.unmodifiableMap(parametersMap);
        this.neededObligatoryParams = Collections.unmodifiableSet(neededObligatoryParams);
    }

    /**
     * Parses arguments from iterator.
     *
     * @param argIter Args iterator.
     * @return Parsed parameters.
     */
    public ParsedParameters<E> parse(CommandArgIterator argIter) {
        Set<CommandParameter<E, ? extends Object>> neededObligatoryParams = new HashSet<>(this.neededObligatoryParams);

        Map<E, Object> res = new HashMap<>();

        while (true) {
            String str = argIter.peekNextArg();

            if (str == null)
                break;

            E arg = of(str, parametersEnum);

            if (arg == null)
                throw new IgniteException("Unexpected parameter [arg=" + arg + ", argNum=" + argIter.getArgsHaveBeenRead() + "]");

            CommandParameter<E, ? extends Object> param = parametersMap.get(arg.argName());

            assert param != null;

            argIter.nextArg("");

            Object val = null;

            String peekedVal = argIter.peekNextArg();

            if (param.valueType() != null) {
                switch (param.valueType().getSimpleName()) {
                    case "String":
                        val = argIter.nextArg(arg.argName());

                        break;

                    case "UUID":
                        val = argIter.nextUUIDArg(arg.argName());

                        break;

                    case "Long":
                        val = argIter.nextLongArg(arg.argName());

                        break;

                    case "Set":
                        val = argIter.nextStringSet(arg.argName());

                        break;

                    case "Boolean":
                        val = true;

                        break;

                    default:
                        if (param.valueType().isEnum())
                            val = ofString(argIter.nextArg(arg.argName()), (Class) param.valueType());
                }
            }

            res.put(arg, val);

            if (val != null)
                neededObligatoryParams.remove(param);
            else if (neededObligatoryParams.contains(param) && peekedVal != null)
                throw new IllegalArgumentException("Invalid value for parameter [val=" + peekedVal + ", param=" + param + "]");
        }

        if (!neededObligatoryParams.isEmpty())
            throw new IgniteException("Missing obligatory parameters: " + neededObligatoryParams);

        return new ParsedParameters<>(parametersMap, res);
    }

    /**
     * @return Usage string for all possible parameters.
     */
    public String[] paramUsageStrings() {
        return transform(parametersMap.values(), CommandParameter::usage).toArray(EMPTY_STRING_ARRAY);
    }

    /**
     * @return Help text.
     */
    public String helpText() {
        GridStringBuilder sb = new GridStringBuilder("Usage: ");

        for (CommandParameter param : parametersMap.values())
            sb.a(param.usage()).a(" ");

        for (CommandParameter param : parametersMap.values()) {
            Object dfltVal = null;

            try {
                dfltVal = param.defaultValueSupplier().get();
            }
            catch (Exception ignored) {
                /* No op. */
            }

            sb.a("\n\n").a(param.parameterName()).a(": ").a(param.help());

            if (param.isOptional())
                sb.a(" Default value: ").a(dfltVal);
        }

        return sb.toString();
    }
}