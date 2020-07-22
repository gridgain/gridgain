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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class for control.sh arguments.
 */
public class CommandArgUtils {
    /**
     * Tries convert {@code text} to one of values {@code enumClass}.
     * @param text Input test.
     * @param enumClass {@link CommandArg} enum class.
     * @param <E>
     * @return Converted argument or {@code null} if convert failed.
     */
    public static <E extends Enum<E> & CommandArg> @Nullable E of(String text, Class<E> enumClass) {
        for (E e : enumClass.getEnumConstants()) {
            if (e.argName().equalsIgnoreCase(text))
                return e;
        }

        return null;
    }

    public static <E extends Enum> @Nullable E ofString(String text, Class<E> enumClass) {
        for (E e : enumClass.getEnumConstants()) {
            if (e.toString().equalsIgnoreCase(text))
                return e;
        }

        return null;
    }

    /**
     * @param argIter Argument iterator.
     * @param argsCls Args class.
     * @param parameters Parameters.
     */
    public static <E extends Enum<E> & CommandArg> Map<E, Object> parseArgs(
        CommandArgIterator argIter,
        Class<E> argsCls,
        CommandParameterConfig<E> parameters
    ) {
        Set<CommandParameter<E>> neededObligatoryParams = new HashSet<>(parameters.obligatoryParameters());

        Map<E, Object> res = new HashMap<>();

        while (true) {
            String str = argIter.peekNextArg();

            if (str == null)
                break;

            E arg = of(str, argsCls);

            if (arg == null)
                throw new IgniteException("Unexpected parameter: " + arg);

            CommandParameter<E> param = parameters.parametersMap().get(arg);

            assert param != null;

            argIter.nextArg("");

            if (param.valueType() == null)
                res.put(arg, null);
            else {
                switch (param.valueType().getSimpleName()) {
                    case "String":
                        res.put(arg, argIter.nextArg(arg.argName()));

                        break;

                    case "UUID":
                        res.put(arg, argIter.nextUUIDArg(arg.argName()));

                        break;

                    case "Long":
                        res.put(arg, argIter.nextLongArg(arg.argName()));

                        break;

                    case "Set":
                        res.put(arg, argIter.nextStringSet(arg.argName()));

                        break;

                    default:
                        if (param.valueType().isEnum())
                            res.put(arg, ofString(argIter.nextArg(arg.argName()), param.valueType()));
                }

            }

            neededObligatoryParams.remove(param);
        }

        if (!neededObligatoryParams.isEmpty())
            throw new IgniteException("Missing obligatory parameters: " + neededObligatoryParams);

        return res;
    }

    /** Private constructor. */
    private CommandArgUtils() {
        /* No-op. */
    }
}
