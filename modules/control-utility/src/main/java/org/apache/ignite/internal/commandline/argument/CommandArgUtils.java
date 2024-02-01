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

import java.util.Collection;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;

/**
 * Utility class for control.sh arguments.
 */
public class CommandArgUtils {
    /**
     * Tries convert {@code text} to one of values {@code enumClass}.
     * @param text Input text.
     * @param enumClass {@link CommandArg} enum class, which values will be used for lookup
     * @param <E> command argument type, must extend {@link CommandArg}
     * @return Converted argument or {@code null} if convert failed.
     *
     * @deprecated name <code>of()</code> is too abstract, consider using {@link #ofArg(Class, String)}
     */
    @Deprecated
    public static <E extends Enum<E> & CommandArg> @Nullable E of(String text, Class<E> enumClass) {
        for (E e : enumClass.getEnumConstants()) {
            if (e.argName().equalsIgnoreCase(text))
                return e;
        }

        return null;
    }

    /**
     * Same as {@link #of(String, Class)} but name that more clearly reflects its purpose
     */
    public static <E extends Enum<E> & CommandArg> @Nullable E ofArg(Class<E> enumClass, @NotNull String text) {
        return of(text, enumClass);
    }

    /**
     * Tries convert {@code text} to one of values {@code enumClass}.
     * @param text Input text.
     * @param enumClass {@link CommandArg} enum class, which values will be used for lookup
     * @param <E> command argument type, must extend {@link CommandArg}
     * @return Converted argument or {@code null} if convert failed.
     */
    public static <E extends Enum<E>> @Nullable E ofEnum(Class<E> enumClass, @NotNull String text) {
        for (E e : enumClass.getEnumConstants()) {
            if (e.name().equalsIgnoreCase(text))
                return e;
        }

        return null;
    }

    /**
     * Returns provided value if it's not <code>null</code> otherwise
     * throws {@link IllegalArgumentException} with specified message
     */
    public static <T> @NotNull T failIfNull(@Nullable T val, String errorMsg) throws IllegalArgumentException {
        if (val == null)
            throw new IllegalArgumentException(errorMsg);

        return val;
    }

    /**
     * Checks that provided cache list doesn't include illegal values (like internal <code>utility</code> cache)
     */
    public static <C extends Collection<String>> C validateCachesArgument(C caches, String operation) {
        if (F.constainsStringIgnoreCase(caches, UTILITY_CACHE_NAME)) {
            throw new IllegalArgumentException(
                operation + " not allowed for '" + UTILITY_CACHE_NAME + "' cache."
            );
        }

        return caches;
    }

    /** Private constructor. */
    private CommandArgUtils() {
        /* No-op. */
    }
}
