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

package org.apache.ignite.internal.commandline;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;

/**
 * Iterator over command arguments.
 */
public class CommandArgIterator {
    /** */
    private final Iterator<String> argsIt;

    /** */
    private String peekedArg;

    /**
     * Set of common arguments names and high level command name set.
     */
    private final Set<String> commonArgumentsAndHighLevelCommandSet;

    /**
     * @param argsIt Raw argument iterator.
     * @param commonArgumentsAndHighLevelCommandSet All known subcommands.
     */
    public CommandArgIterator(Iterator<String> argsIt, Set<String> commonArgumentsAndHighLevelCommandSet) {
        this.argsIt = argsIt;
        this.commonArgumentsAndHighLevelCommandSet = commonArgumentsAndHighLevelCommandSet;
    }

    /**
     * @return Returns {@code true} if the iteration has more elements.
     */
    public boolean hasNextArg() {
        return peekedArg != null || argsIt.hasNext();
    }

    /**
     * @return <code>true</code> if there's next argument for subcommand.
     */
    public boolean hasNextSubArg() {
        return hasNextArg() && !isKnownCommandOrOption(peekNextArg());
    }

    /**
     * Extract next argument.
     *
     * @param err Error message.
     * @return Next argument value.
     */
    public String nextArg(String err) {
        if (peekedArg != null) {
            String res = peekedArg;

            peekedArg = null;

            return res;
        }

        if (argsIt.hasNext())
            return argsIt.next();

        throw new IllegalArgumentException(err);
    }

    /**
     * Extract next non command argument value
     * @param argName Name of argument, which values we're trying to extract.
     * @return Next argument value.
     */
    @NotNull
    public String nextArgValue(String argName) {
        return nextToken(
            msgExpectedArgValueButNothing(argName),
            input -> msgExpectedArgValueButGot(argName, input)
        );
    }

    /**
     * Extract next Non Auto Confirmation argument
     * @param errOnNoData Message for error when not enough data to parse.
     * @param unexpectedDataErrMapper Mapper that must provide error message based on unexpected input value.
     * @return Next argument value.
     */
    @NotNull
    public String nextToken(String errOnNoData, Function<String, String> unexpectedDataErrMapper) {
        String next = nextArg(errOnNoData);

        if (isKnownCommandOrOption(next))
            throw new IllegalArgumentException(unexpectedDataErrMapper.apply(next));

        return next;
    }

    /**
     * Parse next argument as specified {@link CommandArg} Enum value
     * @param type {@link CommandArg} Enum class.
     * @param argName Name of argument, which values we're trying to convert to Enum.
     * @return Enum value
     * @throws IllegalArgumentException if no suitable Enum value found
     */
    @NotNull
    public <E extends Enum<E> & CommandArg> E nextCmdArgOrFail(Class<E> type, String argName) {
        String val = nextArgValue(argName);
        return CommandArgUtils.failIfNull(CommandArgUtils.ofArg(type, val), msgExpectedArgValueButGot(argName, val));
    }

    /**
     * Parse next argument as specified Enum value
     * @param type Enum class.
     * @param argName Name of argument, which values we're trying to convert to Enum.
     * @return Enum value
     * @throws IllegalArgumentException if no suitable Enum value found
     */
    @NotNull
    public <E extends Enum<E>> E nextEnumOrFail(Class<E> type, String argName) {
        String val = nextArgValue(argName);
        return CommandArgUtils.failIfNull(CommandArgUtils.ofEnum(type, val), msgExpectedArgValueButGot(argName, val));
    }

    /**
     * Returns the next argument in the iteration, without advancing the iteration.
     *
     * @return Next argument value or {@code null} if no next argument.
     */
    public String peekNextArg() {
        if (peekedArg == null && argsIt.hasNext())
            peekedArg = argsIt.next();

        return peekedArg;
    }

    /**
     * @return Numeric value.
     */
    public long nextNonNegativeLongArg(String argName) {
        long val = nextLongArg(argName);

        if (val < 0)
            throw new IllegalArgumentException("Invalid value for " + argName + ": " + val);

        return val;
    }

    /**
     * @return Numeric value.
     */
    public int nextNonNegativeIntArg(String argName) {
        int val = nextIntArg(argName);

        if (val < 0)
            throw new IllegalArgumentException("Invalid value for " + argName + ": " + val);

        return val;
    }

    /**
     * @return Numeric value.
     */
    public long nextLongArg(String argName) {
        String str = nextArg("Expecting " + argName);

        try {
            return str.startsWith("0x") ? Long.parseLong(str.substring(2), 16) : Long.parseLong(str);
        }
        catch (NumberFormatException ignored) {
            throw new IllegalArgumentException("Invalid value for " + argName + ": " + str);
        }
    }

    /**
     * @return Numeric value.
     */
    public int nextIntArg(String argName) {
        String str = nextArg("Expecting " + argName);

        try {
            return str.startsWith("0x") ? Integer.parseInt(str.substring(2), 16) : Integer.parseInt(str);
        }
        catch (NumberFormatException ignored) {
            throw new IllegalArgumentException("Invalid value for " + argName + ": " + str);
        }
    }

    /**
     * @return Numeric value.
     */
    public byte nextByteArg(String argName) {
        String str = nextArg("Expecting " + argName);

        try {
            byte val = Byte.parseByte(str);

            if (val < 0)
                throw new IllegalArgumentException("Invalid value for " + argName + ": " + val);

            return val;
        }
        catch (NumberFormatException ignored) {
            throw new IllegalArgumentException("Invalid value for " + argName + ": " + str);
        }
    }

    /**
     * @param expectedValDescr description of expected data.
     * @return Set of string parsed from next argument or
     * {@link Collections#emptySet()} if next argument is an option
     */
    public Set<String> nextStringSet(String expectedValDescr) {
        return nextStringSet0(expectedValDescr, false);
    }

    /** @see #nextStringSet0(String, boolean) */
    public Set<String> nextNonEmptyStringSet(String expectedValDescr) {
        return nextStringSet0(expectedValDescr, true);
    }

    /**
     * @param expectedValDescr description of expected data.
     * @param requireNonEmpty  <code>True</code> if exception must be thrown on empty set
     * @return Set of string parsed from next argument
     */
    public Set<String> nextStringSet0(String expectedValDescr, boolean requireNonEmpty) {
        String nextArg = peekNextArg();

        if (isCommandOrOption(nextArg)) {

            if (requireNonEmpty)
                throw new IllegalArgumentException(msgExpectedButGot(expectedValDescr, nextArg));

            return Collections.emptySet();
        }

        nextArg = nextToken(
            msgExpected(expectedValDescr),
            input -> msgExpectedButGot(expectedValDescr, input)
        );

        return parseStringSet(nextArg);
    }

    /**
     * @param string To scan on for string set.
     * @return Set of string parsed from string param.
     */
    @NotNull public Set<String> parseStringSet(String string) {
        Set<String> namesSet = new HashSet<>();

        for (String name : string.split(",")) {
            if (F.isEmpty(name))
                throw new IllegalArgumentException("Non-empty string expected.");

            namesSet.add(name.trim());
        }
        return namesSet;
    }

    /**
     * Parses next value as set of caches names
     * @param cachesArgName Name of argument
     * @param groups        True if we're parsing cache group names
     * @return Set of cache (group) names
     */
    @NotNull
    public Set<String> nextCachesSet0(String cachesArgName, boolean groups) {
        return nextStringSet0(
            "set of cache" + (groups ? "s" : " groups") + " for '" + cachesArgName + "' parameter",
            true
        );
    }

    /** @see #nextCachesSet0(String, boolean) */
    @NotNull
    public Set<String> nextCachesSet(String cachesArgName) {
        return nextCachesSet0(cachesArgName, false);
    }

    /** @see #nextCachesSet0(String, boolean) */
    @NotNull
    public Set<String> nextCacheGroupsSet(String cachesGroupsArgName) {
        return nextCachesSet0(cachesGroupsArgName, true);
    }

    /**
     * @param argName Name of the argument to be parsed.
     * @return UUID value.
     */
    public UUID nextUuidArg(String argName) {
        String str = nextArg("Expecting " + argName);

        try {
            return UUID.fromString(str);
        }
        catch (IllegalArgumentException ignored) {
            throw new IllegalArgumentException("Invalid value for " + argName + " (must be UUID): " + str);
        }
    }

    /**
     * Check if raw arg is command or option.
     *
     * @return {@code true} If raw arg is command, otherwise {@code false}.
     */
    public static boolean isCommandOrOption(String raw) {
        return raw != null && raw.contains("--");
    }

    /**
     * @return <code>true</code> if string value is not a command.
     */
    private boolean isKnownCommandOrOption(String val) {
        return CommandList.of(val) != null
            || commonArgumentsAndHighLevelCommandSet.contains(val)
            || CMD_AUTO_CONFIRMATION.equals(val);
    }

    /** Message template */
    private static String msgExpected(String val) {
        return "Expected " + val;
    }

    /** Message template */
    private static String msgExpectedButNothing(String expectedDataDescription) {
        return msgExpected(expectedDataDescription) + " but no data provided";
    }

    /** Message template */
    private static String msgExpectedArgValueButNothing(String argName) {
        return msgExpectedButNothing("value for '" + argName + "'");
    }

    /** Message template */
    private static String msgExpectedButGot(String expectedDataDescription, String unexpectedInput) {
        return msgExpected(expectedDataDescription) + " but got '" + unexpectedInput + "'";
    }

    /** Message template */
    private static String msgExpectedArgValueButGot(String argName, String unexpectedInput) {
        return msgExpectedButGot("value for '" + argName + "'", unexpectedInput);
    }
}
