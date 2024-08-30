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

import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;

/**
 * Utility class for creating {@link CommandHandler} log messages.
 */
public class CommandLogger {
    /** Indent for help output. */
    public static final String INDENT = "  ";

    /** Double indent for help output. */
    public static final String DOUBLE_INDENT = INDENT + INDENT;

    /**
     * Join input parameters with specified {@code delimiter} between them.
     *
     * @param delimiter Specified delimiter.
     * @param params Other input parameter.
     * @return Joined parameters with specified {@code delimiter}.
     */
    public static <T> String join(String delimiter, T... params) {
        return join(new SB(), "", delimiter, params).toString();
    }

    /**
     * Join input parameters with specified {@code delimiter} between them and append to the end {@code delimiter}.
     *
     * @param sb Specified string builder.
     * @param sbDelimiter Delimiter between {@code sb} and appended {@code param}.
     * @param delimiter Specified delimiter.
     * @param params Other input parameter.
     * @return SB with appended to the end joined parameters with specified {@code delimiter}.
     */
    public static <T> SB join(SB sb, String sbDelimiter, String delimiter, T... params) {
        if (!F.isEmpty(params)) {
            sb.a(sbDelimiter);

            for (Object par : params)
                sb.a(par).a(delimiter);

            sb.setLength(sb.length() - delimiter.length());
        }

        return sb;
    }

    /**
     * Join input parameters with space and wrap optional braces {@code []}.
     *
     * @param params Other input parameter.
     * @return Joined parameters wrapped optional braces.
     */
    public static <T> String optional(Object... params) {
        return join(new SB(), "[", " ", params).a("]").toString();
    }

    /**
     * Concatenates input parameters to single string with OR delimiter {@code |}.
     *
     * @param params Remaining parameters.
     * @return Concatenated string.
     */
    public static String or(Object... params) {
        return join("|", params);
    }

    /**
     * Join input parameters with space and wrap grouping braces {@code ()}.
     *
     * @param params Input parameter.
     * @return Joined parameters wrapped grouped braces.
     */
    public static String grouped(Object... params) {
        return join(new SB(), "(", " ", params).a(")").toString();
    }

    /**
     * Generates readable error message from exception
     * @param e Exception
     * @return error message
     */
    public static String errorMessage(Throwable e) {
        String msg = e.getMessage();

        if (F.isEmpty(msg))
            msg = e.getClass().getName();
        else if (msg.startsWith("Failed to handle request")) {
            int p = msg.indexOf("err=");

            msg = msg.substring(p + 4, msg.length() - 1);
        }

        return msg;
    }

    /**
     * Prints exception messages to log
     *
     * @param exceptions map containing node ids and exceptions
     * @param infoMsg single message to log
     * @param logger Logger to use
     * @return true if errors were printed.
     */
    public static boolean printErrors(Map<UUID, Exception> exceptions, String infoMsg, Logger logger) {
        if (!F.isEmpty(exceptions)) {
            logger.info(infoMsg);

            for (Map.Entry<UUID, Exception> e : exceptions.entrySet()) {
                logger.info(INDENT + "Node ID: " + e.getKey());

                logger.info(INDENT + "Exception message:");
                logger.info(DOUBLE_INDENT + e.getValue().getMessage());
                logger.info("");
            }

            return true;
        }

        return false;
    }
}
