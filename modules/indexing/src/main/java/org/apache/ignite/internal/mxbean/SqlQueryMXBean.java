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

package org.apache.ignite.internal.mxbean;

import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.mxbean.MXBeanParameter;

/**
 * An MX bean allowing to monitor and tune SQL queries.
 *
 * @deprecated Temporary monitoring solution.
 */
@Deprecated
public interface SqlQueryMXBean {
    /**
     * @return Timeout in milliseconds after which long query warning will be printed.
     */
    @MXBeanDescription("Timeout in milliseconds after which long query warning will be printed.")
    long getLongQueryWarningTimeout();

    /**
     * Sets timeout in milliseconds after which long query warning will be printed.
     *
     * @param longQueryWarningTimeout Timeout in milliseconds after which long query warning will be printed.
     */
    @MXBeanDescription("Sets timeout in milliseconds after which long query warning will be printed.")
    void setLongQueryWarningTimeout(
        @MXBeanParameter(name = "longQueryWarningTimeout",
            description = "Timeout in milliseconds after which long query warning will be printed.")
            long longQueryWarningTimeout
    );

    /**
     * @return Long query timeout multiplier.
     */
    @MXBeanDescription("Long query timeout multiplier. The warning will be printed after: timeout, " +
        "timeout * multiplier, timeout * multiplier * multiplier, etc. " +
        "If the multiplier <= 1, the warning message is printed once.")
    int getLongQueryTimeoutMultiplier();

    /**
     * Sets long query timeout multiplier. The warning will be printed after:
     *      - timeout;
     *      - timeout * multiplier;
     *      - timeout * multiplier * multiplier;
     *      - etc.
     * If the multiplier <= 1, the warning message is printed once.
     *
     * @param longQueryTimeoutMultiplier Long query timeout multiplier.
     */
    @MXBeanDescription("Sets long query timeout multiplier. The warning will be printed after: timeout, " +
        "timeout * multiplier, timeout * multiplier * multiplier, etc. " +
        "If the multiplier <= 1, the warning message is printed once.")
    void setLongQueryTimeoutMultiplier(
        @MXBeanParameter(name = "longQueryTimeoutMultiplier", description = "Long query timeout multiplier.")
            int longQueryTimeoutMultiplier
    );

    /**
     * @return Threshold for the number of rows of the result, when count of fetched rows is bigger than the threshold
     *      warning will be printed.
     */
    @MXBeanDescription("Threshold for the number of rows of the result, when count of fetched rows is bigger than the threshold" +
        "warning will be printed.")
    long getResultSetSizeThreshold();

    /**
     * Sets threshold result's row count, when count of fetched rows is bigger than the threshold
     *      warning will be printed.
     *
     * @param rsSizeThreshold Threshold result's row count, when count of fetched rows is bigger than the threshold
     *      warning will be printed.
     */
    @MXBeanDescription("Sets threshold for the number of rows of the result, when count of fetched rows is bigger than the threshold " +
        "warning will be printed")
    void setResultSetSizeThreshold(
        @MXBeanParameter(
            name = "rsSizeThreshold",
            description = "Threshold for the number of rows of the result, when count of fetched rows " +
                "is bigger than the threshold warning will be printed."
        )
        long rsSizeThreshold
    );

    /**
     * Gets result set size threshold multiplier. The warning will be printed after:
     *  - size of result set > threshold;
     *  - size of result set > threshold * multiplier;
     *  - size of result set > threshold * multiplier * multiplier;
     *  - etc.
     * If the multiplier <= 1, the warning message is printed once during query execution and the next one on the query end.
     *
     * @return Result set size threshold multiplier.
     */
    @MXBeanDescription("Gets result set size threshold multiplier. The warning will be printed when size " +
        "of result set is bugger than: threshold, threshold * multiplier, threshold * multiplier * multiplier, " +
        "etc. If the multiplier <= 1, the warning message is printed once during query execution " +
        "and the next one on the query end.")
    int getResultSetSizeThresholdMultiplier();

    /**
     * Sets result set size threshold multiplier.
     *
     * @param rsSizeThresholdMultiplier Result set size threshold multiplier.
     */
    @MXBeanDescription("Sets result set size threshold multiplier. The warning will be printed when size " +
        "of result set is bugger than: threshold, threshold * multiplier, threshold * multiplier * multiplier," +
        "etc. If the multiplier <= 1, the warning message is printed once.")
    void setResultSetSizeThresholdMultiplier(
        @MXBeanParameter(
            name = "rsSizeThresholdMultiplier",
            description = "TResult set size threshold multiplier."
        )
        int rsSizeThresholdMultiplier
    );

    /**
     * Gets global query quota.
     *
     * @return Global query quota as a long.
     */
    @MXBeanDescription("Gets global SQL query memory quota. Global SQL query memory pool size or SQL query memory " +
        "quota is an upper bound for the heap memory part which might be occupied by the SQL query execution engine. " +
        "This quota is shared among all simultaneously running queries, hence it be easily consumed by the single " +
        "heavy analytics query.  Same as SqlGlobalMemoryQuota, but with a Long return type.")
    Long getSqlGlobalMemoryQuotaBytes();

    /**
     * Gets global query quota.
     *
     * @return Global query quota.
     */
    @MXBeanDescription("Gets global SQL query memory quota. Global SQL query memory pool size or SQL query memory " +
        "quota is an upper bound for the heap memory part which might be occupied by the SQL query execution engine. " +
        "This quota is shared among all simultaneously running queries, hence it be easily consumed by the single " +
        "heavy analytics query.")
    String getSqlGlobalMemoryQuota();

    /**
     * Sets global query quota.
     *
     * @param size Size of global memory pool for SQL queries.
     */
    @MXBeanDescription("Sets global query quota. Global SQL query memory pool size or SQL query memory quota is" +
        " an upper bound for the heap memory part which might be occupied by the SQL query execution engine. " +
        "This quota is shared among all simultaneously running queries, hence it be easily consumed by the single " +
        "heavy analytics query. If you want to control memory quota on per-query basis consider sqlQueryMemoryQuota}")
    void setSqlGlobalMemoryQuota(
        @MXBeanParameter(
            name = "size",
            description = "Size of global memory pool for SQL queries in bytes. Can be followed by the" +
                "letters 'k' for kilobytes, 'm' for megabytes, 'g' for gigabytes and '%' for the percentage of " +
                "the current heap. For example:  '1000', '10M', '100k', '1G', '70%'"
        )
        String size
    );


    /**
     * Gets global query quota.
     *
     * @return Global query quota as a long.
     */
    @MXBeanDescription("Global SQL query memory pool size or SQL query memory quota is" +
        " an upper bound for the heap memory part which might be occupied by the SQL query execution engine. " +
        "This quota is shared among all simultaneously running queries, hence it be easily consumed by the single " +
        "heavy analytics query. Same as SqlQueryMemoryQuota, but with a Long return type.")
    Long getSqlQueryMemoryQuotaBytes();

    /**
     * Gets global query quota.
     *
     * @return Global query quota.
     */
    @MXBeanDescription("Global SQL query memory pool size or SQL query memory quota is" +
        " an upper bound for the heap memory part which might be occupied by the SQL query execution engine. " +
        "This quota is shared among all simultaneously running queries, hence it be easily consumed by the single " +
        "heavy analytics query.")
    String getSqlQueryMemoryQuota();

    /**
     * Gets the amount of memory available for SQL queries. This is equal to the {@link #getSqlGlobalMemoryQuotaBytes()} minus the amount of memory currently occupied by the queries.
     *
     * @return Amount of memory bytes available for SQL queries.
     */
    @MXBeanDescription("The amount of memory available for SQL queries. This is equal to SqlGlobalMemoryQuotaBytes minus the amount of memory currently occupied by the queries.")
    long getSqlFreeMemoryBytes();

    /**
     * Sets per-query memory quota.
     *
     * @param size Size of per-query memory quota in bytes, kilobytes, megabytes, or percentage of the max heap.
     */
    @MXBeanDescription("Gets SQL query memory quota. Query memory quota is the maximum amount of memory intended" +
        " for the particular single query execution." +
        " If a query execution exceeds this bound, the either would happen:\n" +
        " If disk offloading is disabled, the query caller gets an error that quota was exceeded. </li>\n" +
        " If disk offloading is enabled, the intermediate query results will be offloaded to a disk. </li>\n" +
        " See SqlOffloadingEnabled for details")
    void setSqlQueryMemoryQuota(
        @MXBeanParameter(
            name = "size",
            description = "Size of per-query memory pool for SQL queries in bytes. Can be followed by the" +
                "letters 'k' for kilobytes, 'm' for megabytes, 'g' for gigabytes and '%' for the percentage of " +
                "the current heap. For example:  '1000', '10M', '100k', '1G', '70%'"
        )
        String size
    );

    /**
     * Gets offloading flag.
     *
     * @return Flag whether query disk offloading is enabled.
     */
    @MXBeanDescription("Offloading flag specifies the query execution behavior on either global or query memory " +
        "quota excess. If flag is set to 'true', the query result will be offloaded to the disk. " +
        "If flag is set to 'false', an exception will be thrown.")
    boolean isSqlOffloadingEnabled();

    /**
     * Sets offloading flag.
     *
     * @param enabled The value whether offloading flag is enabled.
     */
    @MXBeanDescription("Offloading flag specifies the query execution behavior on either global or query memory " +
        "quota excess. If flag is set to 'true', the query result will be offloaded to disk. If flag is set to 'false', " +
        "an exception will be thrown.")
    void setSqlOffloadingEnabled(
        @MXBeanParameter(name = "enabled", description = "The value whether offloading flag is enabled.")
        boolean enabled
    );
}
