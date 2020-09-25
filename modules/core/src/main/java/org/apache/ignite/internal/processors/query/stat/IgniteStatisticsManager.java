package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.IgniteCheckedException;

import java.util.function.Supplier;

/**
 * TBD
 */
public interface IgniteStatisticsManager {

    void collectObjectStatistics(String schemaName, String objName, String ... colNames) throws IgniteCheckedException;

    /**
     * Collect object
     *
     * @param schemaName schema name.
     * @param objName object to collect statistics by.
     * @param cancelled supplier to test if task cancelled.
     * @param colNames columns to collect statistics by.
     * @throws IgniteCheckedException
     */
    void collectObjectStatistics(String schemaName, String objName, Supplier<Boolean> cancelled, String ... colNames) throws IgniteCheckedException;

    /**
     * Get local statistics by object.
     *
     * @param schemaName schema name.
     * @param objName object to collect statistics by.
     * @return object statistics or {@code null} if there are no available statistics by specified object.
     */
    ObjectStatistics getLocalStatistics(String schemaName, String objName);

    /**
     * Clear object statistics.
     *
     * @param schemaName schema name.
     * @param objName object to collect statistics by.
     * @param colNames columns to remove statistics by.
     */
    void clearObjectStatistics(String schemaName, String objName, String ... colNames);

}
