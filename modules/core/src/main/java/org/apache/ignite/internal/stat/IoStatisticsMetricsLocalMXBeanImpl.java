/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.stat;

import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.mxbean.IoStatisticsMetricsMXBean;

import static org.apache.ignite.internal.stat.IoStatisticsHolderIndex.HASH_PK_IDX_NAME;

/**
 * JMX bean to expose local node IO statistics.
 */
public class IoStatisticsMetricsLocalMXBeanImpl implements IoStatisticsMetricsMXBean {
    /** IO statistic manager. */
    private IoStatisticsManager statMgr;

    /**
     * @param statMgr IO statistic manager.
     */
    public IoStatisticsMetricsLocalMXBeanImpl(IoStatisticsManager statMgr) {
        this.statMgr = statMgr;
    }

    /** {@inheritDoc} */
    @Override public long getStartTime() {
        return statMgr.startTime().toEpochSecond();
    }

    /** {@inheritDoc} */
    @Override public String getStartTimeLocal() {
        return statMgr.startTime().format(DateTimeFormatter.ISO_DATE_TIME);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        statMgr.reset();
    }

    /** {@inheritDoc} */
    @Override public String getCacheGroupStatistics(String cacheGrpName) {
        return formattedStats(IoStatisticsType.CACHE_GROUP, cacheGrpName, null);
    }

    /** {@inheritDoc} */
    @Override public Long getCacheGroupPhysicalReads(String cacheGrpName) {
        return statMgr.physicalReads(IoStatisticsType.CACHE_GROUP, cacheGrpName, null);
    }

    /** {@inheritDoc} */
    @Override public Long getCacheGroupLogicalReads(String cacheGrpName) {
        return statMgr.logicalReads(IoStatisticsType.CACHE_GROUP, cacheGrpName, null);
    }

    /** {@inheritDoc} */
    @Override public String getIndexStatistics(String cacheGrpName, String idxName) {
        return formattedStats(getIndexStatType(idxName), cacheGrpName, idxName);
    }

    /**
     * @param idxName Name of index
     * @return Type of index statistics.
     */
    private IoStatisticsType getIndexStatType(String idxName) {
        return idxName.equals(HASH_PK_IDX_NAME) ? IoStatisticsType.HASH_INDEX : IoStatisticsType.SORTED_INDEX;
    }

    /**
     * Gets string presentation of IO statistics for given parameters.
     *
     * @param statType Type of statistics.
     * @param name Name of statistics
     * @param subName SubName of statistics.
     * @return String presentation of IO statistics for given parameters.
     */
    private String formattedStats(IoStatisticsType statType, String name, String subName) {
        Map<String, Long> logicalReads = statMgr.logicalReadsMap(statType, name, subName);

        Map<String, Long> physicalReads = statMgr.physicalReadsMap(statType, name, subName);

        String stats = Stream.concat(logicalReads.entrySet().stream(), physicalReads.entrySet().stream())
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.joining(", ", "[", "]"));

        String statInfo = statType.name() + " " + (subName != null ? name + "." + subName : name);

        return statInfo + " " + stats;
    }

    /** {@inheritDoc} */
    @Override public Long getIndexPhysicalReads(String cacheGrpName, String idxName) {
        return statMgr.physicalReads(getIndexStatType(idxName), cacheGrpName, idxName);
    }

    /** {@inheritDoc} */
    @Override public Long getIndexLogicalReads(String cacheGrpName, String idxName) {
        return statMgr.logicalReads(getIndexStatType(idxName), cacheGrpName, idxName);
    }

    /** {@inheritDoc} */
    @Override public Long getIndexLeafLogicalReads(String cacheGrpName, String idxName) {
        Map<String, Long> logicalReads = statMgr.logicalReadsMap(getIndexStatType(idxName), cacheGrpName, idxName);

        return logicalReads.get(IoStatisticsHolderIndex.LOGICAL_READS_LEAF);
    }

    /** {@inheritDoc} */
    @Override public Long getIndexLeafPhysicalReads(String cacheGrpName, String idxName) {
        Map<String, Long> logicalReads = statMgr.physicalReadsMap(getIndexStatType(idxName), cacheGrpName, idxName);

        return logicalReads.get(IoStatisticsHolderIndex.PHYSICAL_READS_LEAF);
    }

    /** {@inheritDoc} */
    @Override public Long getIndexInnerLogicalReads(String cacheGrpName, String idxName) {
        Map<String, Long> logicalReads = statMgr.logicalReadsMap(getIndexStatType(idxName), cacheGrpName, idxName);

        return logicalReads.get(IoStatisticsHolderIndex.LOGICAL_READS_INNER);
    }

    /** {@inheritDoc} */
    @Override public Long getIndexInnerPhysicalReads(String cacheGrpName, String idxName) {
        Map<String, Long> logicalReads = statMgr.physicalReadsMap(getIndexStatType(idxName), cacheGrpName, idxName);

        return logicalReads.get(IoStatisticsHolderIndex.PHYSICAL_READS_INNER);
    }

}
