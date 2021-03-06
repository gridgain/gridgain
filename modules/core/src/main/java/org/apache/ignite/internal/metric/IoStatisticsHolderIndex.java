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

package org.apache.ignite.internal.metric;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Index statistics holder to gather statistics related to concrete index.
 */
public class IoStatisticsHolderIndex implements IoStatisticsHolder {
    /** Display name of hash PK index. */
    public static final String HASH_PK_IDX_NAME = "HASH_PK";

    /** */
    public static final String LOGICAL_READS_LEAF = "LOGICAL_READS_LEAF";

    /** */
    public static final String LOGICAL_READS_INNER = "LOGICAL_READS_INNER";

    /** */
    public static final String PHYSICAL_READS_LEAF = "PHYSICAL_READS_LEAF";

    /** */
    public static final String PHYSICAL_READS_INNER = "PHYSICAL_READS_INNER";

    /** */
    private final LongAdderMetric logicalReadLeafCtr;

    /** */
    private final LongAdderMetric logicalReadInnerCtr;

    /** */
    private final LongAdderMetric physicalReadLeafCtr;

    /** */
    private final LongAdderMetric physicalReadInnerCtr;

    /** */
    private final String grpName;

    /** */
    private final String idxName;

    /** */
    private IoStatisticsHolder statCache;

    /** */
    private IoStatisticsType type;

    /**
     * @param type Type of statistics.
     * @param grpName Group name.
     * @param idxName Index name.
     * @param mmgr Metric manager.
     */
    public IoStatisticsHolderIndex(
        IoStatisticsType type,
        String grpName,
        String idxName,
        IoStatisticsHolder statCache,
        GridMetricManager mmgr) {
        assert grpName != null && idxName != null;

        this.type = type;
        this.grpName = grpName;
        this.idxName = idxName;

        MetricRegistry mreg = mmgr.registry(metricRegistryName());

        mreg.longMetric("startTime", null).value(U.currentTimeMillis());
        mreg.objectMetric("name", String.class, null).value(grpName);
        mreg.objectMetric("indexName", String.class, null).value(idxName);

        logicalReadLeafCtr = mreg.longAdderMetric(LOGICAL_READS_LEAF, null);
        logicalReadInnerCtr = mreg.longAdderMetric(LOGICAL_READS_INNER, null);
        physicalReadLeafCtr = mreg.longAdderMetric(PHYSICAL_READS_LEAF, null);
        physicalReadInnerCtr = mreg.longAdderMetric(PHYSICAL_READS_INNER, null);

        this.statCache = statCache;
    }

    /** {@inheritDoc} */
    @Override public void trackLogicalRead(long pageAddr) {
        IndexPageType idxPageType = PageIO.deriveIndexPageType(pageAddr);

        switch (idxPageType) {
            case INNER:
                logicalReadInnerCtr.increment();

                IoStatisticsQueryHelper.trackLogicalReadQuery(pageAddr);

                break;

            case LEAF:
                logicalReadLeafCtr.increment();

                IoStatisticsQueryHelper.trackLogicalReadQuery(pageAddr);

                break;

            default:
                statCache.trackLogicalRead(pageAddr);

                break;
        }
    }

    /** {@inheritDoc} */
    @Override public void trackPhysicalAndLogicalRead(long pageAddr) {
        IndexPageType idxPageType = PageIO.deriveIndexPageType(pageAddr);

        switch (idxPageType) {
            case INNER:
                logicalReadInnerCtr.increment();
                physicalReadInnerCtr.increment();

                IoStatisticsQueryHelper.trackPhysicalAndLogicalReadQuery(pageAddr);

                break;

            case LEAF:
                logicalReadLeafCtr.increment();
                physicalReadLeafCtr.increment();

                IoStatisticsQueryHelper.trackPhysicalAndLogicalReadQuery(pageAddr);

                break;

            default:
                statCache.trackPhysicalAndLogicalRead(pageAddr);

                break;
        }
    }

    /** {@inheritDoc} */
    @Override public long logicalReads() {
        return logicalReadLeafCtr.value() + logicalReadInnerCtr.value();
    }

    /** {@inheritDoc} */
    @Override public long physicalReads() {
        return physicalReadLeafCtr.value() + physicalReadInnerCtr.value();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> logicalReadsMap() {
        Map<String, Long> res = new HashMap<>(3);

        res.put(LOGICAL_READS_LEAF, logicalReadLeafCtr.value());
        res.put(LOGICAL_READS_INNER, logicalReadInnerCtr.value());

        return res;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> physicalReadsMap() {
        Map<String, Long> res = new HashMap<>(3);

        res.put(PHYSICAL_READS_LEAF, physicalReadLeafCtr.value());
        res.put(PHYSICAL_READS_INNER, physicalReadInnerCtr.value());

        return res;
    }

    /** {@inheritDoc} */
    @Override public void resetStatistics() {
        logicalReadLeafCtr.reset();
        logicalReadInnerCtr.reset();
        physicalReadLeafCtr.reset();
        physicalReadInnerCtr.reset();
    }

    /** {@inheritDoc} */
    @Override public String metricRegistryName() {
        return metricName(type.metricGroupName(), grpName, idxName);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IoStatisticsHolderIndex.class, this,
            "logicalReadLeafCtr", logicalReadLeafCtr,
            "logicalReadInnerCtr", logicalReadInnerCtr,
            "physicalReadLeafCtr", physicalReadLeafCtr,
            "physicalReadInnerCtr", physicalReadInnerCtr,
            "grpName", grpName,
            "idxName", idxName);
    }
}
