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

package org.apache.ignite.internal;

import java.util.Map;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.mxbean.TransactionMetricsMxBean;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.transactions.TransactionMetrics;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.EMPTY_HISTOGRAM_JSON;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.toJson;

/**
 * Transactions MXBean implementation.
 */
public class TransactionMetricsMxBeanImpl implements TransactionMetricsMxBean {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final TransactionMetrics transactionMetrics;

    /** */
    private final MetricRegistry transactionMetricRegistry;

    /**
     * Create TransactionMetricsMxBeanImpl.
     */
    public TransactionMetricsMxBeanImpl() {
        this(null, null);
    }

    /**
     * @param transactionMetrics Transaction metrics.
     * @param transactionMetricRegistry Metric registry used for transaction metrics.
     */
    public TransactionMetricsMxBeanImpl(TransactionMetrics transactionMetrics, MetricRegistry transactionMetricRegistry) {
        this.transactionMetrics = transactionMetrics;
        this.transactionMetricRegistry = transactionMetricRegistry;
    }

    /** {@inheritDoc} */
    @Override public long commitTime() {
        return transactionMetrics.commitTime();
    }

    /** {@inheritDoc} */
    @Override public long rollbackTime() {
        return transactionMetrics.rollbackTime();
    }

    /** {@inheritDoc} */
    @Override public int txCommits() {
        return transactionMetrics.txCommits();
    }

    /** {@inheritDoc} */
    @Override public int txRollbacks() {
        return transactionMetrics.txRollbacks();
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> getAllOwnerTransactions() {
        return transactionMetrics.getAllOwnerTransactions();
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> getLongRunningOwnerTransactions(int duration) {
        return transactionMetrics.getLongRunningOwnerTransactions(duration);
    }

    /** {@inheritDoc} */
    @Override public long getTransactionsCommittedNumber() {
        return transactionMetrics.getTransactionsCommittedNumber();
    }

    /** {@inheritDoc} */
    @Override public long getTransactionsRolledBackNumber() {
        return transactionMetrics.getTransactionsRolledBackNumber();
    }

    /** {@inheritDoc} */
    @Override public long getTransactionsHoldingLockNumber() {
        return transactionMetrics.getTransactionsHoldingLockNumber();
    }

    /** {@inheritDoc} */
    @Override public long getLockedKeysNumber() {
        return transactionMetrics.getLockedKeysNumber();
    }

    /** {@inheritDoc} */
    @Override public long getOwnerTransactionsNumber() {
        return transactionMetrics.getOwnerTransactionsNumber();
    }

    /** {@inheritDoc} */
    @Override public long getTotalNodeSystemTime() {
        LongMetric totalNodeSystemTime =
            (LongMetric)transactionMetricRegistry.findMetric(GridNearTxLocal.METRIC_TOTAL_SYSTEM_TIME);

        return totalNodeSystemTime == null ? 0L : totalNodeSystemTime.value();
    }

    /** {@inheritDoc} */
    @Override public long getTotalNodeUserTime() {
        LongMetric totalNodeUserTime =
            (LongMetric)transactionMetricRegistry.findMetric(GridNearTxLocal.METRIC_TOTAL_USER_TIME);

        return totalNodeUserTime == null ? 0L : totalNodeUserTime.value();
    }

    /** {@inheritDoc} */
    @Override public String getNodeSystemTimeHistogram() {
        HistogramMetric systemTimeHistogram =
            (HistogramMetric)transactionMetricRegistry.findMetric(GridNearTxLocal.METRIC_SYSTEM_TIME_HISTOGRAM);

        return systemTimeHistogram == null ? EMPTY_HISTOGRAM_JSON : toJson(systemTimeHistogram);
    }

    /** {@inheritDoc} */
    @Override public String getNodeUserTimeHistogram() {
        HistogramMetric userTimeHistogram =
            (HistogramMetric)transactionMetricRegistry.findMetric(GridNearTxLocal.METRIC_USER_TIME_HISTOGRAM);

        return userTimeHistogram == null ? EMPTY_HISTOGRAM_JSON : toJson(userTimeHistogram);
    }
}


