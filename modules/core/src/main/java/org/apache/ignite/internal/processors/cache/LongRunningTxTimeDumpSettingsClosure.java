package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Closure that is sent on all server nodes in order to change configuration parameters
 * of dumping long running transactions' system and user time values.
 */
public class LongRunningTxTimeDumpSettingsClosure implements IgniteRunnable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Long timeoutThreshold;

    /** */
    private final Float samplesPercentage;

    /**
     * Auto-inject Ignite instance
     */
    @IgniteInstanceResource
    private Ignite ignite;

    /** */
    public LongRunningTxTimeDumpSettingsClosure(Long timeoutThreshold, Float samplesPercentage) {
        this.timeoutThreshold = timeoutThreshold;
        this.samplesPercentage = samplesPercentage;
    }

    /** {@inheritDoc} */
    @Override
    public void run() {
        IgniteTxManager tm = ((IgniteEx) ignite)
                .context()
                .cache()
                .context()
                .tm();

        if (timeoutThreshold != null) {
            tm.longTransactionTimeDumpThreshold(timeoutThreshold);
        }

        if (samplesPercentage != null) {
            tm.longTransactionTimeDumpSampleLimit(samplesPercentage);
        }
    }
}
