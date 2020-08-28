package org.apache.ignite.internal.cluster;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationLifecycleListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DUMP_TX_COLLISIONS_INTERVAL;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_TRANSACTION_TIME_DUMP_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_COEFFICIENT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_PER_SECOND_LIMIT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TX_OWNER_DUMP_REQUESTS_ALLOWED;
import static org.apache.ignite.IgniteSystemProperties.getFloat;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.internal.IgniteKernal.DFLT_LONG_OPERATIONS_DUMP_TIMEOUT;
import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.makeUpdateListener;
import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.setDefaultValue;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty.detachedBooleanProperty;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedDoubleProperty.detachedDoubleProperty;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedIntegerProperty.detachedIntegerProperty;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedLongProperty.detachedLongProperty;

/**
 * Distributed transaction configuration.
 */
public class DistributedTransactionConfiguration {
    /**
     * Property update message.
     */
    private static final String PROPERTY_UPDATE_MESSAGE =
        "Transactions parameter '%s' was changed from '%s' to '%s'";

    /** */
    private static final long dfltLongOpsDumpTimeout =
        getLong(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, DFLT_LONG_OPERATIONS_DUMP_TIMEOUT);

    /** */
    private static final long dfltLongTransactionTimeDumpThreshold =
        getLong(IGNITE_LONG_TRANSACTION_TIME_DUMP_THRESHOLD, 0);

    /**
     * The coefficient for samples of completed transactions that will be dumped in log.
     */
    private static final double dfltTransactionTimeDumpSamplesCoefficient =
        getFloat(IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_COEFFICIENT, 0.0f);

    /**
     * The limit of samples of completed transactions that will be dumped in log per second, if
     * {@link #transactionTimeDumpSamplesCoefficient} is above <code>0.0</code>. Must be integer value
     * greater than <code>0</code>.
     */
    private static final int dfltLongTransactionTimeDumpSamplesPerSecondLimit =
        getInteger(IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_PER_SECOND_LIMIT, 5);

    /** Collisions dump interval. */
    private static final int dfltCollisionsDumpInterval =
        IgniteSystemProperties.getInteger(IGNITE_DUMP_TX_COLLISIONS_INTERVAL, 1000);

    /**
     * Shows if dump requests from local node to near node are allowed, when long running transaction
     * is found. If allowed, the compute request to near node will be made to get thread dump of transaction
     * owner thread.
     */
    private static final boolean dfltTxOwnerDumpRequestsAllowed =
        IgniteSystemProperties.getBoolean(IGNITE_TX_OWNER_DUMP_REQUESTS_ALLOWED, true);

    /** */
    private final DistributedChangeableProperty<Long> txTimeoutOnPartMapExchange =
        detachedLongProperty("txTimeoutOnPartitionMapExchange");

    /** */
    private final DistributedChangeableProperty<Boolean> txOwnerDumpRequestsAllowed =
        detachedBooleanProperty("txOwnerDumpRequestsAllowed");

    /** Long operations dump timeout. */
    private final DistributedChangeableProperty<Long> longOperationsDumpTimeout =
        detachedLongProperty("longOperationsDumpTimeout");

    /**
     * Threshold timeout for long transactions, if transaction exceeds it, it will be dumped in log with
     * information about how much time did it spent in system time (time while aquiring locks, preparing,
     * commiting, etc) and user time (time when client node runs some code while holding transaction and not
     * waiting it). Equals 0 if not set. No transactions are dumped in log if this parameter is not set.
     */
    private final DistributedChangeableProperty<Long> longTransactionTimeDumpThreshold =
        detachedLongProperty("longTransactionTimeDumpThreshold");

    /** */
    private final DistributedChangeableProperty<Double> transactionTimeDumpSamplesCoefficient =
        detachedDoubleProperty("transactionTimeDumpSamplesCoefficient");

    /** */
    private final DistributedChangeableProperty<Integer> longTransactionTimeDumpSamplesPerSecondLimit =
        detachedIntegerProperty("longTransactionTimeDumpSamplesPerSecondLimit");

    /** */
    private final DistributedChangeableProperty<Integer> collisionsDumpInterval =
        detachedIntegerProperty("collisionsDumpInterval");

    /**
     * @param ctx Kernal context.
     * @param log log.
     */
    public DistributedTransactionConfiguration(
        GridKernalContext ctx,
        IgniteLogger log
    ) {
        ctx.internalSubscriptionProcessor().registerDistributedConfigurationListener(
            new DistributedConfigurationLifecycleListener() {
                @Override public void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
                    txTimeoutOnPartMapExchange.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));
                    txOwnerDumpRequestsAllowed.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));
                    longOperationsDumpTimeout.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));
                    longTransactionTimeDumpThreshold.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));
                    transactionTimeDumpSamplesCoefficient.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));
                    longTransactionTimeDumpSamplesPerSecondLimit.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));
                    collisionsDumpInterval.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));

                    dispatcher.registerProperties(txTimeoutOnPartMapExchange, txOwnerDumpRequestsAllowed,
                        longOperationsDumpTimeout, longTransactionTimeDumpThreshold, transactionTimeDumpSamplesCoefficient,
                        longTransactionTimeDumpSamplesPerSecondLimit, collisionsDumpInterval);
                }

                @Override public void onReadyToWrite() {
                    setDefaultValue(longOperationsDumpTimeout, dfltLongOpsDumpTimeout, log);
                    setDefaultValue(longTransactionTimeDumpThreshold, dfltLongTransactionTimeDumpThreshold, log);
                    setDefaultValue(transactionTimeDumpSamplesCoefficient, dfltTransactionTimeDumpSamplesCoefficient, log);
                    setDefaultValue(longTransactionTimeDumpSamplesPerSecondLimit, dfltLongTransactionTimeDumpSamplesPerSecondLimit, log);
                    setDefaultValue(collisionsDumpInterval, dfltCollisionsDumpInterval, log);
                    setDefaultValue(txOwnerDumpRequestsAllowed, dfltTxOwnerDumpRequestsAllowed, log);
                }
            }
        );
    }

    /**
     *
     */
    public GridFutureAdapter<?> updateTxTimeoutOnPartitionMapExchangeAsync(long timeout) throws IgniteCheckedException {
        return txTimeoutOnPartMapExchange.propagateAsync(timeout);
    }

    /**
     *
     */
    public void updateTxTimeoutOnPartitionMapExchangeLocal(long timeout) {
        txTimeoutOnPartMapExchange.localUpdate(timeout);
    }

    /**
     *
     */
    public Boolean txTimeoutOnPartitionMapExchange() {
        return txOwnerDumpRequestsAllowed.get();
    }


    /**
     *
     */
    public GridFutureAdapter<?> updateLongOperationsDumpTimeoutAsync(long timeout) throws IgniteCheckedException {
        return longOperationsDumpTimeout.propagateAsync(timeout);
    }

    /**
     *
     */
    public void updateLongOperationsDumpTimeoutLocal(long timeout) {
        longOperationsDumpTimeout.localUpdate(timeout);
    }

    /**
     *
     */
    public Long longOperationsDumpTimeout() {
        return longOperationsDumpTimeout.getOrDefault(dfltLongOpsDumpTimeout);
    }

    /**
     * @param longTransactionTimeDumpThreshold Value of threshold timeout in milliseconds.
     */
    public GridFutureAdapter<?> updateLongTransactionTimeDumpThreshold(long longTransactionTimeDumpThreshold) throws IgniteCheckedException {
        return this.longTransactionTimeDumpThreshold.propagateAsync(longTransactionTimeDumpThreshold);
    }

    /**
     *
     */
    public void updateLongTransactionTimeDumpThresholdLocal(long longTransactionTimeDumpThreshold) {
        this.longTransactionTimeDumpThreshold.localUpdate(longTransactionTimeDumpThreshold);
    }

    /**
     *
     */
    public Long longTransactionTimeDumpThreshold() {
        return longTransactionTimeDumpThreshold.get();
    }

    public GridFutureAdapter<?> updatetransactionTimeDumpSamplesCoefficient(double longTransactionTimeDumpThreshold) throws IgniteCheckedException {
        return this.transactionTimeDumpSamplesCoefficient.propagateAsync(longTransactionTimeDumpThreshold);
    }

    /**
     *
     */
    public void updateTransactionTimeDumpSamplesCoefficientLocal(double longTransactionTimeDumpThreshold) {
        this.transactionTimeDumpSamplesCoefficient.localUpdate(longTransactionTimeDumpThreshold);
    }

    /**
     *
     */
    public Double transactionTimeDumpSamplesCoefficient() {
        return transactionTimeDumpSamplesCoefficient.get();
    }

    public GridFutureAdapter<?> updateLongTransactionTimeDumpSamplesPerSecondLimit(int limit) throws IgniteCheckedException {
        return this.longTransactionTimeDumpSamplesPerSecondLimit.propagateAsync(limit);
    }

    /**
     *
     */
    public void updateLongTransactionTimeDumpSamplesPerSecondLimitLocal(int limit) {
        this.longTransactionTimeDumpSamplesPerSecondLimit.localUpdate(limit);
    }

    /**
     *
     */
    public Integer longTransactionTimeDumpSamplesPerSecondLimit() {
        return longTransactionTimeDumpSamplesPerSecondLimit.get();
    }

    public GridFutureAdapter<?> updateCollisionsDumpInterval(int limit) throws IgniteCheckedException {
        return this.collisionsDumpInterval.propagateAsync(limit);
    }

    /**
     *
     */
    public void updateCollisionsDumpIntervalLocal(int limit) {
        this.collisionsDumpInterval.localUpdate(limit);
    }

    /**
     *
     */
    public Integer collisionsDumpInterval() {
        return collisionsDumpInterval.get();
    }

    public GridFutureAdapter<?> updateTxOwnerDumpRequestsAllowed(boolean allowed) throws IgniteCheckedException {
        return this.txOwnerDumpRequestsAllowed.propagateAsync(allowed);
    }

    /**
     *
     */
    public void updateTxOwnerDumpRequestsAllowedLocal(boolean allowed) {
        this.txOwnerDumpRequestsAllowed.localUpdate(allowed);
    }

    /**
     *
     */
    public Boolean txOwnerDumpRequestsAllowed() {
        return txOwnerDumpRequestsAllowed.get();
    }

}
