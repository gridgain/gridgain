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

package org.apache.ignite.internal.processors.query.h2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.configuration.distributed.DistributePropertyListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationLifecycleListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.configuration.distributed.SimpleDistributedProperty;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteInClosure;
import org.gridgain.internal.h2.util.DateTimeUtils;

import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.makeUpdateListener;
import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.setDefaultValue;

/**
 * Distributed configuration of the indexing module.
 */
public class DistributedSqlConfiguration {
    /** Property update message. */
    private static final String PROPERTY_UPDATE_MESSAGE =
        "SQL parameter '%s' was changed from '%s' to '%s'";

    /** Default disabled SQL functions. */
    public static final HashSet<String> DFLT_DISABLED_FUNCS = (HashSet<String>)Arrays.stream(new String[] {
        "FILE_READ",
        "FILE_WRITE",
        "CSVWRITE",
        "CSVREAD",
        "MEMORY_FREE",
        "MEMORY_USED",
        "LOCK_MODE",
        "LINK_SCHEMA",
        "SESSION_ID",
        "CANCEL_SESSION"
    }).collect(Collectors.toSet());

    /** Default value of the query timeout. */
    public static final int DFLT_QRY_TIMEOUT = 0;

    /** Disabled SQL functions. */
    private final SimpleDistributedProperty<HashSet<String>> disabledSqlFuncs = new SimpleDistributedProperty<>(
        "sql.disabledFunctions",
        SimpleDistributedProperty::parseStringSet
    );

    /** Value of cluster time zone. */
    private final SimpleDistributedProperty<TimeZone> timeZone = new SimpleDistributedProperty<>(
        "sql.timeZone",
        TimeZone::getTimeZone);

    /** Query timeout. */
    private final SimpleDistributedProperty<Integer> dfltQueryTimeout = new SimpleDistributedProperty<>(
        "sql.defaultQueryTimeout",
        SimpleDistributedProperty::parseNonNegativeInteger
    );

    /**
     * Disable creation Lucene index for String value type by default.
     * See: 'H2TableDescriptor#luceneIdx'.
     */
    private final DistributedBooleanProperty disableCreateLuceneIndexForStringValueType =
        DistributedBooleanProperty.detachedBooleanProperty("sql.disableCreateLuceneIndexForStringValueType");

    /** Context. */
    private final GridKernalContext ctx;

    /**
     * @param ctx Kernal context.
     * @param log Logger.
     */
    public DistributedSqlConfiguration(
        GridKernalContext ctx,
        IgniteLogger log
    ) {
        this.ctx = ctx;

        ctx.internalSubscriptionProcessor().registerDistributedConfigurationListener(
            new DistributedConfigurationLifecycleListener() {
                @Override public void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
                    disabledSqlFuncs.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));
                    dfltQueryTimeout.addListener(makeUpdateListener(PROPERTY_UPDATE_MESSAGE, log));

                    timeZone.addListener((name, oldTz, newTz) -> {
                        if (!Objects.equals(oldTz, newTz))
                            DateTimeUtils.setTimeZone(newTz);
                    });

                    dispatcher.registerProperties(disabledSqlFuncs, timeZone, dfltQueryTimeout);
                    dispatcher.registerProperties(disableCreateLuceneIndexForStringValueType);
                }

                @Override public void onReadyToWrite() {
                    if (ReadableDistributedMetaStorage.isSupported(ctx)) {
                        setDefaultValue(
                            disabledSqlFuncs,
                            DFLT_DISABLED_FUNCS,
                            log);

                        setTimeZoneDefault(log);

                        setDefaultValue(
                            dfltQueryTimeout,
                            (int)ctx.config().getSqlConfiguration().getDefaultQueryTimeout(),
                            log);

                        setDefaultValue(
                            disableCreateLuceneIndexForStringValueType,
                            false,
                            log);
                    }
                    else {
                        log.warning("Distributed metastorage is not supported. " +
                            "All distributed SQL configuration parameters are unavailable.");

                        // Set properties to default.
                        disabledSqlFuncs.localUpdate(null);
                        dfltQueryTimeout.localUpdate((int)ctx.config().getSqlConfiguration().getDefaultQueryTimeout());
                        disableCreateLuceneIndexForStringValueType.localUpdate(false);
                    }
                }
            }
        );
    }

    /**
     * @param log Logger.
     */
    private void setTimeZoneDefault(IgniteLogger log) {
        TimeZone tz = timeZone.get();

        if (tz == null) {
            try {
                timeZone.propagateAsync(null, TimeZone.getDefault())
                    .listen((IgniteInClosure<IgniteInternalFuture<?>>)future -> {
                        if (future.error() != null)
                            log.error("Cannot set default value of '" + timeZone.getName() + '\'', future.error());
                    });
            }
            catch (IgniteCheckedException e) {
                log.error("Cannot initiate setting default value of '" + timeZone.getName() + '\'', e);
            }

        }
        else {
            if (!tz.equals(TimeZone.getDefault())) {
                log.warning("Node time zone is '" + TimeZone.getDefault().getID() + "'. " +
                    "SQL timezone is set up to '" + tz.getID() + '\'');
            }

            DateTimeUtils.setTimeZone(tz);
        }
    }

    /**
     * @return Disabled SQL functions.
     */
    public Set<String> disabledFunctions() {
        return disabledSqlFuncs.getOrDefault(DFLT_DISABLED_FUNCS);
    }

    /**
     * @param disabledFuncs Set of disabled functions.
     * @throws IgniteCheckedException if failed.
     */
    public GridFutureAdapter<?> disabledFunctions(HashSet<String> disabledFuncs)
        throws IgniteCheckedException {
        return disabledSqlFuncs.propagateAsync(disabledFuncs);
    }

    /** */
    public void listenDisabledFunctions(DistributePropertyListener<? super HashSet<String>> lsnr) {
        disabledSqlFuncs.addListener(lsnr);
    }

    /**
     * @return Cluster SQL time zone.
     */
    public TimeZone timeZone() {
        assert timeZone.get() != null;

        return timeZone.get();
    }

    /**
     * @param tz New SQL time zone for cluster.
     * @throws IgniteCheckedException if failed.
     */
    public GridFutureAdapter<?> updateTimeZone(TimeZone tz)
        throws IgniteCheckedException {
        return timeZone.propagateAsync(tz);
    }

    /**
     * @return Disabled SQL functions.
     */
    public int defaultQueryTimeout() {
        Integer t = dfltQueryTimeout.get();

        return t != null ? t : DFLT_QRY_TIMEOUT;
    }

    /**
     * @param timeout Default query timeout.
     * @throws IgniteCheckedException if failed.
     */
    public GridFutureAdapter<?> defaultQueryTimeout(int timeout) throws IgniteCheckedException {
        A.ensure(timeout >= 0,
            "default query timeout value must not be negative.");

        return dfltQueryTimeout.propagateAsync(timeout);
    }

    /** */
    public void listenDefaultQueryTimeout(DistributePropertyListener<Integer> lsnr) {
        dfltQueryTimeout.addListener(lsnr);
    }

    /** */
    public boolean isDisableCreateLuceneIndexForStringValueType() {
        Boolean ret = disableCreateLuceneIndexForStringValueType.get();

        return ret != null && ret;
    }

    /** */
    public GridFutureAdapter<?> disableCreateLuceneIndexForStringValueType(boolean disableCreateIdx)
        throws IgniteCheckedException {
        return disableCreateLuceneIndexForStringValueType.propagateAsync(disableCreateIdx);
    }
}
