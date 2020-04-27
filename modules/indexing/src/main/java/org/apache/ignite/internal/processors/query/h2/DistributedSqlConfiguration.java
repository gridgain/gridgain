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
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationLifecycleListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.configuration.distributed.SimpleDistributedProperty;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteInClosure;
import org.h2.util.DateTimeUtils;

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

    /** Disabled SQL functions. */
    private final SimpleDistributedProperty<HashSet<String>> disabledSqlFuncs
        = new SimpleDistributedProperty<>("sql.disabledFunctions");

    /** Value of cluster time zone. */
    private final SimpleDistributedProperty<TimeZone> timeZone = new SimpleDistributedProperty<>("sql.timeZone");

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

                    timeZone.addListener((name, oldTz, newTz) -> {
                        if (!Objects.equals(oldTz, newTz))
                            DateTimeUtils.setTimeZone(newTz);
                    });

                    dispatcher.registerProperties(disabledSqlFuncs, timeZone);
                }

                @Override public void onReadyToWrite() {
                    if (ReadableDistributedMetaStorage.isSupported(ctx)) {
                        setDefaultValue(
                            disabledSqlFuncs,
                            DFLT_DISABLED_FUNCS,
                            log);

                        setTimeZoneDefault(log);
                    }
                    else {
                        log.warning("Distributed metastorage is not supported. " +
                            "All distributed SQL configuration parameters are unavailable.");

                        // Set disabled functions to default.
                        disabledSqlFuncs.localUpdate(null);
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
        Set<String> ret = disabledSqlFuncs.get();

        return ret != null ? ret : DFLT_DISABLED_FUNCS;
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
}
