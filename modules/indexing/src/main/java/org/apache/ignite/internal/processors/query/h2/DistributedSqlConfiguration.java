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

import java.util.Objects;
import java.util.TimeZone;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationLifecycleListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.configuration.distributed.SimpleDistributedProperty;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteInClosure;
import org.h2.util.DateTimeUtils;

/**
 * Distributed configuration of the indexing module.
 */
public class DistributedSqlConfiguration {
    /** */
    private final IgniteLogger log;

    /** Value of cluster time zone. */
    private final SimpleDistributedProperty<TimeZone> timeZone = new SimpleDistributedProperty<>("sqlTimeZone");

    /**
     * @param isp Subscription processor.
     * @param ctx Kernal context.
     */
    public DistributedSqlConfiguration(
        GridInternalSubscriptionProcessor isp,
        GridKernalContext ctx,
        IgniteLogger log
    ) {
        this.log = log;

        isp.registerDistributedConfigurationListener(
            new DistributedConfigurationLifecycleListener() {
                @Override public void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
                    timeZone.addListener((name, oldTz, newTz) -> {
                        if (!Objects.equals(oldTz, newTz))
                            DateTimeUtils.setTimeZone(newTz);
                    });

                    dispatcher.registerProperties(timeZone);
                }

                @Override public void onReadyToWrite() {
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
            }
        );
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
