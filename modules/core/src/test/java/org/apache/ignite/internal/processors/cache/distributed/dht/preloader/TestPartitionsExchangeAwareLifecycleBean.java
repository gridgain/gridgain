/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Test lifecycle bean which registers {@link PartitionsExchangeAware} listener before the discovery manager is started.
 */
public class TestPartitionsExchangeAwareLifecycleBean implements LifecycleBean {
    /** Ignite instance. */
    @IgniteInstanceResource
    IgniteEx ignite;

    /** User exchange aware listner. */
    private final PartitionsExchangeAware exchangeAware;

    /**
     * Creates a new instance of TestPartitionsExchangeAwareLifecycleBean.
     * @param exchangeAware
     */
    public TestPartitionsExchangeAwareLifecycleBean(PartitionsExchangeAware exchangeAware) {
        this.exchangeAware = exchangeAware;
    }

    /** {@inheritDoc} */
    @Override public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
        if (evt == LifecycleEventType.BEFORE_NODE_START) {
            TestDistributedMetastorageLifecycleListener metastorageLifecycleLsnr = new TestDistributedMetastorageLifecycleListener();

            ignite.context().internalSubscriptionProcessor().registerDistributedMetastorageListener(metastorageLifecycleLsnr);
        }
    }

    /** Test metastorage lifecycle listener. */
    private class TestDistributedMetastorageLifecycleListener implements DistributedMetastorageLifecycleListener {
        /** {@inheritDoc} */
        @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
            ignite.context().cache().context().exchange().registerExchangeAwareComponent(exchangeAware);
        }
    }
}
