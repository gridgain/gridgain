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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxPessimisticOriginatingNodeFailureAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test pessimistic tx failures in colocated cache.
 */
public class GridCacheColocatedTxPessimisticOriginatingNodeFailureSelfTest extends
    IgniteTxPessimisticOriginatingNodeFailureAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected Collection<Class<?>> ignoreMessageClasses() {
        return F.asList((Class<?>)GridNearTxPrepareRequest.class, GridDhtTxPrepareRequest.class);
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }
}