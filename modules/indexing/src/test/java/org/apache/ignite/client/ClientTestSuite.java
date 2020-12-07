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

package org.apache.ignite.client;

import org.apache.ignite.internal.client.thin.CacheAsyncTest;
import org.apache.ignite.internal.client.thin.ClusterApiTest;
import org.apache.ignite.internal.client.thin.ClusterGroupTest;
import org.apache.ignite.internal.client.thin.ComputeTaskTest;
import org.apache.ignite.internal.client.thin.ReliableChannelTest;
import org.apache.ignite.internal.client.thin.ServicesTest;
import org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessDiscoveryTest;
import org.apache.ignite.internal.client.thin.ThinClientAffinityAwarenessConnectionTest;
import org.apache.ignite.internal.client.thin.ThinClientAffinityAwarenessStableTopologyTest;
import org.apache.ignite.internal.client.thin.ThinClientAffinityAwarenessUnstableTopologyTest;
import org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessResourceReleaseTest;
import org.apache.ignite.internal.client.thin.TimeoutTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Tests for Java thin client.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    ClientConfigurationTest.class,
    ClientCacheConfigurationTest.class,
    ExpiryPolicyTest.class,
    FunctionalTest.class,
    IgniteBinaryTest.class,
    LoadTest.class,
    ReliabilityTest.class,
    ReliabilityTestAsync.class,
    ReliabilityTestPartitionAware.class,
    ReliabilityTestPartitionAwareAsync.class,
    SecurityTest.class,
    FunctionalQueryTest.class,
    IgniteBinaryQueryTest.class,
    SslParametersTest.class,
    ConnectionTest.class,
    ConnectToStartingNodeTest.class,
    AsyncChannelTest.class,
    ComputeTaskTest.class,
    ClusterApiTest.class,
    ClusterGroupTest.class,
    ServicesTest.class,
    ThinClientTxMissingBackupsFailover.class,
    ThinClientAffinityAwarenessConnectionTest.class,
    ThinClientAffinityAwarenessStableTopologyTest.class,
    ThinClientAffinityAwarenessUnstableTopologyTest.class,
    ThinClientPartitionAwarenessResourceReleaseTest.class,
    ThinClientPartitionAwarenessDiscoveryTest.class,
    ReliableChannelTest.class,
    CacheAsyncTest.class,
    TimeoutTest.class
})
public class ClientTestSuite {
    // No-op.
}
