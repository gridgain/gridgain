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

package org.apache.ignite.testsuites;

import org.apache.ignite.internal.processors.security.ClientReconnectSecurityTest;
import org.apache.ignite.internal.processors.security.IgniteSecurityProcessorTest;
import org.apache.ignite.internal.processors.security.InvalidServerTest;
import org.apache.ignite.internal.processors.security.cache.CacheOperationPermissionCheckTest;
import org.apache.ignite.internal.processors.security.cache.CacheOperationPermissionCreateDestroyCheckTest;
import org.apache.ignite.internal.processors.security.cache.EntryProcessorPermissionCheckTest;
import org.apache.ignite.internal.processors.security.cache.ScanQueryPermissionCheckTest;
import org.apache.ignite.internal.processors.security.cache.closure.CacheLoadRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.cache.closure.EntryProcessorRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.cache.closure.ScanQueryRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.client.AttributeSecurityCheckTest;
import org.apache.ignite.internal.processors.security.client.AdditionalSecurityCheckTest;
import org.apache.ignite.internal.processors.security.client.AdditionalSecurityCheckWithGlobalAuthTest;
import org.apache.ignite.internal.processors.security.client.ThinClientPermissionCheckSecurityTest;
import org.apache.ignite.internal.processors.security.client.ThinClientPermissionCheckTest;
import org.apache.ignite.internal.processors.security.client.ThinClientSecurityContextOnRemoteNodeTest;
import org.apache.ignite.internal.processors.security.client.ThinClientSslPermissionCheckTest;
import org.apache.ignite.internal.processors.security.compute.ComputePermissionCheckTest;
import org.apache.ignite.internal.processors.security.compute.closure.ComputeTaskRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.compute.closure.DistributedClosureRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.compute.closure.ExecutorServiceRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.datastreamer.DataStreamerPermissionCheckTest;
import org.apache.ignite.internal.processors.security.datastreamer.closure.DataStreamerRemoteSecurityContextCheckTest;
import org.apache.ignite.ssl.MultipleSSLContextsTest;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import static org.apache.ignite.internal.IgniteFeatures.IGNITE_SECURITY_PROCESSOR;

/**
 * Security test suite.
 */
@RunWith(Suite.class)
@WithSystemProperty(key = "IGNITE_SECURITY_PROCESSOR", value = "true")
@Suite.SuiteClasses({
    CacheOperationPermissionCheckTest.class,
    CacheOperationPermissionCreateDestroyCheckTest.class,
    DataStreamerPermissionCheckTest.class,
    ScanQueryPermissionCheckTest.class,
    EntryProcessorPermissionCheckTest.class,
    ComputePermissionCheckTest.class,
    ThinClientPermissionCheckTest.class,
    ThinClientPermissionCheckSecurityTest.class,

    DistributedClosureRemoteSecurityContextCheckTest.class,
    ComputeTaskRemoteSecurityContextCheckTest.class,
    ExecutorServiceRemoteSecurityContextCheckTest.class,
    ScanQueryRemoteSecurityContextCheckTest.class,
    EntryProcessorRemoteSecurityContextCheckTest.class,
    DataStreamerRemoteSecurityContextCheckTest.class,
    CacheLoadRemoteSecurityContextCheckTest.class,
    ThinClientPermissionCheckTest.class,
    ThinClientSslPermissionCheckTest.class,
    ThinClientSecurityContextOnRemoteNodeTest.class,
    IgniteSecurityProcessorTest.class,

    MultipleSSLContextsTest.class,
    AdditionalSecurityCheckTest.class,
    AttributeSecurityCheckTest.class,
    AdditionalSecurityCheckWithGlobalAuthTest.class,

    InvalidServerTest.class,
    ClientReconnectSecurityTest.class
})
public class SecurityTestSuite {
    /**
     * Remote security context propagation is disabled by default, we have to enable it to run corresponding tests.
     */
    @BeforeClass
    public static void init() {
        System.setProperty(IGNITE_SECURITY_PROCESSOR.name(), "true");
    }

    /**
     * Clears property after suite run.
     */
    @AfterClass
    public static void cleanUp() {
        System.clearProperty(IGNITE_SECURITY_PROCESSOR.name());
    }
}
