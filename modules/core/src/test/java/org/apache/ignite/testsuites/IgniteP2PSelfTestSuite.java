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

import org.apache.ignite.internal.managers.deployment.GridDeploymentMessageCountSelfTest;
import org.apache.ignite.internal.managers.deployment.GridDifferentLocalDeploymentSelfTest;
import org.apache.ignite.internal.managers.deployment.P2PCacheOperationIntoComputeTest;
import org.apache.ignite.internal.managers.deployment.P2PClassLoadingIssuesTest;
import org.apache.ignite.p2p.*;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * P2P test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridP2PDoubleDeploymentSelfTest.class,
    GridP2PHotRedeploymentSelfTest.class,
    GridP2PClassLoadingSelfTest.class,
    GridP2PUndeploySelfTest.class,
    GridP2PRemoteClassLoadersSelfTest.class,
    GridP2PNodeLeftSelfTest.class,
    GridP2PDifferentClassLoaderSelfTest.class,
    GridP2PSameClassLoaderSelfTest.class,
    GridP2PJobClassLoaderSelfTest.class,
    GridP2PRecursionTaskSelfTest.class,
    GridP2PLocalDeploymentSelfTest.class,
    //GridP2PTestTaskExecutionTest.class,
    GridP2PTimeoutSelfTest.class,
    GridP2PMissedResourceCacheSizeSelfTest.class,
    GridP2PContinuousDeploymentSelfTest.class,
    DeploymentClassLoaderCallableTest.class,
    P2PStreamingClassLoaderTest.class,
    SharedDeploymentTest.class,
    P2PScanQueryUndeployTest.class,
    GridDeploymentMessageCountSelfTest.class,
    GridP2PComputeWithNestedEntryProcessorTest.class,
    GridP2PCountTiesLoadClassDirectlyFromClassLoaderTest.class,
    GridP2PScanQueryWithTransformerTest.class,
    P2PCacheOperationIntoComputeTest.class,
    GridDifferentLocalDeploymentSelfTest.class,
    GridP2PContinuousDeploymentClientDisconnectTest.class,
    P2PUnsupportedClassVersionTest.class,
    P2PClassLoadingFailureHandlingTest.class,
    P2PClassLoadingIssuesTest.class,
    P2PCustomSqlFunctionsConfigurationTest.class
})
public class IgniteP2PSelfTestSuite {
}
