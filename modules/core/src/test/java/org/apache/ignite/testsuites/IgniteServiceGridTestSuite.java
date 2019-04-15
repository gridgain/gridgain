/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.testsuites;

import org.apache.ignite.internal.ComputeJobCancelWithServiceSelfTest;
import org.apache.ignite.internal.processors.service.ClosureServiceClientsNodesTest;
import org.apache.ignite.internal.processors.service.GridServiceClientNodeTest;
import org.apache.ignite.internal.processors.service.GridServiceContinuousQueryRedeployTest;
import org.apache.ignite.internal.processors.service.GridServiceDeploymentCompoundFutureSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceDeploymentExceptionPropagationTest;
import org.apache.ignite.internal.processors.service.GridServicePackagePrivateSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorBatchDeploySelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorMultiNodeConfigSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorMultiNodeSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorProxySelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorSingleNodeSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorStopSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProxyClientReconnectSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProxyNodeStopSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceReassignmentSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceSerializationSelfTest;
import org.apache.ignite.internal.processors.service.IgniteServiceDeployment2ClassLoadersDefaultMarshallerTest;
import org.apache.ignite.internal.processors.service.IgniteServiceDeployment2ClassLoadersJdkMarshallerTest;
import org.apache.ignite.internal.processors.service.IgniteServiceDeployment2ClassLoadersOptimizedMarshallerTest;
import org.apache.ignite.internal.processors.service.IgniteServiceDeploymentClassLoadingDefaultMarshallerTest;
import org.apache.ignite.internal.processors.service.IgniteServiceDeploymentClassLoadingJdkMarshallerTest;
import org.apache.ignite.internal.processors.service.IgniteServiceDeploymentClassLoadingOptimizedMarshallerTest;
import org.apache.ignite.internal.processors.service.IgniteServiceDynamicCachesSelfTest;
import org.apache.ignite.internal.processors.service.IgniteServiceProxyTimeoutInitializedTest;
import org.apache.ignite.internal.processors.service.IgniteServiceReassignmentTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentDiscoveryListenerNotificationOrderTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentNonSerializableStaticConfigurationTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentOnActivationTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentOnClientDisconnectTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentOutsideBaselineTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentProcessIdSelfTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentProcessingOnCoordinatorFailTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentProcessingOnCoordinatorLeftTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentProcessingOnNodesFailTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentProcessingOnNodesLeftTest;
import org.apache.ignite.internal.processors.service.ServiceHotRedeploymentViaDeploymentSpiTest;
import org.apache.ignite.internal.processors.service.ServiceInfoSelfTest;
import org.apache.ignite.internal.processors.service.ServicePredicateAccessCacheTest;
import org.apache.ignite.internal.processors.service.ServiceReassignmentFunctionSelfTest;
import org.apache.ignite.internal.processors.service.SystemCacheNotConfiguredTest;
import org.apache.ignite.services.ServiceThreadPoolSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Contains Service Grid related tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    ComputeJobCancelWithServiceSelfTest.class,
    GridServiceProcessorSingleNodeSelfTest.class,
    GridServiceProcessorMultiNodeSelfTest.class,
    GridServiceProcessorMultiNodeConfigSelfTest.class,
    GridServiceProcessorProxySelfTest.class,
    GridServiceReassignmentSelfTest.class,
    GridServiceClientNodeTest.class,
    GridServiceProcessorStopSelfTest.class,
    ServicePredicateAccessCacheTest.class,
    GridServicePackagePrivateSelfTest.class,
    GridServiceSerializationSelfTest.class,
    GridServiceProxyNodeStopSelfTest.class,
    GridServiceProxyClientReconnectSelfTest.class,
    IgniteServiceReassignmentTest.class,
    IgniteServiceProxyTimeoutInitializedTest.class,
    IgniteServiceDynamicCachesSelfTest.class,
    GridServiceContinuousQueryRedeployTest.class,
    ServiceThreadPoolSelfTest.class,
    GridServiceProcessorBatchDeploySelfTest.class,
    GridServiceDeploymentCompoundFutureSelfTest.class,
    SystemCacheNotConfiguredTest.class,
    ClosureServiceClientsNodesTest.class,
    ServiceDeploymentOnActivationTest.class,
    ServiceDeploymentOutsideBaselineTest.class,

    IgniteServiceDeploymentClassLoadingDefaultMarshallerTest.class,
    IgniteServiceDeploymentClassLoadingJdkMarshallerTest.class,
    IgniteServiceDeploymentClassLoadingOptimizedMarshallerTest.class,
    IgniteServiceDeployment2ClassLoadersDefaultMarshallerTest.class,
    IgniteServiceDeployment2ClassLoadersJdkMarshallerTest.class,
    IgniteServiceDeployment2ClassLoadersOptimizedMarshallerTest.class,

    GridServiceDeploymentExceptionPropagationTest.class,
    ServiceDeploymentProcessingOnCoordinatorLeftTest.class,
    ServiceDeploymentProcessingOnCoordinatorFailTest.class,
    ServiceDeploymentProcessingOnNodesLeftTest.class,
    ServiceDeploymentProcessingOnNodesFailTest.class,
    ServiceDeploymentOnClientDisconnectTest.class,
    ServiceDeploymentDiscoveryListenerNotificationOrderTest.class,
    ServiceDeploymentNonSerializableStaticConfigurationTest.class,
    ServiceReassignmentFunctionSelfTest.class,
    ServiceInfoSelfTest.class,
    ServiceDeploymentProcessIdSelfTest.class,
    ServiceHotRedeploymentViaDeploymentSpiTest.class,
})
public class IgniteServiceGridTestSuite {
}
