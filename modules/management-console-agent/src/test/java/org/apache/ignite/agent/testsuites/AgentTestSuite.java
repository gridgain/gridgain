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

package org.apache.ignite.agent.testsuites;

import org.apache.ignite.agent.ManagementConsoleAgentTest;
import org.apache.ignite.agent.ManagementConsoleAgentTest.ManagementConsoleAgentWithMockContextTest;
import org.apache.ignite.agent.action.SessionRegistryTest;
import org.apache.ignite.agent.action.annotation.ActionControllerAnnotationReaderTest;
import org.apache.ignite.agent.action.controller.ActionControllerBaseTest;
import org.apache.ignite.agent.action.controller.ActionControllerWithAuthenticationBaseTest;
import org.apache.ignite.agent.action.controller.BaselineActionsControllerTest;
import org.apache.ignite.agent.action.controller.ClusterActionsControllerTest;
import org.apache.ignite.agent.action.controller.QueryActionsControllerTest;
import org.apache.ignite.agent.action.controller.QueryActionsControllerWithParametersTest;
import org.apache.ignite.agent.action.controller.SecurityActionsControllerTest;
import org.apache.ignite.agent.action.query.QueryRegistryTest;
import org.apache.ignite.agent.dto.IgniteConfigurationWrapperTest;
import org.apache.ignite.agent.dto.action.RequestDeserializerTest;
import org.apache.ignite.agent.dto.topology.TopologySnapshotTest;
import org.apache.ignite.agent.processor.CacheChangesProcessorTest;
import org.apache.ignite.agent.processor.ClusterInfoProcessorTest;
import org.apache.ignite.agent.processor.ManagementConsoleMessagesProcessorTest;
import org.apache.ignite.agent.processor.ManagementConsoleSpanMessagesProcessorTest;
import org.apache.ignite.agent.processor.action.DistributedActionProcessorTest;
import org.apache.ignite.agent.processor.action.DistributedActionProcessorWithAuthenticationTest;
import org.apache.ignite.agent.processor.export.EventsExporterTest;
import org.apache.ignite.agent.processor.export.SpanExporterTest;
import org.apache.ignite.agent.processor.metrics.MetricsProcessorTest;
import org.apache.ignite.agent.utils.AgentUtilsTest;
import org.apache.ignite.agent.ws.RetryableSenderTest;
import org.apache.ignite.agent.ws.WebSocketManagerTest;
import org.apache.ignite.agent.ws.WebSocketManagerTest.WebSocketManagerSSLTest;
import org.apache.ignite.agent.ws.WebSocketManagerTest.WebSocketManagerTwoWaySSLTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Control Center agent test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    ManagementConsoleAgentTest.class,
    ManagementConsoleAgentWithMockContextTest.class,
    SessionRegistryTest.class,
    ActionControllerAnnotationReaderTest.class,
    ActionControllerBaseTest.class,
    ActionControllerWithAuthenticationBaseTest.class,
    BaselineActionsControllerTest.class,
    ClusterActionsControllerTest.class,
    QueryActionsControllerTest.class,
    QueryActionsControllerWithParametersTest.class,
    SecurityActionsControllerTest.class,
    QueryRegistryTest.class,
    IgniteConfigurationWrapperTest.class,
    RequestDeserializerTest.class,
    TopologySnapshotTest.class,
    CacheChangesProcessorTest.class,
    ClusterInfoProcessorTest.class,
    ManagementConsoleMessagesProcessorTest.class,
    ManagementConsoleSpanMessagesProcessorTest.class,
    DistributedActionProcessorTest.class,
    DistributedActionProcessorWithAuthenticationTest.class,
    EventsExporterTest.class,
    SpanExporterTest.class,
    MetricsProcessorTest.class,
    AgentUtilsTest.class,
    RetryableSenderTest.class,
    WebSocketManagerTest.class,
    WebSocketManagerSSLTest.class,
    WebSocketManagerTwoWaySSLTest.class
})
public class AgentTestSuite {
}
