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

import org.apache.ignite.agent.action.controller.BaselineActionsControllerTest;
import org.apache.ignite.agent.action.controller.ClusterActionsControllerTest;
import org.apache.ignite.agent.action.controller.NodeActionsControllerTest;
import org.apache.ignite.agent.action.controller.QueryActionsControllerTest;
import org.apache.ignite.agent.action.controller.QueryActionsControllerWithParametersTest;
import org.apache.ignite.agent.action.controller.SecurityActionsControllerTest;
import org.apache.ignite.agent.action.query.QueryRegistryTest;
import org.apache.ignite.agent.service.CacheServiceSelfTest;
import org.apache.ignite.agent.service.ClusterServiceSelfTest;
import org.apache.ignite.agent.service.action.DistributedActionServiceSelfTest;
import org.apache.ignite.agent.service.action.DistributedActionServiceWithSecuritySelfTest;
import org.apache.ignite.agent.service.metrics.MetricsServiceSelfTest;
import org.apache.ignite.agent.service.tracing.TracingServiceSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * GridGain GMC agent self test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    BaselineActionsControllerTest.class,
    CacheServiceSelfTest.class,
    ClusterActionsControllerTest.class,
    ClusterServiceSelfTest.class,
    DistributedActionServiceSelfTest.class,
    DistributedActionServiceWithSecuritySelfTest.class,
    MetricsServiceSelfTest.class,
    NodeActionsControllerTest.class,
    QueryActionsControllerTest.class,
    QueryActionsControllerWithParametersTest.class,
    QueryRegistryTest.class,
    SecurityActionsControllerTest.class,
    TracingServiceSelfTest.class
})
public class AgentSelfTestSuite {
}
