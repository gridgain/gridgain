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

package org.gridgain.testsuites;

import org.gridgain.action.controller.ActionControllerBaseTest;
import org.gridgain.action.controller.ActionControllerWithSecurityBaseTest;
import org.gridgain.action.controller.BaselineActionsControllerTest;
import org.gridgain.action.controller.ClusterActionsControllerTest;
import org.gridgain.action.controller.NodeActionsControllerTest;
import org.gridgain.action.controller.QueryActionsControllerTest;
import org.gridgain.action.controller.SecurityActionsControllerTest;
import org.gridgain.action.query.QueryRegistryTest;
import org.gridgain.service.MetricProtocolTest;
import org.gridgain.service.CacheServiceSelfTest;
import org.gridgain.service.ClusterServiceSelfTest;
import org.gridgain.service.MetricsServiceSelfTest;
import org.gridgain.service.tracing.TracingServiceSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * GridGain GMC agent self test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    ActionControllerBaseTest.class,
    ActionControllerWithSecurityBaseTest.class,
    BaselineActionsControllerTest.class,
    CacheServiceSelfTest.class,
    ClusterActionsControllerTest.class,
    ClusterServiceSelfTest.class,
    MetricsServiceSelfTest.class,
    MetricProtocolTest.class,
    NodeActionsControllerTest.class,
    QueryActionsControllerTest.class,
    QueryRegistryTest.class,
    SecurityActionsControllerTest.class,
    TracingServiceSelfTest.class
})
public class AgentSelfTestSuite {
}
