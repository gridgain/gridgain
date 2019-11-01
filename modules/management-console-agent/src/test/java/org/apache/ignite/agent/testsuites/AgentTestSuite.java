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

import org.apache.ignite.agent.action.annotation.ActionControllerAnnotationProcessorTest;
import org.apache.ignite.agent.dto.action.RequestDeserializerTest;
import org.apache.ignite.agent.dto.topology.TopologySnapshotTest;
import org.apache.ignite.agent.service.event.EventsExporterTest;
import org.apache.ignite.agent.service.sender.RetryableSenderTest;
import org.apache.ignite.agent.service.tracing.ManagementConsoleSpanExporterTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Management console agent test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    ActionControllerAnnotationProcessorTest.class,
    ManagementConsoleSpanExporterTest.class,
    EventsExporterTest.class,
    RetryableSenderTest.class,
    RequestDeserializerTest.class,
    TopologySnapshotTest.class
})
public class AgentTestSuite {
}
