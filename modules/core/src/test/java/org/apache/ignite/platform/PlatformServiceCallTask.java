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

package org.apache.ignite.platform;

import java.util.UUID;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.internal.processors.platform.PlatformNativeException;
import org.apache.ignite.internal.processors.platform.services.PlatformService;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *  Basic task to calling {@link PlatformService} from Java.
 */
public class PlatformServiceCallTask extends AbstractPlatformServiceCallTask {
    /** {@inheritDoc} */
    @Override ComputeJobAdapter createJob(String svcName) {
        return new PlatformServiceCallJob(svcName);
    }

    /** */
    static class PlatformServiceCallJob extends AbstractServiceCallJob {
        /**
         * @param srvcName Service name.
         */
        PlatformServiceCallJob(String srvcName) {
            super(srvcName);
        }

        /** {@inheritDoc} */
        @Override void runTest() {
            TestPlatformService srv = serviceProxy();

            checkNodeId(srv);
            checkUuidProp(srv);
            checkObjectProp(srv);
            checkErrorMethod(srv);
            checkContextAttribute(srv);
        }

        /** */
        protected void checkNodeId(TestPlatformService srv) {
            UUID nodeId = srv.getNodeId();
            assertTrue(ignite.cluster().nodes().stream().anyMatch(n -> n.id().equals(nodeId)));
        }

        /** */
        protected void checkUuidProp(TestPlatformService srv) {
            UUID expUuid = UUID.randomUUID();
            srv.setGuidProp(expUuid);
            assertEquals(expUuid, srv.getGuidProp());
        }

        /** */
        protected void checkObjectProp(TestPlatformService srv) {
            TestValue exp = new TestValue(1, "test");
            srv.setValueProp(exp);
            assertEquals(exp, srv.getValueProp());
        }

        /** */
        protected void checkErrorMethod(TestPlatformService srv) {
            PlatformNativeException nativeEx = (PlatformNativeException)GridTestUtils
                .assertThrowsWithCause(srv::errorMethod, PlatformNativeException.class)
                .getCause();

            assertTrue(nativeEx.toString().contains(nativeEx.getClass().getSimpleName()));
        }

        /** */
        protected void checkContextAttribute(TestPlatformService srv) {
            assertEquals("value", srv.contextAttribute("attr"));
        }
    }
}
