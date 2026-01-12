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

package org.apache.ignite.util.mbeans;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.worker.WorkersControlMXBeanImpl;
import org.apache.ignite.mxbean.WorkersControlMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.util.IgniteUtils.jdkVersion;
import static org.apache.ignite.internal.util.IgniteUtils.majorJavaVersion;
import static org.junit.Assume.assumeTrue;

/**
 * {@link WorkersControlMXBean} test.
 */
public class WorkersControlMXBeanTest extends GridCommonAbstractTest {
    /** Test thread name. */
    private static final String TEST_THREAD_NAME = "test-thread";

    /**
     * @throws Exception Thrown if test fails.
     */
    @Test
    public void testStopThreadJdk21() throws Exception {
        assumeTrue(majorJavaVersion(jdkVersion()) >= 21);

        WorkersControlMXBean workersCtrlMXBean = new WorkersControlMXBeanImpl(null);

        AtomicBoolean stop = new AtomicBoolean();

        Thread t = startTestThread(stop);

        assertFalse(workersCtrlMXBean.stopThreadByUniqueName(TEST_THREAD_NAME));
        assertFalse(workersCtrlMXBean.stopThreadById(t.getId()));

        stop.set(true);
        t.join(5000);
    }

    /**
     * @throws Exception Thrown if test fails.
     */
    @Test
    public void testStopThreadByUniqueName() throws Exception {
        assumeTrue(majorJavaVersion(jdkVersion()) < 21);

        WorkersControlMXBean workersCtrlMXBean = new WorkersControlMXBeanImpl(null);

        AtomicBoolean stop = new AtomicBoolean();

        Thread t = startTestThread(stop);

        assertTrue(workersCtrlMXBean.stopThreadByUniqueName(TEST_THREAD_NAME));

        t.join(500);

        assertFalse(workersCtrlMXBean.stopThreadByUniqueName(TEST_THREAD_NAME));

        Thread t1 = startTestThread(stop);
        Thread t2 = startTestThread(stop);

        assertFalse(workersCtrlMXBean.stopThreadByUniqueName(TEST_THREAD_NAME));

        stop.set(true);
        t1.join(5000);
        t2.join(5000);
    }

    /**
     * @throws Exception Thrown if test fails.
     */
    @Test
    public void testStopThreadById() throws Exception {
        assumeTrue(majorJavaVersion(jdkVersion()) < 21);

        WorkersControlMXBean workersCtrlMXBean = new WorkersControlMXBeanImpl(null);

        AtomicBoolean stop = new AtomicBoolean();

        Thread t1 = startTestThread(stop);
        Thread t2 = startTestThread(stop);

        assertTrue(workersCtrlMXBean.stopThreadById(t1.getId()));
        assertTrue(workersCtrlMXBean.stopThreadById(t2.getId()));

        t1.join(500);
        t2.join(500);

        assertFalse(workersCtrlMXBean.stopThreadById(t1.getId()));
        assertFalse(workersCtrlMXBean.stopThreadById(t2.getId()));
    }

    /**
     * @return Started thread.
     */
    private static Thread startTestThread(AtomicBoolean stop) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        Thread t = new Thread(TEST_THREAD_NAME) {
            @Override public void run() {
                latch.countDown();

                while (!stop.get()) {
                    // No-op
                }
            }
        };

        t.start();

        assertTrue(latch.await(500, TimeUnit.MILLISECONDS));

        assertTrue(t.isAlive());

        return t;
    }
}
