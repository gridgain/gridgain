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

package org.apache.ignite.internal;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.SystemPropertiesRule;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.testframework.LogListener.matches;

/**
 *
 */
public class TransactionsMXBeanImplTest extends GridCommonAbstractTest {
    /** Class rule. */
    @ClassRule public static final TestRule classRule = new SystemPropertiesRule();

    /** Listener log messages. */
    private static ListeningTestLogger testLog;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        testLog = new ListeningTestLogger(false, log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        testLog.clearListeners();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        return super.getConfiguration(name)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setGridLogger(testLog)
            .setCacheConfiguration(
                new CacheConfiguration<>()
                    .setName(DEFAULT_CACHE_NAME)
                    .setAffinity(new RendezvousAffinityFunction(false, 32))
                    .setBackups(1)
                    .setAtomicityMode(TRANSACTIONAL)
                    .setRebalanceMode(CacheRebalanceMode.ASYNC)
                    .setWriteSynchronizationMode(FULL_SYNC)
            );
    }

    /**
     *
     */
    @Test
    public void testBasic() throws Exception {
        IgniteEx ignite = startGrid(0);

        TransactionsMXBean bean = txMXBean(0);

        ignite.transactions().txStart();

        ignite.cache(DEFAULT_CACHE_NAME).put(0, 0);

        String res = bean.getActiveTransactions(null, null, null, null, null, null, null, null, false, false);

        assertEquals("1", res);

        res = bean.getActiveTransactions(null, null, null, null, null, null, null, null, true, false);

        assertTrue(res.indexOf("Tx:") > 0);

        res = bean.getActiveTransactions(null, null, null, null, null, null, null, null, false, true);

        assertEquals("1", res);

        doSleep(500);

        res = bean.getActiveTransactions(null, null, null, null, null, null, null, null, false, false);

        assertEquals("0", res);
    }

    /**
     * Test to verify the change "Operations dump timeout" to display
     * long running transactions.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = "100")
    public void testOperationsDumpTimeout() throws Exception {
        IgniteEx ignite = startGrid(0);

        TransactionsMXBean txMXBean = txMXBean(0);

        ignite.transactions().txStart();

        LogListener logLsnr = matches("First 10 long running transactions").build();
        testLog.registerListener(logLsnr);

        int waitTime = 500;

        waitForCondition(logLsnr::check, waitTime);

        txMXBean.setOperationsDumpTimeout(4_000);

        logLsnr.reset();

        waitForCondition(() -> !logLsnr.check(), waitTime);
    }

    /**
     *
     */
    private TransactionsMXBean txMXBean(int igniteInt) throws Exception {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(igniteInt), "Transactions",
            TransactionsMXBeanImpl.class.getSimpleName());

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, TransactionsMXBean.class, true);
    }
}
