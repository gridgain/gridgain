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

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.testframework.LogListener.matches;

/**
 *
 */
public class TransactionsMXBeanImplTest extends GridCommonAbstractTest {
    /** Prefix of key for distributed meta storage. */
    private static final String DIST_CONF_PREFIX = "distrConf-";

    /** Listener log messages. */
    private static ListeningTestLogger testLog;

    /** Client node. */
    private boolean clientNode;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        testLog = new ListeningTestLogger(false, log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        //cleanPersistenceDir();

        testLog.clearListeners();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        return super.getConfiguration(name)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setGridLogger(testLog)
            .setClientMode(clientNode)
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
     * Test for changing lrt timeout and their appearance before default
     * timeout through MXBean.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = "60000")
    public void testLongOperationsDumpTimeoutPositive() throws Exception {
        checkLongOperationsDumpTimeoutViaTxMxBean(60_000, 100, 10_000, true);
    }

    /**
     * Test to disable the LRT by setting timeout to 0.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = "100")
    public void testLongOperationsDumpTimeoutZero() throws Exception {
        checkLongOperationsDumpTimeoutViaTxMxBean(100, 0, 1_000, false);
    }

    /**
     * Test to disable the LRT by setting timeout to -1.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = "100")
    public void testLongOperationsDumpTimeoutNegative() throws Exception {
        checkLongOperationsDumpTimeoutViaTxMxBean(100, -1, 1_000, false);
    }

    /**
     * Test to verify the correct change of "Long operations dump timeout." in
     * an immutable cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = "100")
    public void testChangeLongOperationsDumpTimeoutOnImmutableCluster() throws Exception {
        Map<IgniteEx, TransactionsMXBean> allNodes = new HashMap<>();
        Map<IgniteEx, TransactionsMXBean> clientNodes = new HashMap<>();
        Map<IgniteEx, TransactionsMXBean> serverNodes = new HashMap<>();

        for (int i = 0; i < 2; i++) {
            IgniteEx igniteEx = startGrid(i);
            TransactionsMXBean transactionsMXBean = txMXBean(i);
            allNodes.put(igniteEx, transactionsMXBean);
            serverNodes.put(igniteEx, transactionsMXBean);
        }

        clientNode = true;

        for (int i = 2; i < 4; i++) {
            IgniteEx igniteEx = startGrid(i);
            TransactionsMXBean transactionsMXBean = txMXBean(i);
            allNodes.put(igniteEx, transactionsMXBean);
            clientNodes.put(igniteEx, transactionsMXBean);
        }

        //check for default value
        checkLongOperationsDumpTimeoutViaTxMxBean(allNodes, 100L);

        //create property update latches for client nodes
        Map<IgniteEx, List<CountDownLatch>> updateLatches = new HashMap<>();

        clientNodes.keySet().forEach(ignite -> updateLatches.put(ignite, F.asList(new CountDownLatch(1), new CountDownLatch(1))));

        clientNodes.forEach((igniteEx, bean) -> {
            igniteEx.context().distributedMetastorage().listen(
                    (key) -> key.startsWith(DIST_CONF_PREFIX),
                    (String key, Serializable oldVal, Serializable newVal) -> {
                        if ((long) newVal == 200)
                            updateLatches.get(igniteEx).get(0).countDown();
                        if ((long) newVal == 300)
                            updateLatches.get(igniteEx).get(1).countDown();
                    });
        });

        long newTimeout = 200L;

        //update value via server node
        updateLongOperationsDumpTimeoutViaTxMxBean(serverNodes, newTimeout);

        //check new value in server nodes
        checkLongOperationsDumpTimeoutViaTxMxBean(serverNodes, newTimeout);

        //check new value in client nodes
        for (List<CountDownLatch> list : updateLatches.values()) {
            CountDownLatch countDownLatch = list.get(0);
            countDownLatch.await(100, TimeUnit.MILLISECONDS);
        }

        newTimeout = 300L;

        //update value via server node
        updateLongOperationsDumpTimeoutViaTxMxBean(clientNodes, newTimeout);

        //check new value in server nodes
        checkLongOperationsDumpTimeoutViaTxMxBean(serverNodes, newTimeout);

        //check new value in client nodes
        for (List<CountDownLatch> list : updateLatches.values()) {
            CountDownLatch countDownLatch = list.get(1);
            countDownLatch.await(100, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Test to verify the correct change of "Long operations dump timeout." in
     * an mutable cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = "100")
    public void testChangeLongOperationsDumpTimeoutOnMutableCluster() throws Exception {
        Map<IgniteEx, TransactionsMXBean> node0 = singletonMap(startGrid(0), txMXBean(0));
        Map<IgniteEx, TransactionsMXBean> node1 = singletonMap(startGrid(1), txMXBean(1));

        Map<IgniteEx, TransactionsMXBean> allNodes = new HashMap<>();
        allNodes.putAll(node0);
        allNodes.putAll(node1);

        long newTimeout = 200L;
        updateLongOperationsDumpTimeoutViaTxMxBean(allNodes, newTimeout);
        checkLongOperationsDumpTimeoutViaTxMxBean(allNodes, newTimeout);
        allNodes.clear();

        stopGrid(1);
        node1 = singletonMap(startGrid(1), txMXBean(1));

        //check that new value after restart
        checkLongOperationsDumpTimeoutViaTxMxBean(node0, newTimeout);
        checkLongOperationsDumpTimeoutViaTxMxBean(node1, newTimeout);

        newTimeout = 300L;
        updateLongOperationsDumpTimeoutViaTxMxBean(node0, newTimeout);

        Map<IgniteEx, TransactionsMXBean> node2 = singletonMap(startGrid(2), txMXBean(2));

        //check that last value in new node
        checkLongOperationsDumpTimeoutViaTxMxBean(node0, newTimeout);
        checkLongOperationsDumpTimeoutViaTxMxBean(node1, newTimeout);
        checkLongOperationsDumpTimeoutViaTxMxBean(node2, newTimeout);
    }

    /**
     * Test to verify the correct change of "Allowance to dump requests from local node to near node,
     * when long running transaction is found." in an mutable cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_TX_OWNER_DUMP_REQUESTS_ALLOWED, value = "false")
    public void testChangeTxOwnerDumpRequestsAllowed() throws Exception{
        Map<IgniteEx, TransactionsMXBean> node0 = singletonMap(startGrid(0), txMXBean(0));
        Map<IgniteEx, TransactionsMXBean> node1 = singletonMap(startGrid(1), txMXBean(1));

        Map<IgniteEx, TransactionsMXBean> allNodes = new HashMap<>();
        allNodes.putAll(node0);
        allNodes.putAll(node1);

        //check for default value
        checkTxOwnerDumpRequestsAllowedViaTxMxBean(allNodes, false);

        updateTxOwnerDumpRequestsAllowedViaTxMxBean(allNodes, true);

        //check new value
        checkTxOwnerDumpRequestsAllowedViaTxMxBean(allNodes, true);

        stopGrid(1);
        node1 = singletonMap(startGrid(1), txMXBean(1));

        //check that new value after restart
        checkTxOwnerDumpRequestsAllowedViaTxMxBean(node0, true);
        checkTxOwnerDumpRequestsAllowedViaTxMxBean(node1, true);

        Map<IgniteEx, TransactionsMXBean> node2 = singletonMap(startGrid(2), txMXBean(2));

        //check that last value in new node
        checkTxOwnerDumpRequestsAllowedViaTxMxBean(node0, true);
        checkTxOwnerDumpRequestsAllowedViaTxMxBean(node1, true);
        checkTxOwnerDumpRequestsAllowedViaTxMxBean(node2, true);
    }

    /**
     * Test to verify the correct change of "Threshold timeout for long transactions." in an mutable cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_TRANSACTION_TIME_DUMP_THRESHOLD, value = "0")
    public void testChangeLongTransactionTimeDumpThreshold() throws Exception{
        Map<IgniteEx, TransactionsMXBean> node0 = singletonMap(startGrid(0), txMXBean(0));
        Map<IgniteEx, TransactionsMXBean> node1 = singletonMap(startGrid(1), txMXBean(1));

        Map<IgniteEx, TransactionsMXBean> allNodes = new HashMap<>();
        allNodes.putAll(node0);
        allNodes.putAll(node1);

        //check for default value
        checkLongTransactionTimeDumpThresholdViaTxMxBean(allNodes, 0);

        updateLongTransactionTimeDumpThresholdViaTxMxBean(allNodes, 999);

        //check new value
        checkLongTransactionTimeDumpThresholdViaTxMxBean(allNodes, 999);

        stopGrid(1);
        node1 = singletonMap(startGrid(1), txMXBean(1));

        //check that new value after restart
        checkLongTransactionTimeDumpThresholdViaTxMxBean(node0, 999);
        checkLongTransactionTimeDumpThresholdViaTxMxBean(node1, 999);

        Map<IgniteEx, TransactionsMXBean> node2 = singletonMap(startGrid(2), txMXBean(2));

        //check that last value in new node
        checkLongTransactionTimeDumpThresholdViaTxMxBean(node0, 999);
        checkLongTransactionTimeDumpThresholdViaTxMxBean(node1, 999);
        checkLongTransactionTimeDumpThresholdViaTxMxBean(node2, 999);
    }

    /**
     * Test to verify the correct change of "The coefficient for samples of completed transactions
     * that will be dumped in log." in an mutable cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_COEFFICIENT, value = "0.0")
    public void testChangeTransactionTimeDumpSamplesCoefficient() throws Exception{
        Map<IgniteEx, TransactionsMXBean> node0 = singletonMap(startGrid(0), txMXBean(0));
        Map<IgniteEx, TransactionsMXBean> node1 = singletonMap(startGrid(1), txMXBean(1));

        Map<IgniteEx, TransactionsMXBean> allNodes = new HashMap<>();
        allNodes.putAll(node0);
        allNodes.putAll(node1);

        //check for default value
        checkTransactionTimeDumpSamplesCoefficientViaTxMxBean(allNodes, 0.0);

        updateTransactionTimeDumpSamplesCoefficientViaTxMxBean(allNodes, 1.0);

        //check new value
        checkTransactionTimeDumpSamplesCoefficientViaTxMxBean(allNodes, 1.0);

        stopGrid(1);
        node1 = singletonMap(startGrid(1), txMXBean(1));

        //check that new value after restart
        checkTransactionTimeDumpSamplesCoefficientViaTxMxBean(node0, 1.0);
        checkTransactionTimeDumpSamplesCoefficientViaTxMxBean(node1, 1.0);

        Map<IgniteEx, TransactionsMXBean> node2 = singletonMap(startGrid(2), txMXBean(2));

        //check that last value in new node
        checkTransactionTimeDumpSamplesCoefficientViaTxMxBean(node0, 1.0);
        checkTransactionTimeDumpSamplesCoefficientViaTxMxBean(node1, 1.0);
        checkTransactionTimeDumpSamplesCoefficientViaTxMxBean(node2, 1.0);
    }

    /**
     * Test to verify the correct change of "The coefficient for samples of completed transactions
     * that will be dumped in log." in an mutable cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_PER_SECOND_LIMIT, value = "5")
    public void testChangeLongTransactionTimeDumpSamplesPerSecondLimit() throws Exception{
        Map<IgniteEx, TransactionsMXBean> node0 = singletonMap(startGrid(0), txMXBean(0));
        Map<IgniteEx, TransactionsMXBean> node1 = singletonMap(startGrid(1), txMXBean(1));

        Map<IgniteEx, TransactionsMXBean> allNodes = new HashMap<>();
        allNodes.putAll(node0);
        allNodes.putAll(node1);

        //check for default value
        checkLongTransactionTimeDumpSamplesPerSecondLimitViaTxMxBean(allNodes, 5);

        updateLongTransactionTimeDumpSamplesPerSecondLimitViaTxMxBean(allNodes, 10);

        //check new value
        checkLongTransactionTimeDumpSamplesPerSecondLimitViaTxMxBean(allNodes, 10);

        stopGrid(1);
        node1 = singletonMap(startGrid(1), txMXBean(1));

        //check that new value after restart
        checkLongTransactionTimeDumpSamplesPerSecondLimitViaTxMxBean(node0, 10);
        checkLongTransactionTimeDumpSamplesPerSecondLimitViaTxMxBean(node1, 10);

        Map<IgniteEx, TransactionsMXBean> node2 = singletonMap(startGrid(2), txMXBean(2));

        //check that last value in new node
        checkLongTransactionTimeDumpSamplesPerSecondLimitViaTxMxBean(node0, 10);
        checkLongTransactionTimeDumpSamplesPerSecondLimitViaTxMxBean(node1, 10);
        checkLongTransactionTimeDumpSamplesPerSecondLimitViaTxMxBean(node2, 10);
    }

    /**
     * Search for the first node and change "Long operations dump timeout." through TransactionsMXBean.
     *
     * @param nodes Nodes with TransactionsMXBean's.
     * @param newTimeout New timeout.
     */
    private void updateLongOperationsDumpTimeoutViaTxMxBean(
        Map<IgniteEx, TransactionsMXBean> nodes,
        long newTimeout
    ) {
        assertNotNull(nodes);

        nodes.entrySet().stream()
            .findAny().get().getValue().setLongOperationsDumpTimeout(newTimeout);
    }

    /**
     * Search for the first node and change "Allowance to dump requests from local node to near node, when long
     * running transaction is found." through TransactionsMXBean.
     *
     * @param nodes Nodes with TransactionsMXBean's.
     * @param allowed New allowance flag.
     */
    private void updateTxOwnerDumpRequestsAllowedViaTxMxBean(
            Map<IgniteEx, TransactionsMXBean> nodes,
            boolean allowed
    ) {
        assertNotNull(nodes);

        nodes.entrySet().stream()
                .findAny().get().getValue().setTxOwnerDumpRequestsAllowed(allowed);
    }

    /**
     * Search for the first node and change "Threshold timeout for long transactions." through TransactionsMXBean.
     *
     * @param nodes Nodes with TransactionsMXBean's.
     * @param threshold New threshold timeout for long transactions.
     */
    private void updateLongTransactionTimeDumpThresholdViaTxMxBean(
            Map<IgniteEx, TransactionsMXBean> nodes,
            long threshold
    ) {
        assertNotNull(nodes);

        nodes.entrySet().stream()
                .findAny().get().getValue().setLongTransactionTimeDumpThreshold(threshold);
    }

    /**
     * Search for the first node and change "The coefficient for samples of completed transactions
     * that will be dumped in log." through TransactionsMXBean.
     *
     * @param nodes Nodes with TransactionsMXBean's.
     * @param coefficient New coefficient for samples of completed transactions.
     */
    private void updateTransactionTimeDumpSamplesCoefficientViaTxMxBean(
            Map<IgniteEx, TransactionsMXBean> nodes,
            double coefficient
    ) {
        assertNotNull(nodes);

        nodes.entrySet().stream()
                .findAny().get().getValue().setTransactionTimeDumpSamplesCoefficient(coefficient);
    }

    /**
     * Search for the first node and change "Limit of samples of completed transactions
     * that will be dumped in log per second." through TransactionsMXBean.
     *
     * @param nodes Nodes with TransactionsMXBean's.
     * @param limit New limit of samples of completed transactions that will be dumped in log per second.
     */
    private void updateLongTransactionTimeDumpSamplesPerSecondLimitViaTxMxBean(
            Map<IgniteEx, TransactionsMXBean> nodes,
            int limit
    ) {
        assertNotNull(nodes);

        nodes.entrySet().stream()
                .findAny().get().getValue().setTransactionTimeDumpSamplesPerSecondLimit(limit);
    }

    /**
     * Checking the value of "Long operations dump timeout." on all nodes
     * through the TransactionsMXBean.
     *
     * @param nodes Nodes with TransactionsMXBean's.
     * @param checkTimeout Checked timeout.
     */
    private void checkLongOperationsDumpTimeoutViaTxMxBean(Map<IgniteEx, TransactionsMXBean> nodes, long checkTimeout) {
        assertNotNull(nodes);

        nodes.forEach((node, txMxBean) -> {
            log.info("NODE: " + node.name());
            assertEquals(checkTimeout, txMxBean.getLongOperationsDumpTimeout());
        });
    }

    /**
     * Checking the value of "Allowance to dump requests from local node to near node, when long running transaction
     * is found." on nodes through the TransactionsMXBean.
     *
     * @param nodes Nodes with TransactionsMXBean's.
     * @param allowed Checked allowance flag.
     */
    private void checkTxOwnerDumpRequestsAllowedViaTxMxBean(Map<IgniteEx, TransactionsMXBean> nodes, boolean allowed) {
        assertNotNull(nodes);

        nodes.forEach((node, txMxBean) -> assertEquals(allowed, txMxBean.getTxOwnerDumpRequestsAllowed()));
    }

    /**
     * Checking the value of "Threshold timeout for long transactions." on nodes through the TransactionsMXBean.
     *
     * @param nodes Nodes with TransactionsMXBean's.
     * @param threshold Checked threshold timeout for long transactions.
     */
    private void checkLongTransactionTimeDumpThresholdViaTxMxBean(Map<IgniteEx, TransactionsMXBean> nodes, long threshold) {
        assertNotNull(nodes);

        nodes.forEach((node, txMxBean) -> assertEquals(threshold, txMxBean.getLongTransactionTimeDumpThreshold()));
    }

    /**
     * Checking the value of "The coefficient for samples of completed transactions that will be dumped in log."
     * on nodes through the TransactionsMXBean.
     *
     * @param nodes Nodes with TransactionsMXBean's.
     * @param coefficient Checked coefficient for samples of completed transactions.
     */
    private void checkTransactionTimeDumpSamplesCoefficientViaTxMxBean(Map<IgniteEx, TransactionsMXBean> nodes, double coefficient) {
        assertNotNull(nodes);

        nodes.forEach((node, txMxBean) -> assertEquals(coefficient, txMxBean.getTransactionTimeDumpSamplesCoefficient()));
    }

    /**
     * Checking the value of "Limit of samples of completed transactions that will be dumped in log per second."
     * on nodes through the TransactionsMXBean.
     *
     * @param nodes Nodes with TransactionsMXBean's.
     * @param limit Checked limit of samples of completed transactions that will be dumped in log per second.
     */
    private void checkLongTransactionTimeDumpSamplesPerSecondLimitViaTxMxBean(Map<IgniteEx, TransactionsMXBean> nodes, int limit) {
        assertNotNull(nodes);

        nodes.forEach((node, txMxBean) -> assertEquals(limit, txMxBean.getTransactionTimeDumpSamplesPerSecondLimit()));
    }

    /**
     * Checking changes and receiving lrt through MXBean.
     *
     * @param defTimeout Default lrt timeout.
     * @param newTimeout New lrt timeout.
     * @param waitTimeTx Waiting time for a lrt.
     * @param expectTx Expect or not a lrt to log.
     * @throws Exception If failed.
     */
    private void checkLongOperationsDumpTimeoutViaTxMxBean(
        long defTimeout,
        long newTimeout,
        long waitTimeTx,
        boolean expectTx
    ) throws Exception {
        IgniteEx ignite = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        TransactionsMXBean txMXBean = txMXBean(0);
        TransactionsMXBean txMXBean1 = txMXBean(1);

        assertEquals(defTimeout, txMXBean.getLongOperationsDumpTimeout());
        assertEquals(defTimeout, txMXBean1.getLongOperationsDumpTimeout());

        Transaction tx = ignite.transactions().txStart();

        LogListener lrtLogLsnr = matches("First 10 long running transactions [total=1]").build();
        LogListener txLogLsnr = matches(((TransactionProxyImpl)tx).tx().xidVersion().toString()).build();

        testLog.registerListener(lrtLogLsnr);
        testLog.registerListener(txLogLsnr);

        txMXBean.setLongOperationsDumpTimeout(newTimeout);

        assertEquals(newTimeout, ignite.context().cache().context().tm().longOperationsDumpTimeout());
        assertEquals(newTimeout, ignite1.context().cache().context().tm().longOperationsDumpTimeout());

        if (expectTx)
            assertTrue(waitForCondition(() -> lrtLogLsnr.check() && txLogLsnr.check(), waitTimeTx));
        else
            assertFalse(waitForCondition(() -> lrtLogLsnr.check() && txLogLsnr.check(), waitTimeTx));
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
