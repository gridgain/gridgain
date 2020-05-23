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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

/**
 * GridDhtCacheEntry::toString leads to system "deadlock".
 */
public class TxDeadlockOnEntryToStringTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        /*if(getTestIgniteInstanceName(2).equals(igniteInstanceName))
            cfg.setCommunicationSpi(new BlockTcpCommunicationSpi());*/

        return cfg;
    }

    @Test
    public void testDeadlockReproducer() throws Exception {
        Ignite keyAffinityNode = startGrid(0);
        Ignite nearNode = startGrid(1);
        Ignite listenerNode = startGrid(2);


        Integer keyNode0 = primaryKey(keyAffinityNode.cache(DEFAULT_CACHE_NAME));

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        qry.setLocalListener((evts) ->
            evts.forEach(e -> System.out.println("key=" + e.getKey() + ", val=" + e.getValue())));

        listenerNode.cache(DEFAULT_CACHE_NAME).query(qry);

        try(Transaction tx = nearNode.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED, 10000, 10)) {
            nearNode.cache(DEFAULT_CACHE_NAME).put(keyNode0, 1235);


            tx.commit();
        }

        /*

        Stan  5:10 PM
таймаут воркер пытается взять лок энтри
лок энтри держит поток X
поток X пытается открыть комуникейшн соединение
комуникейшн подвисает - ему нужен таймаут воркер, чтобы продолжить

         */



        // 1. run 3 nodes
        // 2. node 1 does transaction with key is affinity on node 0
        // 3. node node 2 has continious listener
        // 4. connection on node 3 doesn't exist and can't connect

    }

/*
    *//**
     *
     *//*
    protected static class BlockTcpCommunicationSpi extends TcpCommunicationSpi {
        *//** *//*
        volatile Class msgCls;

        *//** *//*
        AtomicBoolean collectStart = new AtomicBoolean(false);

        *//** *//*
        ConcurrentHashMap<String, ClusterNode> classes = new ConcurrentHashMap<>();

        *//** *//*
        @LoggerResource
        private IgniteLogger log;

        *//** {@inheritDoc} *//*
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            Class msgCls0 = msgCls;

            if (collectStart.get() && msg instanceof GridIoMessage)
                classes.put(((GridIoMessage)msg).message().getClass().getName(), node);

            if (msgCls0 != null && msg instanceof GridIoMessage
                && ((GridIoMessage)msg).message().getClass().equals(msgCls)) {
                log.info("Block message: " + msg);

                return;
            }

            super.sendMessage(node, msg, ackC);
        }



        *//**
         * @param clazz Class of messages which will be block.
         *//*
        public void blockMessage(Class clazz) {
            msgCls = clazz;
        }

        *//**
         * Unlock all message.
         *//*
        public void unblockMessage() {
            msgCls = null;
        }

        *//**
         * Start collect messages.
         *//*
        public void start() {
            collectStart.set(true);
        }

        *//**
         * Print collected messages.
         *//*
        public void print() {
            for (String s : classes.keySet())
                log.error(s);
        }
    }*/

}
