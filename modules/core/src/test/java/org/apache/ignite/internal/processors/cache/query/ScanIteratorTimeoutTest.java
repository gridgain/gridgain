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

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/** Test */
public class ScanIteratorTimeoutTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(2, true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String s) throws Exception {
        return new IgniteConfiguration(super.getConfiguration(s)).
                setCommunicationSpi(new BlockedTcpCommunicationSpi());
    }

    /** Test */
    @Test
    public void testTimeoutOnLoosingQueryResponse() throws Exception {
        IgniteEx ignite0 = grid(0);
        Ignite ignite1 = grid(1);

        BlockedTcpCommunicationSpi commSpi0 = (BlockedTcpCommunicationSpi)ignite0.configuration().getCommunicationSpi();
        commSpi0.blockFirstQueryResponse();

        BlockedTcpCommunicationSpi commSpi1 = (BlockedTcpCommunicationSpi)ignite1.configuration().getCommunicationSpi();
        commSpi1.blockFirstQueryResponse();

        CacheConfiguration<Integer, String> cc = new CacheConfiguration<Integer, String>().
                setName("cache").
                setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).
                setCacheMode(PARTITIONED);

        IgniteCache<Integer, String> cache = ignite1.createCache(cc);

        //writer
        for (int i = 0; i < 1_000; i++)
            cache.put(i, i + "_value");

        IgniteInternalCache<Integer, String> cache1 = ignite0.cachex("cache");

        IgniteCallable<Void> c = (IgniteCallable<Void>) () -> {
            cache1.context().gate().enter();

            try (GridNearTxLocal tx1 = cache1.txStartEx(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                String val = cache1.get(34);
                cache1.put(34, "NEW VAL");

                Iterator<Cache.Entry<Integer, String>> itr = cache1.scanIterator(false, null, 2000);

                while (itr.hasNext()) {
                    Cache.Entry<Integer, String> entry = itr.next();

                    cache1.invoke(entry.getKey(), new MyEntryProcessor());
                }

                tx1.commit();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                cache1.context().gate().leave();
            }

            return null;
        };

        GridTestUtils.assertThrowsWithCause(c, IgniteFutureTimeoutCheckedException.class);
    }

    /** */
    public static class MyEntryProcessor implements EntryProcessor<Integer, String, Void> {
        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, String> entry, Object... arguments) throws EntryProcessorException {
            entry.setValue(entry.getKey() + "_NEW_VALUE");
            return null;
        }
    }

    /** Communication SPI which block fist GridCacheQueryResponse */
    private static class BlockedTcpCommunicationSpi extends TcpCommunicationSpi {
        /** if true, then GridCacheQueryResponse will be blocked */
        private final AtomicBoolean block;

        /** */
        public BlockedTcpCommunicationSpi() {
            block = new AtomicBoolean(false);
        }

        /** cock needToBlock */
        public void blockFirstQueryResponse() {
            block.set(true);
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (!block.get())
                super.sendMessage(node, msg, ackC);

            else {
                if (msg instanceof GridIoMessage && ((GridIoMessage) msg).message() instanceof GridCacheQueryResponse) {
                    if (!block.compareAndSet(true, false))
                        super.sendMessage(node, msg, ackC);
                }
                else
                    super.sendMessage(node, msg, ackC);
            }
        }
    }
}
