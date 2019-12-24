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
package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch.ExchangeLatchManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch.Latch;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests for {@link ExchangeLatchManager} functionality when latch coordinator is failed.
 */
public class IgniteExchangeLatchManagerTest extends GridCommonAbstractTest {
    /** */
    private static final String LATCH_NAME = "test";

    /** */
    private static final String OTHER_LATCH_NAME = "otherTest";

    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldCorrectlyExecuteLatchWithAnyIdsWhenCrdStartedLast() throws Exception {
        IgniteEx crd = startGrids(3);

        //Increase version. It needs because latch manager don't included last joining node.
        crd.cluster().active(true);
        crd.cluster().active(false);

        AffinityTopologyVersion lesserVer = new AffinityTopologyVersion(crd.cluster().topologyVersion(), 1);
        AffinityTopologyVersion greaterVer = new AffinityTopologyVersion(crd.cluster().topologyVersion(), 2);

        AtomicReference<IgniteCheckedException> errors = new AtomicReference<>();

        //Create three different latches.
        IgniteInternalFuture lesserFuture = countDownAndAwaitTwoLatches(LATCH_NAME, lesserVer, errors);
        IgniteInternalFuture greaterFuture = countDownAndAwaitTwoLatches(LATCH_NAME, greaterVer, errors);
        IgniteInternalFuture otherFuture = countDownAndAwaitTwoLatches(OTHER_LATCH_NAME, greaterVer, errors);

        assertFalse(lesserFuture.isDone());
        assertFalse(greaterFuture.isDone());
        assertFalse(otherFuture.isDone());

        //Create coordinator latch of 'otherLatch' after client latch creation.
        Latch latchCrdOther = latchManager(0).getOrCreate(OTHER_LATCH_NAME, greaterVer);

        latchCrdOther.countDown();
        latchCrdOther.await();

        assertFalse(lesserFuture.isDone());
        assertFalse(greaterFuture.isDone());
        assertTrue(waitForCondition(otherFuture::isDone, 1000));

        //Create coordinator latch with greater version after client latch creation.
        Latch latchCrdGreater = latchManager(0).getOrCreate(LATCH_NAME, greaterVer);

        latchCrdGreater.countDown();
        latchCrdGreater.await();

        assertFalse(lesserFuture.isDone());
        assertTrue(waitForCondition(greaterFuture::isDone, 1000));

        //Create coordinator latch with lesser version after client latch creation.
        Latch latchCrdLesser = latchManager(0).getOrCreate(LATCH_NAME, lesserVer);

        latchCrdLesser.countDown();
        latchCrdLesser.await();

        assertTrue(waitForCondition(lesserFuture::isDone, 1000));
        assertNull(String.valueOf(errors.get()), errors.get());
    }

    /**
     * Create latch and await finishing asynchronously.
     *
     * @param latchName Latch id.
     * @param ver Version.
     * @param errors Holder of error if case of fail.
     * @return Future for latch finish.
     */
    private IgniteInternalFuture countDownAndAwaitTwoLatches(
        String latchName,
        AffinityTopologyVersion ver,
        AtomicReference<IgniteCheckedException> errors
    ) {
        Latch latch1 = latchManager(1).getOrCreate(latchName, ver);
        Latch latch2 = latchManager(2).getOrCreate(latchName, ver);

        latch1.countDown();
        latch2.countDown();

        return GridTestUtils.runAsync(() -> {
            try {
                latch1.await();
                latch2.await();
            }
            catch (IgniteCheckedException e) {
                errors.set(e);
            }
        });
    }

    /**
     * Extract latch manager.
     *
     * @param nodeId Node id from which latch should be extracted.
     * @return Latch manager.
     */
    private ExchangeLatchManager latchManager(int nodeId) {
        return grid(nodeId).context().cache().context().exchange().latch();
    }
}
