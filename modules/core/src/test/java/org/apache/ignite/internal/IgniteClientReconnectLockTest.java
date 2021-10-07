/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * The test check that lock properly work after client reconnected.
 */
public class IgniteClientReconnectLockTest extends IgniteClientReconnectAbstractTest {
    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected int clientCount() {
        return 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockAfterClientReconnected() throws Exception {
        IgniteEx srv = grid(0);
        IgniteEx client = grid(1);

        IgniteLock lock = client.reentrantLock("lock", true, false, true);

        lock.lock();
        lock.unlock();

        reconnectClientNode(client, srv, () -> {
            boolean wasThrown = false;

            try {
                lock.lock();
            }
            catch (IgniteClientDisconnectedException e) {
                wasThrown = true;
            }

            assertTrue("IgniteClientDisconnectedException was not thrown", wasThrown);
        });

        CountDownLatch lockLatch = new CountDownLatch(1);

        AtomicReference<Boolean> tryLockRes = new AtomicReference<>();

        IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(() -> {
            boolean awaited;

            lock.lock();

            try {
                lockLatch.countDown();

                awaited = GridTestUtils.waitForCondition(() -> tryLockRes.get() != null, 5000);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new RuntimeException(e);
            }
            finally {
                lock.unlock();
            }

            assertTrue("Condition was not achived.", awaited);
        });

        IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(() -> {
            try {
                lockLatch.await();

                tryLockRes.set(lock.tryLock());
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            finally {
                if (lock.isHeldByCurrentThread())
                    lock.unlock();
            }

            assertFalse(tryLockRes.get());
        });

        fut1.get();
        fut2.get();
    }
}
