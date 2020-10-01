/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.testframework;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

/**
 * Allows to listen a log and block execution by {@link MessageWaitingLogListener#await} method until message
 * appears in log.
 */
public class MessageWaitingLogListener extends LogListener {
    /** Regexp for message that triggers callback */
    private final String expMsg;

    /** Callback. */
    private final CountDownLatch latch;

    /**
     * Constructor.
     *
     * @param expMsg Regexp for message that triggers callback.
     */
    public MessageWaitingLogListener(String expMsg) {
        this(expMsg, 1);
    }

    /**
     * Constructor.
     *
     * @param expMsg Regexp for message that triggers callback.
     * @param msgCnt Count of messages to wait.
     */
    public MessageWaitingLogListener(String expMsg, int msgCnt) {
        this.expMsg = expMsg;

        latch = new CountDownLatch(msgCnt);
    }

    /** {@inheritDoc} */
    @Override public boolean check() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        /* No-op */
    }

    /** {@inheritDoc} */
    @Override public void accept(String s) {
        if (s.matches(expMsg))
            latch.countDown();
    }

    /** */
    public void await(long timeout, TimeUnit unit, String failMsg) throws InterruptedException {
        if (!latch.await(timeout, unit))
            fail(failMsg);
    }
}
