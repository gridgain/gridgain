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

package org.gridgain.service.tracing;

import org.apache.ignite.IgniteLogger;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

/**
 * Retryable sender with limited queue.
 */
public class RetryableSender<T> implements Runnable {
    /** Max sleep time seconds. */
    private static final int MAX_SLEEP_TIME_SECONDS = 10;

    /** Logger. */
    private IgniteLogger log;

    /** Queue. */
    private final LinkedBlockingDeque<T> queue;

    /** Send function. */
    private final Consumer<T> sndFn;

    /** Retry count. */
    private int retryCnt;

    /**
     * @param log Logger.
     */
    protected RetryableSender(IgniteLogger log, int cap, Consumer<T> sndFn) {
        this.log = log;
        this.sndFn = sndFn;
        queue = new LinkedBlockingDeque<>(cap);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        while (true) {
            T e = null;

            try {
                Thread.sleep(retryCnt * 1000);

                e = queue.takeFirst();
                sndFn.accept(e);

                retryCnt = 0;
            }
            catch (InterruptedException ex) {
                break;
            }
            catch (Exception ex) {
                if (retryCnt <= MAX_SLEEP_TIME_SECONDS)
                    retryCnt++;

                if (retryCnt == 0)
                    log.warning("Failed to send message with spans, will retry in " + retryCnt * 1000 + " ms", ex);

                addToSendQueue(e);
            }
        }
    }

    /**
     * @param e Span list.
     */
    public synchronized void addToSendQueue(T e) {
        if (e != null && !queue.offerLast(e)) {
            queue.removeFirst();
            queue.offerLast(e);
        }
    }
}
