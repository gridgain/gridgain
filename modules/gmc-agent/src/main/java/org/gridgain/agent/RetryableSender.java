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

package org.gridgain.agent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;
import org.apache.ignite.IgniteLogger;

/**
 * Retryable sender with limited queue.
 */
public class RetryableSender<T> implements Runnable, AutoCloseable {
    /** Max sleep time seconds. */
    private static final int MAX_SLEEP_TIME_SECONDS = 10;

    /** Batch size. */
    private static final int BATCH_SIZE = 10;

    /** Logger. */
    private IgniteLogger log;

    /** Queue. */
    private final LinkedBlockingDeque<List<T>> queue;

    /** Send function. */
    private final Consumer<List<T>> sndFn;

    /** Retry count. */
    int retryCnt;

    /** Executor service. */
    private final ExecutorService exSrvc = Executors.newSingleThreadExecutor();

    /**
     * @param log Logger.
     */
    public RetryableSender(IgniteLogger log, int cap, Consumer<List<T>> sndFn) {
        this.log = log;
        this.sndFn = sndFn;
        queue = new LinkedBlockingDeque<>(cap);
        exSrvc.submit(this);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        while (true) {
            List<T> e = null;

            try {
                Thread.sleep(Math.min(MAX_SLEEP_TIME_SECONDS, retryCnt) * 1000);

                e = queue.take();
                sndFn.accept(e);

                retryCnt = 0;
            }
            catch (InterruptedException ex) {
                break;
            }
            catch (Exception ex) {
                retryCnt++;

                if (retryCnt == 1)
                    log.warning("Failed to send message with spans, will retry in " + retryCnt * 1000 + " ms", ex);

                addToQueue(e);
            }
        }
    }

    /**
     * @param elements Elements to send.
     */
    public void send(List<T> elements) {
        if (elements != null)
            splitOnBatches(elements).forEach(this::addToQueue);
    }

    /**
     * @param list List.
     */
    private List<List<T>> splitOnBatches(List<T> list) {
        List<T> batch = new ArrayList<>();
        List<List<T>> res = new ArrayList<>();

        for (T e : list) {
            batch.add(e);

            if (batch.size() >= BATCH_SIZE) {
                res.add(batch);
                batch = new ArrayList<>();
            }
        }

        if (!batch.isEmpty())
            res.add(batch);

        return res;
    }

    /**
     * @param batch Batch.
     */
    private synchronized void addToQueue(List<T> batch) {
        if (!queue.offerLast(batch)) {
            queue.removeFirst();
            queue.offerLast(batch);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        exSrvc.shutdown();
    }
}
