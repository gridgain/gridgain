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

package org.apache.ignite.agent.service.sender;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

/**
 * Retryable sender with limited queue.
 */
public abstract class RetryableSender<T> implements Runnable, AutoCloseable {
    /** Queue capacity. */
    protected static final int DEFAULT_QUEUE_CAP = 1;

    /** Batch size. */
    private static final int BATCH_SIZE = 10;

    /** Queue. */
    private final BlockingQueue<List<T>> queue;

    /** Executor service. */
    private final ExecutorService exSrvc;

    /** Logger. */
    protected final IgniteLogger log;

    /**
     * @param log Logger.
     * @param threadNamePrefix the prefix to use for the names of newly created threads.
     * @param cap Capacity.
     */
    protected RetryableSender(IgniteLogger log, String threadNamePrefix, int cap) {
        this.log = log;
        queue = new ArrayBlockingQueue<>(cap);
        
        exSrvc = Executors.newSingleThreadExecutor(new CustomizableThreadFactory(threadNamePrefix));
        exSrvc.submit(this);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        while (true) {
            List<T> e = null;

            try {
                e = queue.take();

                sendInternal(e);
            }
            catch (Exception ex) {
                if (X.hasCause(ex, InterruptedException.class)) {
                    U.quiet(true, "Caught interrupted exception: " + ex);

                    Thread.currentThread().interrupt();

                    break;
                }

                addToQueue(e);
            }
        }
    }

    /**
     * Abstract send method.
     * @param elements Elements.
     */
    protected abstract void sendInternal(List<T> elements) throws Exception;

    /** {@inheritDoc} */
    @Override public void close() {
        U.shutdownNow(getClass(), exSrvc, log);
    }

    /**
     * @param element Element to send.
     */
    public void send(T element) {
        if (element != null)
            addToQueue(Collections.singletonList(element));
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
    private void addToQueue(List<T> batch) {
        while (!queue.offer(batch))
            queue.poll();
    }
}
