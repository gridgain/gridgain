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

package org.apache.ignite.ml.dataset.impl.cache.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Local storage used to keep partition {@code data}.
 */
class PartitionDataStorage implements AutoCloseable {
    /** Storage of a partition {@code data}. */
    private final ConcurrentMap<Integer, Object> storage = new ConcurrentHashMap<>();

    /** Storage of locks correspondent to partition {@code data} objects. */
    private final ConcurrentMap<Integer, Lock> locks = new ConcurrentHashMap<>();

    /** Schedured thread pool executor for cleaners. */
    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

    /** Time-to-live in seconds (-1 for an infinite lifetime). */
    private final long ttl;

    /**
     * Constructs a new instance of partition data storage.
     *
     * @param ttl Time-to-live in seconds (-1 for an infinite lifetime).
     */
    public PartitionDataStorage(long ttl) {
       this.ttl = ttl;
    }

    /**
     * Retrieves partition {@code data} correspondent to specified partition index if it exists in local storage or
     * loads it using the specified {@code supplier}. Unlike {@link ConcurrentMap#computeIfAbsent(Object, Function)},
     * this method guarantees that function will be called only once.
     *
     * @param <D> Type of data.
     * @param part Partition index.
     * @param supplier Partition {@code data} supplier.
     * @return Partition {@code data}.
     */
    <D> D computeDataIfAbsent(int part, Supplier<D> supplier) {
        Object data = storage.get(part);

        if (data == null) {
            Lock lock = locks.computeIfAbsent(part, p -> new ReentrantLock());

            lock.lock();
            try {
                data = storage.computeIfAbsent(part, p -> {
                    Object res = supplier.get();

                    if (ttl > -1)
                        executor.schedule(new Cleaner(part), ttl, TimeUnit.SECONDS);

                    return res;
                });
            }
            finally {
                lock.unlock();
            }
        }

        return (D)data;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // We assume that after close all objects stored in this storage will be collected by GC.
        executor.shutdownNow();
    }

    /**
     * Cleaner that removes partition data.
     */
    private class Cleaner implements Runnable  {
        /** Partition number. */
        private final int part;

        /**
         * Constructs a new instance of cleaner.
         *
         * @param part Partition number.
         */
        public Cleaner(int part) {
            this.part = part;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            Object data = storage.remove(part);
            locks.remove(part);

            if (data instanceof AutoCloseable) {
                AutoCloseable closeableData = (AutoCloseable) data;
                try {
                    closeableData.close();
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
