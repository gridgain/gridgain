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

package org.apache.ignite.ml.inference.storage.model;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Implementation of {@link ModelStorageProvider} based on local {@link ConcurrentHashMap}.
 */
public class LocalModelStorageProvider implements ModelStorageProvider {
    /** Storage of the files and directories. */
    private final ConcurrentMap<String, FileOrDirectory> storage = new ConcurrentHashMap<>();

    /** Storage of the locks. */
    private final ConcurrentMap<String, WeakReference<Lock>> locks = new ConcurrentHashMap<>();

    /** */
    private final ThreadLocal<SynchronizationContext> syncCtx = ThreadLocal.withInitial(SynchronizationContext::new);

    /** {@inheritDoc} */
    @Override public FileOrDirectory get(String key) {
        acquireIfNeeded(key);

        return storage.get(key);
    }

    /** {@inheritDoc} */
    @Override public void put(String key, FileOrDirectory file) {
        acquireIfNeeded(key);

        storage.put(key, file);
    }

    /** {@inheritDoc} */
    @Override public void remove(String key) {
        acquireIfNeeded(key);

        storage.remove(key);
    }

    /** {@inheritDoc} */
    @Override public <T> T synchronize(Supplier<T> supplier) {
        if (syncCtx.get() != null)
            return supplier.get();
        else {
            SynchronizationContext ctx = new SynchronizationContext();

            syncCtx.set(ctx);

            try {
                return supplier.get();
            }
            finally {
                ctx.releaseLocks();

                syncCtx.remove();
            }
        }
    }

    /**
     * Acquires a lock for the given key if synchronization context is present.
     *
     * @param key Key to acquire lock.
     */
    private void acquireIfNeeded(String key) {
        SynchronizationContext ctx = syncCtx.get();

        if (ctx != null)
            ctx.heldLocks.add(lock(key));
    }

    /**
     * Ensures that a lock for the given key exists in the map and acquries it.
     *
     * @param key Key to acquire lock for.
     * @return Acquired lock.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    private Lock lock(String key) {
        Lock resLock;

        do {
            WeakReference<Lock> lockRef = locks.computeIfAbsent(key, k -> new WeakReference<>(new ReentrantLock()));

            resLock = lockRef.get();

            if (resLock == null)
                locks.remove(key, lockRef);
        }
        while (resLock == null);

        resLock.lock();

        return resLock;
    }

    /**
     * Context for {@link #synchronize(Supplier)} calls.
     */
    private static class SynchronizationContext {
        /** */
        List<Lock> heldLocks = new ArrayList<>(3);

        /**
         * Releases all locks.
         */
        private void releaseLocks() {
            for (Lock heldLock : heldLocks)
                heldLock.unlock();
        }
    }
}
