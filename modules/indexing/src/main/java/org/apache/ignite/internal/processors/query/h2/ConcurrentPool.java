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

package org.apache.ignite.internal.processors.query.h2;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Concurrent pool of object based on ConcurrentLinkedDeque.
 */
public class ConcurrentPool<E> {
    /** Pool. */
    private final ConcurrentLinkedQueue<E> pool = new ConcurrentLinkedQueue<>();

    /** Ppool size (calculates fast, optimistic and approximate). */
    private final AtomicInteger size = new AtomicInteger(0);

    /** Max pool size. */
    private volatile int maxPoolSize;

    /**
     * Constructor.
     *
     * @param maxPoolSize Max pool size.
     */
    public ConcurrentPool(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    /**
     * Pushes an element onto the pool.
     *
     * @param e the element to push
     * @throws NullPointerException if the specified element is null and this deque does not permit null elements
     * @return {@code true} if the element is returned to the pool, {@code false} if the is no space at the pool.
     */
    public boolean recycle(E e) {
        int poolSize;
        do {
             poolSize = size.get();

            if (poolSize > maxPoolSize)
                return false;

        } while (!size.compareAndSet(poolSize, poolSize + 1));

        pool.add(e);

        return true;
    }

    /**
     * Retrieves element from pool, or returns {@code null} if the pool is empty.
     *
     * @return the  element of the pool, or {@code null} if the pool is empty.
     */
    public E borrow() {
        E r = pool.poll();

        if (r != null)
            size.decrementAndGet();

        return r;
    }

    /**
     * Performs the given action for each element of the pool until all elements have been processed or the action
     * throws an exception. Exceptions thrown by the action are relayed to the caller.
     *
     * @param action The action to be performed for each element
     * @throws NullPointerException if the specified action is null
     */
    public void forEach(Consumer<? super E> action) {
        Objects.requireNonNull(action);

        pool.forEach(action);
    }

    /**
     * Returns a sequential {@code Stream} of the pool.
     *
     * @return a sequential {@code Stream} over the elements iof the pool.
     */
    public Stream<E> stream() {
        return pool.stream();
    }

    /**
     * Removes all the elements from the pool on stop.
     */
    public void clear() {
        pool.clear();
        size.set(0);
    }

    /**
     * @param newSize New max pool size.
     */
    public void resize(int newSize) {
        maxPoolSize = newSize;
    }
}
