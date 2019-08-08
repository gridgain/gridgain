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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.jetbrains.annotations.NotNull;

/**
 * Concurrent pool of object based on ConcurrentLinkedDeque.
 */
public class ConcurrentStripedPool<E> implements Iterable<E> {
    /** Stripe pools. */
    private final ConcurrentLinkedQueue<E>[] stripePools;

    /** Stripe pools size (calculates fast, optimistic and approximate). */
    private LongAdder[] stripeSize;

    /** Stripes count. */
    private final int stripes;

    /**
     * Constructor.
     *
     * @param stripes Count of stripes.
     */
    @SuppressWarnings("unchecked")
    public ConcurrentStripedPool(int stripes) {
        this.stripes = stripes;

        stripePools = new ConcurrentLinkedQueue[stripes];
        stripeSize = new LongAdder[stripes];

        for (int i = 0; i < stripes; ++i) {
            stripePools[i] = new ConcurrentLinkedQueue<>();
            stripeSize[i] = new LongAdder();
        }
    }

    /**
     * Pushes an element onto the pool.
     *
     * @param e the element to push
     * @throws NullPointerException if the specified element is null and this
     *         deque does not permit null elements
     */
    public void recycle(E e) {
        int idx = (int)(Thread.currentThread().getId() % stripes);

        stripePools[idx].add(e);

        stripeSize[idx].increment();
    }

    /**
     * Retrieves element from pool, or returns {@code null} if the pool is empty.
     *
     * @return the  element of the pool, or {@code null} if the pool is empty.
     */
    public E borrow() {
        int idx = (int)(Thread.currentThread().getId() % stripes);

        E r = stripePools[idx].poll();

        if (r != null)
         stripeSize[idx].decrement();

        return r;
    }

    /**
     * @return Approximate size of pool (faster than ConcurrentLinkedDeque#size()).
     */
    public int size() {
        return stripeSize[(int)(Thread.currentThread().getId() % stripes)].intValue();
    }

    /**
     * Performs the given action for each element of the pool
     * until all elements have been processed or the action throws an
     * exception. Exceptions thrown by the action are relayed to the
     * caller.
     *
     * @param action The action to be performed for each element
     * @throws NullPointerException if the specified action is null
     */
    @Override public void forEach(Consumer<? super E> action) {
        Objects.requireNonNull(action);

        for (int i = 0; i < stripes; ++i)
            stripePools[i].forEach(action);
    }

    /**
     * Removes all of the elements from the pool..
     */
    public void clear() {
        for (int i = 0; i < stripes; ++i)
            stripePools[i].clear();
    }

    /** {@inheritDoc} */
    @Override public @NotNull Iterator<E> iterator() {
        return new Iterator<E>() {
            int idx = 0;
            Iterator<E> it = stripePools[idx].iterator();

            @Override public boolean hasNext() {
                if (it.hasNext())
                    return true;

                idx++;

                if (idx < stripes) {
                    it = stripePools[idx].iterator();

                    return it.hasNext();
                }
                else
                    return false;
            }

            @Override public E next() {
                if (it.hasNext())
                    return it.next();

                idx++;

                if (idx < stripes) {
                    it = stripePools[idx].iterator();

                    return it.next();
                }
                else
                    throw new NoSuchElementException();
            }
        };
    }

    /**
     * Returns a sequential {@code Stream} of the pool.
     *
     * @return a sequential {@code Stream} over the elements iof the pool.
     */
    public Stream<E> stream() {
        return StreamSupport.stream(spliterator(), false);
    }
}
