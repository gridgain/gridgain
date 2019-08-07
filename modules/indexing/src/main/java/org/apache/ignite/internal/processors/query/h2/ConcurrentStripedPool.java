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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.jetbrains.annotations.NotNull;

/**
 * Concurrent pool of object based on ConcurrentLinkedDeque.
 */
public class ConcurrentStripedPool<E> implements Iterable<E> {
    /** Stripe pools. */
    private final ConcurrentLinkedDeque<E>[] stripePools;

    /** Stripe pools size (calculates fast, optimistic and approximate). */
    private AtomicInteger[] stripeSize;

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

        stripePools = new ConcurrentLinkedDeque[stripes];
        stripeSize = new AtomicInteger[stripes];

        for (int i = 0; i < stripes; ++i) {
            stripePools[i] = new ConcurrentLinkedDeque<>();
            stripeSize[i] = new AtomicInteger();
        }
    }

    /**
     * Pushes an element onto the stack represented by this deque (in other
     * words, at the head of this deque) if it is possible to do so
     * immediately without violating capacity restrictions, throwing an
     * {@code IllegalStateException} if no space is currently available.
     *
     * @param e the element to push
     * @throws IllegalStateException if the element cannot be added at this
     *         time due to capacity restrictions
     * @throws ClassCastException if the class of the specified element
     *         prevents it from being added to this deque
     * @throws NullPointerException if the specified element is null and this
     *         deque does not permit null elements
     * @throws IllegalArgumentException if some property of the specified
     *         element prevents it from being added to this deque
     */
    public void push(E e) {
        int idx = (int)(Thread.currentThread().getId() % stripes);

        stripePools[idx].push(e);

        stripeSize[idx].incrementAndGet();
    }

    /**
     * Retrieves and removes the head of the queue represented by this deque
     * (in other words, the first element of this deque), or returns
     * {@code null} if this deque is empty.
     *
     * @return the first element of this deque, or {@code null} if
     *         this deque is empty
     */
    public E poll() {
        int idx = (int)(Thread.currentThread().getId() % stripes);

        E r = stripePools[idx].poll();

        if (r != null)
         stripeSize[idx].decrementAndGet();

        return r;
    }

    /**
     * @return Approximate size of pool (faster than ConcurrentLinkedDeque#size()).
     */
    public int size() {
        return stripeSize[(int)(Thread.currentThread().getId() % stripes)].get();
    }

    /**
     * Performs the given action for each element of the {@code Iterable}
     * until all elements have been processed or the action throws an
     * exception.  Unless otherwise specified by the implementing class,
     * actions are performed in the order of iteration (if an iteration order
     * is specified).  Exceptions thrown by the action are relayed to the
     * caller.
     *
     * @implSpec
     * <p>The default implementation behaves as if:
     * <pre>{@code
     *     for (T t : this)
     *         action.accept(t);
     * }</pre>
     *
     * @param action The action to be performed for each element
     * @throws NullPointerException if the specified action is null
     * @since 1.8
     */
    public void forEach(Consumer<? super E> action) {
        Objects.requireNonNull(action);

        for (int i = 0; i < stripes; ++i)
            stripePools[i].forEach(action);
    }

    /**
     * Removes all of the elements from this deque.
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
     * Returns a sequential {@code Stream} with this collection as its source.
     *
     * @return a sequential {@code Stream} over the elements in this collection
     * @since 1.8
     */
    public Stream<E> stream() {
        return StreamSupport.stream(spliterator(), false);
    }
}
