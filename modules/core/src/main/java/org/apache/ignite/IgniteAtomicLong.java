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

package org.apache.ignite;

import java.io.Closeable;

/**
 * This interface provides a rich API for working with distributedly cached atomic long value.
 * <h1 class="header">Description</h1>
 * <p>
 * <h2 class="header">Functionality</h2>
 * Distributed atomic long includes the following main functionality:
 * <ul>
 * <li>
 * Method {@link #get()} gets current value of atomic long.
 * </li>
 * <li>
 * Various {@code get..(..)} methods get current value of atomic long
 * and increase or decrease value of atomic long.
 * </li>
 * <li>
 * Method {@link #addAndGet(long l)} sums {@code l} with current value of atomic long
 * and returns result.
 * </li>
 * <li>
 * Method {@link #incrementAndGet()} increases value of atomic long and returns result.
 * </li>
 * <li>
 * Method {@link #decrementAndGet()} decreases value of atomic long and returns result.
 * </li>
 * <li>
 * Method {@link #getAndSet(long l)} gets current value of atomic long and sets {@code l}
 * as value of atomic long.
 * </li>
 * <li>
 * Method {@link #name()} gets name of atomic long.
 * </li>
 * </ul>
 * <p>
 * <h2 class="header">Creating Distributed Atomic Long</h2>
 * Instance of distributed atomic long can be created by calling the following method:
 * <ul>
 *     <li>{@link Ignite#atomicLong(String, long, boolean)}</li>
 * </ul>
 * @see Ignite#atomicLong(String, long, boolean)
 */
public interface IgniteAtomicLong extends Closeable {
    /**
     * Name of atomic long.
     *
     * @return Name of atomic long.
     */
    public String name();

    /**
     * Gets current value of atomic long.
     *
     * @return Current value of atomic long.
     * @throws IgniteException If operation failed.
     */
    public long get() throws IgniteException;

    /**
     * Increments and gets current value of atomic long.
     *
     * @return Value.
     * @throws IgniteException If operation failed.
     */
    public long incrementAndGet() throws IgniteException;

    /**
     * Gets and increments current value of atomic long.
     *
     * @return Value.
     * @throws IgniteException If operation failed.
     */
    public long getAndIncrement() throws IgniteException;

    /**
     * Adds {@code l} and gets current value of atomic long.
     *
     * @param l Number which will be added.
     * @return Value.
     * @throws IgniteException If operation failed.
     */
    public long addAndGet(long l) throws IgniteException;

    /**
     * Gets current value of atomic long and adds {@code l}.
     *
     * @param l Number which will be added.
     * @return Value.
     * @throws IgniteException If operation failed.
     */
    public long getAndAdd(long l) throws IgniteException;

    /**
     * Decrements and gets current value of atomic long.
     *
     * @return Value.
     * @throws IgniteException If operation failed.
     */
    public long decrementAndGet() throws IgniteException;

    /**
     * Gets and decrements current value of atomic long.
     *
     * @return Value.
     * @throws IgniteException If operation failed.
     */
    public long getAndDecrement() throws IgniteException;

    /**
     * Gets current value of atomic long and sets new value {@code l} of atomic long.
     *
     * @param l New value of atomic long.
     * @return Value.
     * @throws IgniteException If operation failed.
     */
    public long getAndSet(long l) throws IgniteException;

    /**
     * Atomically compares current value to the expected value, and if they are equal, sets current value
     * to new value.
     *
     * @param expVal Expected atomic long's value.
     * @param newVal New atomic long's value to set if current value equal to expected value.
     * @return {@code True} if comparison succeeded, {@code false} otherwise.
     * @throws IgniteException If failed.
     */
    public boolean compareAndSet(long expVal, long newVal) throws IgniteException;

    /**
     * Gets status of atomic.
     *
     * @return {@code true} if atomic was removed from cache, {@code false} in other case.
     */
    public boolean removed();

    /**
     * Removes this atomic long.
     *
     * @throws IgniteException If operation failed.
     */
    @Override public void close();
}
