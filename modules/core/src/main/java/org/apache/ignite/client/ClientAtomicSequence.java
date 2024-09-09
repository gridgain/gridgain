/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.client;

import org.apache.ignite.IgniteException;

import java.io.Closeable;

/**
 * Client API for distributed atomic sequence.
 *
 * <h1 class="header">Description</h1>
 * <p>
 * Distributed atomic sequence provides an efficient way to generate unique numbers.
 * Unlike {@link ClientAtomicLong}, atomic sequence only goes up, and uses batching to reserve a local range of values,
 * so that most operations do not require a network round-trip.
 * <ul>
 * <li>
 * Method {@link #get()} gets current value from atomic sequence.
 * </li>
 * <li>
 * Various {@code get..(..)} methods get current value from atomic sequence
 * and increase atomic sequences value.
 * </li>
 * <li>
 * Various {@code add..(..)} {@code increment(..)} methods increase atomic sequences value
 * and return increased value.
 * </li>
 * <li>
 * Method {@link #batchSize(int size)} sets batch size of current atomic sequence.
 * </li>
 * <li>
 * Method {@link #batchSize()} gets current batch size of atomic sequence.
 * </li>
 * <li>
 * Method {@link #name()} gets name of atomic sequence.
 * </li>
 * </ul>
 * <h2 class="header">Creating Distributed Atomic Sequence</h2>
 * To create a new sequence or get an existing sequence use one of the following methods:
 * <ul>
 *     <li>{@link IgniteClient#atomicSequence(String, long, boolean)}</li>
 *     <li>{@link IgniteClient#atomicSequence(String, ClientAtomicConfiguration, long, boolean)}</li>
 * </ul>
 * @see IgniteClient#atomicSequence(String, long, boolean)
 */
public interface ClientAtomicSequence extends Closeable {
    /**
     * Name of atomic sequence.
     *
     * @return Name of atomic sequence.
     */
    public String name();

    /**
     * Gets current value of atomic sequence.
     *
     * @return Value of atomic sequence.
     * @throws IgniteException If operation failed.
     */
    public long get() throws IgniteException;

    /**
     * Increments and returns the value of atomic sequence.
     *
     * @return Value of atomic sequence after increment.
     * @throws IgniteException If operation failed.
     */
    public long incrementAndGet() throws IgniteException;

    /**
     * Gets and increments current value of atomic sequence.
     *
     * @return Value of atomic sequence before increment.
     * @throws IgniteException If operation failed.
     */
    public long getAndIncrement() throws IgniteException;

    /**
     * Adds {@code l} elements to atomic sequence and gets value of atomic sequence.
     *
     * @param l Number of added elements.
     * @return Value of atomic sequence.
     * @throws IgniteException If operation failed.
     */
    public long addAndGet(long l) throws IgniteException;

    /**
     * Gets current value of atomic sequence and adds {@code l} elements.
     *
     * @param l Number of added elements.
     * @return Value of atomic sequence.
     * @throws IgniteException If operation failed.
     */
    public long getAndAdd(long l) throws IgniteException;

    /**
     * Gets local batch size for this atomic sequence.
     *
     * @return Sequence batch size.
     */
    public int batchSize();

    /**
     * Sets local batch size for atomic sequence.
     *
     * @param size Sequence batch size. Must be more than 0.
     */
    public void batchSize(int size);

    /**
     * Gets status of atomic sequence.
     *
     * @return {@code true} if atomic sequence was removed from cache, {@code false} otherwise.
     */
    public boolean removed();

    /**
     * Removes this atomic sequence.
     *
     * @throws IgniteException If operation failed.
     */
    @Override public void close();
}