/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.metastorage;

import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * API for distributed data storage with the ability to write into it.
 *
 * @see ReadableDistributedMetaStorage
 */
public interface DistributedMetaStorage extends ReadableDistributedMetaStorage {
    /**
     * Write value into distributed metastorage.
     *
     * @param key The key.
     * @param val Value to write. Must not be null.
     * @throws IgniteCheckedException If cluster is in deactivated state.
     */
    void write(@NotNull String key, @NotNull Serializable val) throws IgniteCheckedException;

    /**
     * Write value into distributed metastorage asynchronously.
     *
     * @param key The key.
     * @param val Value to write. Must not be null.
     * @throws IgniteCheckedException If cluster is in deactivated state.
     */
    GridFutureAdapter<?> writeAsync(@NotNull String key, @NotNull Serializable val) throws IgniteCheckedException;

    /**
     * Remove value from distributed metastorage.
     *
     * @param key The key.
     * @throws IgniteCheckedException If cluster is in deactivated state.
     */
    void remove(@NotNull String key) throws IgniteCheckedException;

    /**
     * Write value into distributed metastorage but only if current value matches the expected one.
     *
     * @param key The key.
     * @param expVal Expected value. Might be null.
     * @param newVal Value to write. Must not be null.
     * @throws IgniteCheckedException If cluster is in deactivated state.
     * @return {@code True} if expected value matched the actual one and write was completed successfully.
     *      {@code False} otherwise.
     */
    boolean compareAndSet(
        @NotNull String key,
        @Nullable Serializable expVal,
        @NotNull Serializable newVal
    ) throws IgniteCheckedException;

    /**
     * Write value into distributed metastorage asynchronously but only if current value matches the expected one.
     *
     * @param key The key.
     * @param expVal Expected value. Might be null.
     * @param newVal Value to write. Must not be null.
     * @throws IgniteCheckedException If cluster is in deactivated state.
     * @return {@code True} if expected value matched the actual one and write was completed successfully.
     *      {@code False} otherwise.
     */
    GridFutureAdapter<Boolean> compareAndSetAsync(
        @NotNull String key,
        @Nullable Serializable expVal,
        @NotNull Serializable newVal
    ) throws IgniteCheckedException;

    /**
     * Remove value from distributed metastorage but only if current value matches the expected one.
     *
     * @param key The key.
     * @param expVal Expected value. Must not be null.
     * @throws IgniteCheckedException If cluster is in deactivated state.
     * @return {@code True} if expected value matched the actual one and remove was completed successfully.
     *      {@code False} otherwise.
     */
    boolean compareAndRemove(@NotNull String key, @NotNull Serializable expVal) throws IgniteCheckedException;
}
