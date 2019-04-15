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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.io.Serializable;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Bridge interface to access data storage in {@link DistributedMetaStorageImpl}.
 */
interface DistributedMetaStorageBridge {
    /**
     * Get data by key.
     *
     * @param globalKey The key.
     * @param unmarshal Whether the value should be unmarshalled or not.
     * @return Value associated with the key.
     * @throws IgniteCheckedException If reading or unmarshalling failed.
     */
    Serializable read(String globalKey, boolean unmarshal) throws IgniteCheckedException;

    /**
     * Iterate over all values corresponding to the keys with given prefix. It is guaranteed that iteration will be
     * executed in ascending keys order.
     *
     * @param globalKeyPrefix Prefix for the keys that will be iterated.
     * @param cb Callback that will be applied to all {@code <key, value>} pairs.
     * @throws IgniteCheckedException If reading or unmarshalling failed.
     */
    void iterate(
        String globalKeyPrefix,
        BiConsumer<String, ? super Serializable> cb,
        boolean unmarshal
    ) throws IgniteCheckedException;

    /**
     * Write data into storage.
     *
     * @param globalKey The key.
     * @param valBytes Value bytes.
     * @throws IgniteCheckedException If some IO problem occured.
     */
    void write(String globalKey, @Nullable byte[] valBytes) throws IgniteCheckedException;

    /**
     * Invoked when update message was received. Prepares storage to the writing of new value and notifies listeners
     * (optionally).
     *
     * @param histItem Update data.
     * @throws IgniteCheckedException If some IO or unmarshalling errors occured.
     */
    void onUpdateMessage(
        DistributedMetaStorageHistoryItem histItem
    ) throws IgniteCheckedException;

    /**
     * Remove information about the specific update from the history.
     *
     * @param ver Specific version for which the update information should be deleted.
     * @throws IgniteCheckedException If some IO error occured.
     */
    void removeHistoryItem(long ver) throws IgniteCheckedException;

    /**
     * Returns all {@code <key, value>} pairs currently stored in distributed metastorage. Values are not unmarshalled.
     *
     * @return Array of all keys and values.
     */
    DistributedMetaStorageKeyValuePair[] localFullData() throws IgniteCheckedException;
}
