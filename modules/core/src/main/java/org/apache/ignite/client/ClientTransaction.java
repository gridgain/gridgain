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

package org.apache.ignite.client;

import org.apache.ignite.internal.client.thin.ClientServerError;

/**
 * Thin client transaction.
 */
public interface ClientTransaction extends AutoCloseable {
    /**
     * Commits this transaction.
     *
     * @throws ClientException If the transaction is already closed or transaction was started by another thread.
     * @throws ClientServerError If commit failed.
     */
    public void commit() throws ClientServerError, ClientException;

    /**
     * Rolls back this transaction.
     *
     * @throws ClientServerError If rollback failed.
     */
    public void rollback() throws ClientServerError, ClientException;

    /**
     * Ends the transaction. Transaction will be rolled back if it has not been committed.
     */
    @Override public void close();
}
