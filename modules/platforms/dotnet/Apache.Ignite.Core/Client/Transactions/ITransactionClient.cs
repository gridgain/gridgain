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

namespace Apache.Ignite.Core.Client.Transactions
{
    using System;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Thin client transaction.
    /// </summary>
    public interface ITransactionClient : IDisposable
    {
        /// <summary>
        /// Gets the transaction concurrency mode.
        /// </summary>
        TransactionConcurrency Concurrency { get; }

        /// <summary>
        /// Gets the transaction isolation level.
        /// </summary>
        TransactionIsolation Isolation { get; }

        /// <summary>
        /// Gets the timeout for this transaction. If transaction times
        /// out prior to it's completion, an exception will be thrown.
        /// <see cref="TimeSpan.Zero" /> for infinite timeout.
        /// </summary>
        TimeSpan Timeout { get; }

        /// <summary>
        /// Gets the transaction label.
        /// </summary>
        string Label { get; }

        /// <summary>
        /// Commits this transaction.
        /// </summary>
        void Commit();

        /// <summary>
        /// Rolls back this transaction.
        /// </summary>
        void Rollback();
    }
}
