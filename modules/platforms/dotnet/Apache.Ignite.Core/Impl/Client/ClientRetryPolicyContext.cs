/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

namespace Apache.Ignite.Core.Impl.Client
{
    using System;
    using Apache.Ignite.Core.Client;

    /// <summary>
    /// Retry policy context.
    /// </summary>
    internal sealed class ClientRetryPolicyContext : IClientRetryPolicyContext
    {
        /// <summary>
        /// Initializes a new instance of <see cref="ClientRetryPolicyContext"/> class.
        /// </summary>
        /// <param name="configuration">Configuration.</param>
        /// <param name="operation">Operation.</param>
        /// <param name="iteration">Iteration.</param>
        /// <param name="exception">Exception.</param>
        public ClientRetryPolicyContext(
            IgniteClientConfiguration configuration,
            ClientOperationType operation,
            int iteration,
            Exception exception)
        {
            Configuration = configuration;
            Operation = operation;
            Iteration = iteration;
            Exception = exception;
        }

        /** <inheritDoc /> */
        public IgniteClientConfiguration Configuration { get; }

        /** <inheritDoc /> */
        public ClientOperationType Operation { get; }

        /** <inheritDoc /> */
        public int Iteration { get; }

        /** <inheritDoc /> */
        public Exception Exception { get; }
    }
}
