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

namespace Apache.Ignite.Core.Client
{
    using System;

    /// <summary>
    /// Retry policy context. See <see cref="IClientRetryPolicy.ShouldRetry"/>.
    /// </summary>
    public interface IClientRetryPolicyContext
    {
        /// <summary>
        /// Gets the client configuration.
        /// </summary>
        IgniteClientConfiguration Configuration { get; }

        /// <summary>
        /// Gets the operation type.
        /// </summary>
        ClientOperationType Operation { get; }

        /// <summary>
        /// Gets the current iteration.
        /// </summary>
        int Iteration { get; }

        /// <summary>
        /// Gets the exception that caused current retry iteration.
        /// </summary>
        Exception Exception { get; }
    }
}
