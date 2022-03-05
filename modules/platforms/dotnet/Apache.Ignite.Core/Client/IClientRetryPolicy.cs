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
    /// <summary>
    /// Client retry policy determines whether client operations that have failed due to a connection issue
    /// should be retried.
    /// </summary>
    public interface IClientRetryPolicy
    {
        /// <summary>
        /// Gets a value indicating whether a client operation that has failed due to a connection issue
        /// should be retried.
        /// </summary>
        /// <param name="context">Operation context.</param>
        /// <returns>
        /// <c>true</c> if the operation should be retried on another connection, <c>false</c> otherwise.
        /// </returns>
        bool ShouldRetry(IClientRetryPolicyContext context);
    }
}
