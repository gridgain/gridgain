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

namespace Apache.Ignite.Core.Configuration
{
    using System.ComponentModel;

    /// <summary>
    /// Server-side thin client specific configuration.
    /// </summary>
    public class ThinClientConfiguration
    {
        /// <summary>
        /// Default limit of active transactions count per connection.
        /// </summary>
        public const int DefaultMaxActiveTxPerConnection = 100;

        /// <summary>
        /// Default limit of active compute tasks per connection.
        /// </summary>
        public const int DefaultMaxActiveComputeTasksPerConnection = 0;

        /// <summary>
        /// Default value for <see cref="SendServerExceptionStackTraceToClient"/> property.
        /// </summary>
        public const bool DefaultSendServerExceptionStackTraceToClient = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="ThinClientConfiguration"/> class.
        /// </summary>
        public ThinClientConfiguration()
        {
            MaxActiveTxPerConnection = DefaultMaxActiveTxPerConnection;
            MaxActiveComputeTasksPerConnection = DefaultMaxActiveComputeTasksPerConnection;
            SendServerExceptionStackTraceToClient = DefaultSendServerExceptionStackTraceToClient;
        }

        /// <summary>
        /// Gets or sets active transactions count per connection limit.
        /// </summary>
        [DefaultValue(DefaultMaxActiveTxPerConnection)]
        public int MaxActiveTxPerConnection { get; set; }

        /// <summary>
        /// Gets or sets active compute tasks per connection limit.
        /// Value <c>0</c> means that compute grid functionality is disabled for thin clients.
        /// </summary>
        [DefaultValue(DefaultMaxActiveComputeTasksPerConnection)]
        public int MaxActiveComputeTasksPerConnection { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether server exception stack trace
        /// should be included in client exception details, or just the message.
        /// <para />
        /// Default is <c>false</c>.
        /// </summary>
        [DefaultValue(DefaultSendServerExceptionStackTraceToClient)]
        public bool SendServerExceptionStackTraceToClient { get; set; }
    }
}
