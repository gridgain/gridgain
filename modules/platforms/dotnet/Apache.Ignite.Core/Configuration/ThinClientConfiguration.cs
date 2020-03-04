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
        /// Initializes a new instance of the <see cref="ThinClientConfiguration"/> class.
        /// </summary>
        public ThinClientConfiguration()
        {
            MaxActiveTxPerConnection = DefaultMaxActiveTxPerConnection;
        }

        /// <summary>
        /// Gets or sets active transactions count per connection limit.
        /// </summary>
        [DefaultValue(DefaultMaxActiveTxPerConnection)]
        public int MaxActiveTxPerConnection { get; set; }
    }
}
