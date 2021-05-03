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

namespace Apache.Ignite.Core.Binary
{
    using System;

    /// <summary>
    /// Converts <see cref="DateTime"/> values to Java Timestamp and back.
    /// </summary>
    public interface ITimestampConverter
    {
        /// <summary>Converts date to Java ticks.</summary>
        /// <param name="date">Date</param>
        /// <param name="high">High part (milliseconds).</param>
        /// <param name="low">Low part (nanoseconds)</param>
        void ToJavaTicks(DateTime date, out long high, out int low);

        /// <summary>Converts date from Java ticks.</summary>
        /// <param name="high">High part (milliseconds).</param>
        /// <param name="low">Low part (nanoseconds)</param>
        DateTime FromJavaTicks(long high, int low);
    }
}
