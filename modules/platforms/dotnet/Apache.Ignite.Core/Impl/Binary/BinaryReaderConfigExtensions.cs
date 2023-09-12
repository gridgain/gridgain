/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Reader extensions for configuration serialization.
    /// <para />
    /// See also <see cref="BinaryWriterConfigExtensions"/>.
    /// </summary>
    internal static class BinaryReaderConfigExtensions
    {
        /// <summary>
        /// Reads long as timespan with range checks.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns>TimeSpan.</returns>
        public static TimeSpan ConfigReadLongAsTimespan(this IBinaryRawReader reader)
        {
            return BinaryUtils.LongToTimeSpan(reader.ReadLong());
        }

        /// <summary>
        /// Reads the nullable TimeSpan.
        /// </summary>
        public static TimeSpan? ConfigReadTimeSpanNullable(this IBinaryRawReader reader)
        {
            return reader.ReadBoolean() ? reader.ConfigReadLongAsTimespan() : (TimeSpan?) null;
        }

        /// <summary>
        /// Reads the nullable int.
        /// </summary>
        public static int? ConfigReadIntNullable(this IBinaryRawReader reader)
        {
            return reader.ReadBoolean() ? reader.ReadInt() : (int?) null;
        }

        /// <summary>
        /// Reads the nullable long.
        /// </summary>
        public static long? ConfigReadLongNullable(this IBinaryRawReader reader)
        {
            return reader.ReadBoolean() ? reader.ReadLong() : (long?) null;
        }

        /// <summary>
        /// Reads the nullable bool.
        /// </summary>
        public static bool? ConfigReadBooleanNullable(this IBinaryRawReader reader)
        {
            return reader.ReadBoolean() ? reader.ReadBoolean() : (bool?) null;
        }
    }
}
