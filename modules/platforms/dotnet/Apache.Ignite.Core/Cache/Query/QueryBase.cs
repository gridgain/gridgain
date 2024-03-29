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

namespace Apache.Ignite.Core.Cache.Query
{
    using System;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Cache.Query;

    /// <summary>
    /// Base class for all Ignite cache entry queries.
    /// </summary>
    public abstract class QueryBase : IQueryBaseInternal
    {
        /// <summary> Default page size. </summary>
        public const int DefaultPageSize = 1024;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryBase"/> class.
        /// </summary>
        protected internal QueryBase()
        {
            PageSize = DefaultPageSize;
        }

        /// <summary>
        /// Local flag. When set query will be executed only on local node, so only local
        /// entries will be returned as query result.
        /// <para />
        /// Defaults to <c>false</c>.
        /// </summary>
        public bool Local { get; set; }

        /// <summary>
        /// Optional page size. If set to <c>0</c>, then <c>CacheQueryConfiguration.pageSize</c> is used.
        /// </summary>
        public int PageSize { get; set; }

        /// <summary>
        /// Writes this instance to a stream created with a specified delegate.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        internal abstract void Write(BinaryWriter writer, bool keepBinary);

        /** <inheritdoc /> */
        void IQueryBaseInternal.Write(BinaryWriter writer, bool keepBinary)
        {
            Write(writer, keepBinary);
        }

        /// <summary>
        /// Gets the interop opcode.
        /// </summary>
        internal abstract CacheOp OpId { get; }

        /** <inheritdoc /> */
        CacheOp IQueryBaseInternal.OpId
        {
            get { return OpId; }
        }

        /// <summary>
        /// Write query arguments.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="args">Arguments.</param>
        internal static void WriteQueryArgs(IBinaryRawWriter writer, object[] args)
        {
            if (args == null)
                writer.WriteInt(0);
            else
            {
                writer.WriteInt(args.Length);

                foreach (var arg in args)
                {
                    // Write DateTime as TimeStamp always, otherwise it does not make sense
                    // Wrapped DateTime comparison does not work in SQL
                    var dt = arg as DateTime?;  // Works with DateTime also

                    if (dt != null)
                        writer.WriteTimestamp(dt);
                    else
                        writer.WriteObject(arg);
                }
            }
        }
    }
}
