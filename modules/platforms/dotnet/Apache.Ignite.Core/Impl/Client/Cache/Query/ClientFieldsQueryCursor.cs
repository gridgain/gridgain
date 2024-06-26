﻿/*
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

namespace Apache.Ignite.Core.Impl.Client.Cache.Query
{
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Client fields cursor.
    /// </summary>
    internal class ClientFieldsQueryCursor : ClientQueryCursorBase<IList<object>>, IFieldsQueryCursor
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ClientQueryCursor{TK, TV}" /> class.
        /// </summary>
        /// <param name="socket">Connection that holds the cursor.</param>
        /// <param name="cursorId">The cursor identifier.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        /// <param name="initialBatchStream">Optional stream with initial batch.</param>
        /// <param name="getPageOp">The get page op.</param>
        /// <param name="columns">The columns.</param>
        public ClientFieldsQueryCursor(ClientSocket socket, long cursorId, bool keepBinary,
            IBinaryStream initialBatchStream, ClientOp getPageOp, IList<string> columns)
            : base(socket, cursorId, keepBinary, initialBatchStream, getPageOp,
                r =>
                {
                    var res = new List<object>(columns.Count);

                    for (var i = 0; i < columns.Count; i++)
                    {
                        res.Add(r.ReadObject<object>());
                    }

                    return res;
                })
        {
            Debug.Assert(columns != null);

            FieldNames = new ReadOnlyCollection<string>(columns);
        }

        /** <inheritdoc /> */
        public IList<string> FieldNames { get; private set; }

        /** <inheritdoc /> */
        public IList<IQueryCursorField> Fields
        {
            get
            {
                throw IgniteClient.GetClientNotSupportedException();
            }
        }

        /// <summary>
        /// Reads the columns.
        /// </summary>
        internal static List<string> ReadColumns(IBinaryRawReader reader)
        {
            return reader.ReadStringCollection();
        }
    }
}
