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

namespace Apache.Ignite.Core.Impl.Cluster
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Attribute node filter.
    ///
    /// The filter will evaluate to true if a node has provided attribute set to
    /// corresponding values.
    ///
    /// You can set user attribute using <see cref="IgniteConfiguration.UserAttributes"/> property. 
    /// </summary>
    public sealed class AttributeNodeFilter
    {
        /// <summary>
        /// Attributes dictionary match.
        /// </summary>
        public IDictionary<string, object> Attributes { get; set; }

        /// <summary>
        /// Initializes a new instance of <see cref="AttributeNodeFilter"/>.
        /// </summary>
        public AttributeNodeFilter()
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="AttributeNodeFilter"/>.
        /// </summary>
        /// <param name="attrName">Attribute name.</param>
        /// <param name="attrValue">Attribute value.</param>
        public AttributeNodeFilter(string attrName, object attrValue)
        {
            IgniteArgumentCheck.NotNullOrEmpty(attrName, "attrName");

            Attributes = new Dictionary<string, object>(1)
            {
                {attrName, attrValue}
            };
        }

        /// <summary>
        /// Initializes a new instance of <see cref="AttributeNodeFilter"/> from a binary reader.
        /// </summary>
        /// <param name="reader">Reader.</param>
        internal AttributeNodeFilter(IBinaryRawReader reader)
        {
            IgniteArgumentCheck.NotNull(reader, "reader");

            int count = reader.ReadInt();

            Debug.Assert(count > 0);

            Attributes = new Dictionary<string, object>(count);

            while (count > 0)
            {
                string attrKey = reader.ReadString();
                object attrVal = reader.ReadObject<object>();

                Debug.Assert(attrKey != null);

                Attributes[attrKey] = attrVal;

                count--;
            }
        }

        /// <summary>
        /// Serializes to a binarystream.
        /// </summary>
        /// <param name="writer">Writer.</param>
        internal void Write(IBinaryRawWriter writer)
        {
            writer.WriteInt(Attributes.Count);

            // Does not preserve ordering, it's fine.
            foreach (KeyValuePair<string, object> attr in Attributes)
            {
                writer.WriteString(attr.Key);
                writer.WriteObject(attr.Value);
            }
        }
    }
}