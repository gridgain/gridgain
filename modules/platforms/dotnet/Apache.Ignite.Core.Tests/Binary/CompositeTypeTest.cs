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

namespace Apache.Ignite.Core.Tests.Binary
{
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using NUnit.Framework;

    public class CompositeTypeTest
    {
        /// <summary>
        /// This reproducer demonstrates the problem that Ignite throws a <see cref="System.NullReferenceException"/>
        /// when serializing a composite structure having a child object with a schema, which is a sub-set of a cached
        /// schema.
        /// </summary>
        [Test]
        public void IgniteCanSerializeCompositeStructure()
        {
            var ignite = Ignition.Start(TestUtils.GetTestConfiguration());
            var igniteBinary = ignite.GetBinary();

            // GIVEN an Ignite data model where all the cache items are of the same IBinarizable "Item" type, which is
            // a composite type consisting of the specified fields and optional children of the same type.
            // AND there is a "simpleItem" consisting of fields "f1" and "f2"
            var simpleItem = new Item(new[] { "f1", "f2" });
            // AND there is a "compositeItem" having a child Item with a schema, which is a subset of the "simpleItem"'s
            // schema
            var compositeItem = new Item(new[] { "f3" }, new[] { new Item(new[] { "f1" }) });
            // AND Ignite serializes the "simpleItem"
            igniteBinary.ToBinary<IBinaryObject>(simpleItem);

            // WHEN Ignite serializes the "compositeItem"
            // THEN there should be no exceptions
            var result = igniteBinary.ToBinary<IBinaryObject>(compositeItem);

            Assert.NotNull(result);
        }

        /// <summary>
        /// A composite structure consisting of the specified fields and optional children of the same
        /// <see cref="Item"/> type.
        /// </summary>
        private class Item : IBinarizable
        {
            private readonly IEnumerable<string> _fieldNames;
            private IEnumerable<Item> _children;

            public Item(IEnumerable<string> fieldNames, IEnumerable<Item> children)
            {
                _fieldNames = fieldNames;
                _children = children;
            }

            public Item(IEnumerable<string> fieldNames)
                : this(fieldNames, Enumerable.Empty<Item>())
            {
            }

            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteCollection(nameof(_children), _children.ToList());
                foreach (var name in _fieldNames)
                {
                    // We do not need field values to reproduce the problem
                    writer.WriteString(name, null);
                }
            }

            public void ReadBinary(IBinaryReader reader)
            {
                _children = (ICollection<Item>)reader.ReadCollection(
                    nameof(_children),
                    size => new List<Item>(size),
                    (collection, obj) => ((List<Item>)collection).Add((Item)obj));

                foreach (var name in _fieldNames)
                {
                    reader.ReadString(name);
                }
            }
        }
    }
}
