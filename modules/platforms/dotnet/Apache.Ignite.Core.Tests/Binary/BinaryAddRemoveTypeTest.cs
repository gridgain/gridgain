/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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
    using System;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.Metadata;
    using NUnit.Framework;

    /// <summary>
    /// Tests adding and removing binary types metadata dynamically.
    /// </summary>
    public class BinaryAddRemoveTypeTest : TestBase
    {
        [TearDown]
        public void RemoveBinaryTypes()
        {
            foreach (var binaryType in Ignite.GetBinary().GetBinaryTypes())
            {
                Ignite.GetBinary().RemoveBinaryType(binaryType.TypeId);
            }
        }

        [Test]
        public void TestAddRemoveTypeServerNode()
        {
            var cache = Ignite.GetOrCreateCache<int, TestType>("c");
            TestRemoveType(Ignite.GetBinary(), val => cache.Put(1, val));
        }

        [Test]
        public void TestAddRemoveTypeClientNode()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientMode = true,
                IgniteInstanceName = "client"
            };

            using (var thickClient = Ignition.Start(cfg))
            {
                var cache = thickClient.GetOrCreateCache<int, TestType>("c");
                TestRemoveType(thickClient.GetBinary(), val => cache.Put(1, val));
            }
        }

        [Test]
        public void TestAddRemoveTypeThinClient()
        {
            using (var thinClient = Ignition.StartClient(new IgniteClientConfiguration("localhost")))
            {
                var cache = thinClient.GetOrCreateCache<int, TestType>("c");
                TestRemoveType(thinClient.GetBinary(), val => cache.Put(1, val));
            }
        }

        private static void TestRemoveType(IBinary binary, Action<TestType> putAction)
        {
            putAction(new TestType { Id = 1 });

            var binaryType = binary.GetBinaryType(typeof(TestType));
            var binaryTypeById = binary.GetBinaryType(binaryType.TypeId);
            var binaryTypes = binary.GetBinaryTypes();

            Assert.AreNotSame(BinaryType.Empty, binaryType);
            Assert.AreEqual(typeof(TestType).FullName, binaryType.TypeName);
            Assert.AreEqual(binaryType.TypeName, binaryTypeById.TypeName);
            CollectionAssert.Contains(binaryTypes.Select(x => x.TypeName), binaryTypeById.TypeName);

            CollectionAssert.AreEqual(new[] { "Id" }, binaryType.Fields);
            Assert.AreEqual("int", binaryType.GetFieldTypeName("Id"));

            // Trying to use a different field type fails.
            var ex = Assert.Throws<BinaryObjectException>(
                () => putAction(new TestType { Id = 2, WriteIdAsString = true }));

            Assert.AreEqual(
                "Field type mismatch detected [fieldName=Id, " +
                $"expectedType={BinaryTypeId.Int}, actualType={BinaryTypeId.String}]",
                ex.Message);

            // Remove binary type.
            binary.RemoveBinaryType(binaryType.TypeId);

            var binaryTypeById2 = binary.GetBinaryType(binaryType.TypeId);
            var binaryTypes2 = binary.GetBinaryTypes();

            Assert.AreSame(BinaryType.Empty, binaryTypeById2);
            CollectionAssert.DoesNotContain(binaryTypes2.Select(x => x.TypeName), binaryTypeById.TypeName);

            // Trying to use a different field type now works.
            putAction(new TestType { Id = 2, WriteIdAsString = true });

            var binaryType2 = binary.GetBinaryType(typeof(TestType));
            Assert.AreEqual(binaryType.TypeId, binaryType2.TypeId);

            CollectionAssert.AreEqual(new[] { "Id" }, binaryType2.Fields);
            Assert.AreEqual("String", binaryType2.GetFieldTypeName("Id"));
        }

        public class TestType : IBinarizable
        {
            public int Id { get; set; }

            public bool WriteIdAsString { get; set; }

            public void WriteBinary(IBinaryWriter writer)
            {
                if (WriteIdAsString)
                {
                    writer.WriteString("Id", Id.ToString());
                }
                else
                {
                    writer.WriteInt("Id", Id);
                }
            }

            public void ReadBinary(IBinaryReader reader)
            {
                Id = Convert.ToInt32(reader.ReadObject<object>("Id"));
            }
        }
    }
}
