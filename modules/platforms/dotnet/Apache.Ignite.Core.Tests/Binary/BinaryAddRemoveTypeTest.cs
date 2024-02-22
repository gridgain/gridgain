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
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Binary.Metadata;
    using NUnit.Framework;

    /// <summary>
    /// Tests adding and removing binary types metadata dynamically.
    /// </summary>
    public class BinaryAddRemoveTypeTest : TestBase
    {
        [Test]
        public void TestAddRemoveTypeServerNode()
        {
            // Register type by putting it into cache.
            var cache = Ignite.GetOrCreateCache<int, TestType>("c");
            cache[1] = new TestType { Id = 1 };

            TestRemoveType(Ignite.GetBinary(), val => cache.WithKeepBinary<int, IBinaryObject>().Put(1, val));
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
                // Register type by putting it into cache.
                var cache = thickClient.GetOrCreateCache<int, TestType>("c");
                cache[1] = new TestType { Id = 1 };

                TestRemoveType(thickClient.GetBinary(), val => cache.WithKeepBinary<int, IBinaryObject>().Put(1, val));
            }
        }

        [Test]
        public void TestAddRemoveTypeThinClient()
        {
            using (var thinClient = Ignition.StartClient(new IgniteClientConfiguration("localhost")))
            {
                // Register type by putting it into cache.
                var cache = thinClient.GetOrCreateCache<int, TestType>("c");
                cache[1] = new TestType { Id = 1 };

                TestRemoveType(thinClient.GetBinary(), val => cache.WithKeepBinary<int, IBinaryObject>().Put(1, val));
            }
        }

        private static void TestRemoveType(IBinary binary, Action<IBinaryObject> putAction)
        {
            var binaryType = binary.GetBinaryType(typeof(TestType));
            var binaryTypeById = binary.GetBinaryType(binaryType.TypeId);
            var binaryTypes = binary.GetBinaryTypes();

            Assert.AreNotSame(BinaryType.Empty, binaryType);
            Assert.AreEqual(typeof(TestType).FullName, binaryType.TypeName);
            Assert.AreEqual(binaryType.TypeName, binaryTypeById.TypeName);
            CollectionAssert.Contains(binaryTypes.Select(x => x.TypeName), binaryTypeById.TypeName);

            // Trying to use a different field type fails.
            // TODO: How does this not fail??
            var val = binary.GetBuilder(binaryType.TypeName).SetField(nameof(TestType.Id), "string").Build();
            putAction(val);
            Assert.AreEqual(binaryType.TypeId, val.GetBinaryType().TypeId);

            // Remove binary type.
            binary.RemoveBinaryType(binaryType.TypeId);

            var binaryTypeById2 = binary.GetBinaryType(binaryType.TypeId);
            var binaryTypes2 = binary.GetBinaryTypes();

            Assert.AreSame(BinaryType.Empty, binaryTypeById2);
            CollectionAssert.DoesNotContain(binaryTypes2.Select(x => x.TypeName), binaryTypeById.TypeName);

            // Trying to use a different field type now works.
        }

        public class TestType
        {
            public int Id { get; set; }
        }
    }
}
