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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests adding and removing binary types metadata dynamically.
    /// </summary>
    public class BinaryAddRemoveTypeTest : TestBase
    {
        [Test]
        public void TestAddRemoveTypeServerNode()
        {
            TestAddRemoveType(Ignite.GetBinary());
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
                TestAddRemoveType(thickClient.GetBinary());
            }
        }

        [Test]
        public void TestAddRemoveTypeThinClient()
        {
            using (var thinClient = Ignition.StartClient(new IgniteClientConfiguration("localhost")))
            {
                TestAddRemoveType(thinClient.GetBinary());
            }
        }

        private void TestAddRemoveType(IBinary binary)
        {
            var binaryType = binary.GetBinaryType(typeof(TestType));
            var binaryTypeById = binary.GetBinaryType(binaryType.TypeId);
            var binaryTypes = binary.GetBinaryTypes();

            Assert.AreEqual(binaryType, binaryTypeById);
            CollectionAssert.Contains(binaryTypes, binaryTypeById);

            binary.RemoveBinaryType(binaryType.TypeId);

            var binaryTypeById2 = binary.GetBinaryType(binaryType.TypeId);
            var binaryTypes2 = binary.GetBinaryTypes();

            Assert.IsNull(binaryTypeById2);
        }

        public class TestType
        {
            public int Id { get; set; }
        }
    }
}
