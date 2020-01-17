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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Tests.Binary.Serializable;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="DynamicFieldSetSerializable"/> serialization in thin client.
    /// </summary>
    public class DynamicFieldSetTest : ClientTestBase
    {
        /// <summary>
        /// Tests that dynamically added and removed fields are deserialized correctly.
        /// This verifies proper metadata and schema handling.
        /// </summary>
        [Test]
        public void TestAddRemoveFieldsDynamically()
        {
            var cache1 = Ignition.GetIgnite().CreateCache<int, DynamicFieldSetSerializable>("c");
            var cache2 = Client.GetCache<int, DynamicFieldSetSerializable>("c");

            // Put/get without optional fields.
            var noFields = new DynamicFieldSetSerializable();
            cache1[1] = noFields;

            AssertExtensions.ReflectionEqual(noFields, cache1[1]);
            AssertExtensions.ReflectionEqual(noFields, cache2[1]);

            Assert.AreEqual(new[] {"WriteBar", "WriteFoo"}, GetFieldsServer());
            Assert.AreEqual(new[] {"WriteBar", "WriteFoo"}, GetFieldsClient());

            // Put/get with one optional field.
            var oneField = new DynamicFieldSetSerializable
            {
                Bar = "abc",
                WriteBar = true
            };
            cache1[2] = oneField;

            AssertExtensions.ReflectionEqual(oneField, cache1[2]);
            AssertExtensions.ReflectionEqual(oneField, cache2[2]);
            Assert.AreEqual(new[] {"Bar", "WriteBar", "WriteFoo"}, GetFieldsServer());
            Assert.AreEqual(new[] {"Bar", "WriteBar", "WriteFoo"}, GetFieldsClient());

            // Put/get with another optional field.
            var oneField2 = new DynamicFieldSetSerializable
            {
                Foo = 25,
                WriteFoo = true
            };
            cache1[3] = oneField2;

            AssertExtensions.ReflectionEqual(oneField2, cache1[3]);
            AssertExtensions.ReflectionEqual(oneField2, cache2[3]);

            Assert.AreEqual(new[] {"Bar", "Foo", "WriteBar", "WriteFoo"}, GetFieldsServer());
            Assert.AreEqual(new[] {"Bar", "Foo", "WriteBar", "WriteFoo"}, GetFieldsClient());
        }

        /// <summary>
        /// Gets the fields.
        /// </summary>
        private IEnumerable<string> GetFieldsServer()
        {
            return Ignition
                .GetIgnite()
                .GetBinary()
                .GetBinaryType(typeof(DynamicFieldSetSerializable))
                .Fields
                .OrderBy(f => f);
        }

        /// <summary>
        /// Gets the fields.
        /// </summary>
        private IEnumerable<string> GetFieldsClient()
        {
            return Client
                .GetBinary()
                .GetBinaryType(typeof(DynamicFieldSetSerializable))
                .Fields
                .OrderBy(f => f);
        }
    }
}