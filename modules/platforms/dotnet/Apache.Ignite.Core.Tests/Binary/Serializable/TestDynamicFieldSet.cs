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

namespace Apache.Ignite.Core.Tests.Binary.Serializable
{
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="DynamicFieldSetSerializable"/> serialization.
    /// </summary>
    public class TestDynamicFieldSet
    {
        /// <summary>
        /// Tests that dynamically added fields are deserialized correctly on local node.
        /// </summary>
        [Test]
        public void TestNewFieldsOnLocalNode()
        {
            
        }
        
        /// <summary>
        /// Tests that dynamically added fields are deserialized correctly on remote node.
        /// </summary>
        [Test]
        public void TestNewFieldsOnRemoteNode()
        {
            
        }

        /// <summary>
        /// Tests that dynamically removed fields are deserialized correctly on local node.
        /// </summary>
        [Test]
        public void TestMissingFieldsOnLocalNode()
        {
            
        }

        /// <summary>
        /// Tests that dynamically removed fields are deserialized correctly on remote node.
        /// </summary>
        [Test]
        public void TestMissingFieldsOnRemoteNode()
        {
            
        }
    }
}