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

namespace Apache.Ignite.Core.Tests.Client
{
    using Apache.Ignite.Core.Impl.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ClientProtocolVersion"/>.
    /// </summary>
    public class ClientProtocolVersionTests
    {
        /// <summary>
        /// Tests constructors.
        /// </summary>
        [Test]
        public void TestConstructor()
        {
            var v0 = new ClientProtocolVersion();
            Assert.AreEqual(0, v0.Major);
            Assert.AreEqual(0, v0.Minor);
            Assert.AreEqual(0, v0.Maintenance);
            
            var v1 = new ClientProtocolVersion(2, 4, 8);
            Assert.AreEqual(2, v1.Major);
            Assert.AreEqual(4, v1.Minor);
            Assert.AreEqual(8, v1.Maintenance);
        }
        
        /// <summary>
        /// Tests comparison for equality.
        /// </summary>
        [Test]
        public void TestEqualityComparison()
        {
            Assert.AreEqual(
                new ClientProtocolVersion(), 
                new ClientProtocolVersion());
            
            Assert.AreEqual(
                new ClientProtocolVersion(1, 2, 3), 
                new ClientProtocolVersion(1, 2, 3));
            
            Assert.AreNotEqual(
                new ClientProtocolVersion(1, 2, 3), 
                new ClientProtocolVersion(1, 2, 4));
            
            Assert.AreNotEqual(
                new ClientProtocolVersion(1, 2, 3), 
                new ClientProtocolVersion(1, 3, 3));
            
            Assert.AreNotEqual(
                new ClientProtocolVersion(1, 2, 3), 
                new ClientProtocolVersion(0, 2, 3));
        }
    }
}