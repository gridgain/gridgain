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

namespace Apache.Ignite.Core.Tests
{
    using System;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IgniteUtils"/>.
    /// </summary>
    public class IgniteUtilsTest
    {
        /// <summary>
        /// Tests <see cref="IgniteUtils.EncodePeekModes"/>.
        /// </summary>
        [Test]
        public void TestEncodePeekModes()
        {
            Assert.AreEqual(Tuple.Create(0, false), EncodePeekModes(null));
            Assert.AreEqual(Tuple.Create(0, false), EncodePeekModes());
        }

        /// <summary>
        /// Convenience wrapper: returns tuple instead of using out.
        /// </summary>
        private Tuple<int, bool> EncodePeekModes(params CachePeekMode[] modes)
        {
            bool hasNativeNear;
            var encoded = IgniteUtils.EncodePeekModes(modes, out hasNativeNear);
            return Tuple.Create(encoded, hasNativeNear);
        }
    }
}