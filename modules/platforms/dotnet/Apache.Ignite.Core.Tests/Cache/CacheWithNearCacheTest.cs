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

namespace Apache.Ignite.Core.Tests.Cache
{
    using Apache.Ignite.Core.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ICache{TK,TV}.WithNearCache"/> functionality.
    /// See also <see cref="CacheNearTest"/>.
    /// </summary>
    public class CacheWithNearCacheTest : TestBase
    {
        /// <summary>
        /// Tests that near cache returns the same object instance as we put there.
        /// </summary>
        [Test]
        public void TestNearCacheReturnsSameObjectReference()
        {
            var cache = Ignite.GetOrCreateCache<int, Foo>("c").WithNearCache();

            var obj = new Foo(12);
            cache[1] = obj;

            var res = cache[1];
            Assert.AreSame(obj, res);
        }

        /** */
        private class Foo
        {
            public Foo(int val)
            {
                Val = val;
            }

            public int Val;
        }
        
    }
}
