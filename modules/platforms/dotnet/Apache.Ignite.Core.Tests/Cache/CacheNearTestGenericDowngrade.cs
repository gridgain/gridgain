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
    using System.Linq;
    using Apache.Ignite.Core.Impl.Cache.Near;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="NearCache" /> in generic downgrade mode (see comments in <see cref="NearCacheManager"/>).
    /// </summary>
    [TestFixture]
    public class CacheNearTestGenericDowngrade : CacheNearTest
    {
        /// <summary>
        /// Fixture set up.
        /// </summary>
        public override void FixtureSetUp()
        {
            base.FixtureSetUp();
            
            // Downgrade near cache on all nodes by using it with different generic parameters.
            foreach (var ignite in Ignition.GetAll().Where(i => !i.GetConfiguration().ClientMode))
            {
                var cache1 = ignite.GetCache<int, int>(DefaultCacheName);
                cache1[0] = 0;

                var cache2 = ignite.GetCache<string, string>(DefaultCacheName);
                cache2["0"] = "0";
                
                cache2.RemoveAll();
            }
        }
    }
}