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
    using System.IO;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests IgniteManager class.
    /// </summary>
    public class IgniteManagerTest
    {
        /// <summary>
        /// Tests home dir resolver.
        /// </summary>
        [Test]
        public void TestIgniteHome()
        {
            var env = Environment.GetEnvironmentVariable(IgniteHome.EnvIgniteHome);

            Environment.SetEnvironmentVariable(IgniteHome.EnvIgniteHome, null);

            try
            {
                var home = IgniteHome.Resolve(null);
                Assert.IsTrue(Directory.Exists(home));

                // Invalid home.
                var cfg = new IgniteConfiguration {IgniteHome = @"c:\foo\bar"};
                var ex = Assert.Throws<IgniteException>(() => IgniteHome.Resolve(new IgniteConfiguration(cfg)));
                Assert.AreEqual(string.Format(
                    "IgniteConfiguration.IgniteHome is not valid: '{0}'", cfg.IgniteHome), ex.Message);
            }
            finally
            {
                // Restore
                Environment.SetEnvironmentVariable(IgniteHome.EnvIgniteHome, env);
            }
        }
    }
}