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

namespace Apache.Ignite.Core.Tests.Client.Compute
{
    using System;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests client compute with disabled server-side settings.
    /// </summary>
    public class ComputeClientDisabledTests : ClientTestBase
    {
        /// <summary>
        /// Tests that Compute throws correct exception when not enabled on server.
        /// </summary>
        [Test]
        public void TestComputeThrowsCorrectExceptionWhenNotEnabledOnServer()
        {
            var ex = Assert.Throws<AggregateException>(
                () => Client.GetCompute().ExecuteJavaTask<int>("unused", null));

            var clientEx = (IgniteClientException) ex.GetInnermostException();

            Assert.AreEqual("Compute grid functionality is disabled for thin clients on server node. " +
                            "To enable it set up the " + typeof(ThinClientConfiguration).Name +
                            ".MaxActiveComputeTasksPerConnection property.", clientEx.Message);
        }
    }
}