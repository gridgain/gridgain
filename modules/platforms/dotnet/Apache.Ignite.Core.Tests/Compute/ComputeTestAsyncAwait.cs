/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

namespace Apache.Ignite.Core.Tests.Compute
{
    using System.Threading.Tasks;
    using NUnit.Framework;

    /// <summary>
    /// Tests compute async continuation behavior.
    /// </summary>
    public class ComputeTestAsyncAwait : TestBase
    {
        /// <summary>
        /// Tests that RunAsync continuation does not capture Ignite threads.
        /// </summary>
        [Test]
        public async Task TestComputeRunAsyncContinuation()
        {
            await Ignite.GetCompute().RunAsync(new ComputeAction());

            StringAssert.StartsWith("Thread-", TestUtilsJni.GetJavaThreadName());
        }

        /// <summary>
        /// Tests that ExecuteAsync continuation does not capture Ignite threads.
        /// </summary>
        [Test]
        public async Task TestComputeExecuteAsyncContinuation()
        {
            await Ignite.GetCompute().ExecuteAsync(new StringLengthEmptyTask(), "abc");

            StringAssert.StartsWith("Thread-", TestUtilsJni.GetJavaThreadName());
        }
    }
}
