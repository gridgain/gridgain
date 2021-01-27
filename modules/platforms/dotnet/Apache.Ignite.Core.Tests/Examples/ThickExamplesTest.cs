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

namespace Apache.Ignite.Core.Tests.Examples
{
    using System.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests thick examples.
    /// </summary>
    [Category(TestUtils.CategoryExamples)]
    public class ThickExamplesTest
    {
        /** */
        private static readonly Example[] ThickExamples = Example.AllExamples
            .Where(e => !e.IsThin && !e.IsClient && !e.RequiresExternalNode)
            .ToArray();

        /// <summary>
        /// Tests thick mode example.
        /// </summary>
        [Test, TestCaseSource(nameof(ThickExamples))]
        public void TestThickExample(Example example)
        {
            Assert.IsFalse(example.IsThin);

            example.Run();
        }
    }
}
