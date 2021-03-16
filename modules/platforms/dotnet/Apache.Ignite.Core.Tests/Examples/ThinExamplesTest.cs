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
    using System.Reflection;
    using NUnit.Framework;

    /// <summary>
    /// Tests thin client example
    /// </summary>
    [Category(TestUtils.CategoryExamples)]
    public class ThinExamplesTest
    {
        /** */
        private static readonly Example[] ThinExamples = Example.AllExamples.Where(e => e.IsThin).ToArray();

        /// <summary>
        /// Sets up the fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var ignite = Ignition.Start(TestUtils.GetTestConfiguration());

            // Init default services.
            var asmFile = ExamplePaths.GetAssemblyPath(ExamplePaths.SharedProjFile);
            var asm = Assembly.LoadFrom(asmFile);
            var utils = asm.GetType("Apache.Ignite.Examples.Shared.Utils");

            Assert.IsNotNull(utils);

            utils.InvokeMember(
                "DeployDefaultServices",
                BindingFlags.Static | BindingFlags.Public | BindingFlags.InvokeMethod,
                null,
                null,
                new object[] {ignite});
        }

        /// <summary>
        /// Tears down the fixture.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the thin client example
        /// </summary>
        [Test, TestCaseSource(nameof(ThinExamples))]
        public void TestThinExample(Example example)
        {
            Assert.IsTrue(example.IsThin);

            example.Run();
        }
    }
}
