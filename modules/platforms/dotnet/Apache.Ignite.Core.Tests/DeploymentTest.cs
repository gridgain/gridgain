﻿/*
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

#if !NETCOREAPP
namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Resource;
    using Apache.Ignite.Core.Tests.Process;
    using NUnit.Framework;

    /// <summary>
    /// Tests custom JAR deployment, classpath and IgniteHome logic.
    /// </summary>
    public class DeploymentTest
    {
        /** */
        private string _tempFolder;

        /// <summary>
        /// Tests the custom deployment where IGNITE_HOME can't be resolved, and there is a user-defined classpath.
        /// </summary>
        [Test]
        public void TestCustomDeployment()
        {
            var dllFolder = Path.Combine(_tempFolder, "dlls");
            var jarFolder = Path.Combine(_tempFolder, "jars");

            TestDeployment(dllFolder, jarFolder, true);
        }

        /// <summary>
        /// Tests that the most common scenario of `libs` directory being next to all application files works.
        /// This happens by default on build or publish.
        /// </summary>
        [Test]
        public void TestLibsDirectoryInAppPathDeployment()
        {
            var dllFolder = Path.Combine(_tempFolder, "foo");
            var jarFolder = Path.Combine(dllFolder, "libs");

            TestDeployment(dllFolder, jarFolder, false);
        }

        /// <summary>
        /// Tests that NuGet-based deployment where libs folder is inside `build/output` works.
        /// </summary>
        [Test]
        public void TestNuGetDeployment()
        {
            var dllFolder = Path.Combine(_tempFolder, "lib", "net40");
            var jarFolder = Path.Combine(_tempFolder, "build", "output", "libs");

            TestDeployment(dllFolder, jarFolder, true);
        }

        /// <summary>
        /// Tests missing JARs.
        /// </summary>
        [Test]
        public void TestMissingJarsCauseProperException()
        {
            var dllFolder = Path.Combine(_tempFolder, "foo");
            var jarFolder = Path.Combine(_tempFolder, "bar", "libs");
            var homeFolder = Directory.GetParent(jarFolder).FullName;

            DeployTo(dllFolder, jarFolder);

            // Remove some jars.
            Directory.GetFiles(jarFolder, "cache-api-1.0.0.jar").ToList().ForEach(File.Delete);

            // Start a node and check the exception.
            var exePath = Path.Combine(dllFolder, "Apache.Ignite.exe");
            var reader = new ListDataReader();

            var proc = IgniteProcess.Start(exePath, homeFolder, reader);

            // Wait for process to fail.
            Assert.IsNotNull(proc);
            Assert.IsTrue(proc.WaitForExit(30000));
            Assert.IsTrue(proc.HasExited);
            Assert.AreEqual(-1, proc.ExitCode);

            // Check error message.
            var output = reader.GetOutput();
            var expected =
                "ERROR: Apache.Ignite.Core.Common.IgniteException: Java class is not found " +
                "(did you set IGNITE_HOME environment variable?): " +
                "org/apache/ignite/internal/processors/platform/utils/PlatformUtils";

            CollectionAssert.Contains(output, expected);
        }

        /// <summary>
        /// Sets up the test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            _tempFolder = PathUtils.GetTempDirectoryName();
        }

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
            IgniteProcess.KillAll();

            Directory.Delete(_tempFolder, true);
        }

        /// <summary>
        /// Tests deployment to custom folders.
        /// </summary>
        private void TestDeployment(string dllFolder, string jarFolder, bool buildClasspath)
        {
            DeployTo(dllFolder, jarFolder);

            // Copy config
            var springPath = Path.GetFullPath("Config/Compute/compute-grid2.xml");
            var springFile = Path.GetFileName(springPath);
            File.Copy(springPath, Path.Combine(dllFolder, springFile));

            // Start a node and make sure it works properly
            var exePath = Path.Combine(dllFolder, "Apache.Ignite.exe");

            Assert.IsTrue(File.Exists(exePath));

            var args = new List<string>
            {
                "-springConfigUrl=" + springFile,
                "-assembly=" + Path.GetFileName(GetType().Assembly.Location),
                "-J-ea",
                "-J-Xms512m",
                "-J-Xmx512m"
            };

            if (buildClasspath)
            {
                args.Add("-jvmClasspath=" + string.Join(";", Directory.GetFiles(jarFolder)));
            }

            var reader = new ListDataReader();
            var proc = IgniteProcess.Start(exePath, string.Empty, reader, args.ToArray());

            Assert.IsNotNull(proc);

            if (proc.WaitForExit(300))
            {
                Assert.Fail("Node failed to start: " + string.Join("\n", reader.GetOutput()));
            }

            VerifyNodeStarted(exePath);
        }

        /// <summary>
        /// Deploys binaries to specified folder
        /// </summary>
        private void DeployTo(string folder, string jarFolder = null)
        {
            if (!Directory.Exists(folder))
            {
                Directory.CreateDirectory(folder);
            }

            if (!string.IsNullOrWhiteSpace(jarFolder) && !Directory.Exists(jarFolder))
            {
                Directory.CreateDirectory(jarFolder);
            }

            // Copy jars.
            var home = IgniteHome.Resolve();

            var jarNames = new[] {@"\ignite-core-", @"\cache-api-1.0.0.jar", @"\modules\spring\"};

            var jars = Directory.GetFiles(home, "*.jar", SearchOption.AllDirectories)
                .Where(jarPath => !jarPath.Contains("web-console"))
                .Where(jarPath => jarNames.Any(jarPath.Contains)).ToArray();

            Assert.Greater(jars.Length, 3);

            foreach (var jar in jars)
            {
                var fileName = Path.GetFileName(jar);
                Assert.IsNotNull(fileName);
                File.Copy(jar, Path.Combine(jarFolder ?? folder, fileName), true);
            }

            // Copy .NET binaries
            foreach (var type in new[]
            {
                typeof(IgniteRunner), typeof(Ignition), GetType(), typeof(ConfigurationManager)
            })
            {
                var asm = type.Assembly;
                Assert.IsNotNull(asm.Location);
                File.Copy(asm.Location, Path.Combine(folder, Path.GetFileName(asm.Location)));
            }

            // ReSharper disable once AssignNullToNotNullAttribute
            var cfgMan = Path.Combine(
                Path.GetDirectoryName(typeof(Ignition).Assembly.Location),
                "System.Configuration.ConfigurationManager.dll");

            File.Copy(cfgMan, Path.Combine(folder, Path.GetFileName(cfgMan)));
        }

        /// <summary>
        /// Verifies that custom-deployed node has started.
        /// </summary>
        private static void VerifyNodeStarted(string exePath)
        {
            using (var ignite = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = "Config/Compute/compute-grid1.xml",
            }))
            {
                Assert.IsTrue(ignite.WaitTopology(2));

                var remoteProcPath = ignite.GetCluster().ForRemotes().GetCompute().Call(new ProcessPathFunc());

                Assert.AreEqual(exePath, remoteProcPath);
            }
        }

        #pragma warning disable 649
        /// <summary>
        /// Function that returns process path.
        /// </summary>
        [Serializable]
        private class ProcessPathFunc : IComputeFunc<string>
        {
            [InstanceResource]
            // ReSharper disable once UnassignedField.Local
            private IIgnite _ignite;

            /** <inheritdoc /> */
            public string Invoke()
            {
                // Should be null
                var igniteHome = _ignite.GetConfiguration().IgniteHome;

                return typeof(IgniteRunner).Assembly.Location + igniteHome;
            }
        }
    }
}
#endif
