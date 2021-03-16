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
    using System.Text;
    using System.Text.RegularExpressions;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Communication.Tcp;
    using NUnit.Framework;

    /// <summary>
    /// Tests that Java console output is redirected to .NET console.
    /// </summary>
    public class ConsoleRedirectTest
    {
#if !NETCOREAPP
        /** */
        private const string PrintlnTask = "org.apache.ignite.platform.PlatformPrintlnTask";
#endif

        /** */
        private StringBuilder _outSb;

        /** */
        private StringBuilder _errSb;

        /** */
        private TextWriter _stdOut;

        /** */
        private TextWriter _stdErr;

        /// <summary>
        /// Sets up the test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            _stdOut = Console.Out;
            _stdErr = Console.Error;

            _outSb = new StringBuilder();
            Console.SetOut(new MyStringWriter(_outSb));

            _errSb = new StringBuilder();
            Console.SetError(new MyStringWriter(_errSb));
        }

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            MyStringWriter.Throw = false;

            Console.SetOut(_stdOut);
            Console.SetError(_stdErr);

            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the startup output.
        /// </summary>
        [Test]
        public void TestStartupOutput()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                Logger = null
            };

            using (Ignition.Start(cfg))
            {
                Assert.AreEqual(1, Regex.Matches(_outSb.ToString(), "ver=1, locNode=[a-fA-F0-9]{8,8}, servers=1, clients=0,").Count);
            }
        }

        /// <summary>
        /// Tests the exception in console writer.
        /// </summary>
        [Test]
        public void TestExceptionInWriterPropagatesToJavaAndBack()
        {
            MyStringWriter.Throw = true;

            var ex = Assert.Throws<IgniteException>(() => Ignition.Start(TestUtils.GetTestConfiguration()));
            Assert.AreEqual("foo", ex.Message);
        }

        /// <summary>
        /// Tests startup error in Java.
        /// </summary>
        [Test]
        public void TestStartupJavaError()
        {
            // Invalid config
            Assert.Throws<IgniteException>(() =>
                Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    CommunicationSpi = new TcpCommunicationSpi
                    {
                        IdleConnectionTimeout = TimeSpan.MinValue
                    },
                    Logger = null
                }));

            StringAssert.Contains("SPI parameter failed condition check: idleConnTimeout > 0", _errSb.ToString());
        }

#if !NETCOREAPP
        /// <summary>
        /// Tests the disabled redirect.
        /// </summary>
        [Test]
        public void TestDisabledRedirect()
        {
            // Run test in new process because JVM is initialized only once.
            const string envVar = "ConsoleRedirectTest.TestDisabledRedirect";

            if (Environment.GetEnvironmentVariable(envVar) == bool.TrueString)
            {
                var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration(false))
                {
                    Logger = null
                };

                Assert.IsTrue(cfg.RedirectJavaConsoleOutput);

                cfg.RedirectJavaConsoleOutput = false;

                using (Ignition.Start(cfg))
                {
                    Assert.AreEqual(string.Empty, _errSb.ToString());
                    Assert.AreEqual(string.Empty, _outSb.ToString());
                }
            }
            else
            {
                using (EnvVar.Set(envVar, bool.TrueString))
                {
                    TestUtils.RunTestInNewProcess(GetType().FullName, "TestDisabledRedirect");
                }
            }
        }

        /// <summary>
        /// Tests multiple appdomains and multiple console handlers.
        /// </summary>
        [Test]
        public void TestMultipleDomains()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                Logger = null
            };

            using (var ignite = Ignition.Start(cfg))
            {
                ignite.GetCompute().ExecuteJavaTask<string>(PrintlnTask, "[Primary Domain]");
                StringAssert.Contains("[Primary Domain]", _outSb.ToString());

                // Run twice
                RunInNewDomain("[Domain 2]");
                RunInNewDomain("[Domain 3]");

                // Check topology version (1 running + 2 started + 2 stopped = 5)
                Assert.AreEqual(5, ignite.GetCluster().TopologyVersion);

                // Check output from other domains
                var outTxt = _outSb.ToString();
                StringAssert.Contains("[Domain 2]", outTxt);
                StringAssert.Contains("[Domain 3]", outTxt);
            }
        }

        /// <summary>
        /// Runs the Ignite in a new domain.
        /// </summary>
        private static void RunInNewDomain(string arg)
        {
            AppDomain childDomain = null;

            try
            {
                childDomain = AppDomain.CreateDomain("Child", null, new AppDomainSetup
                {
                    ApplicationBase = AppDomain.CurrentDomain.SetupInformation.ApplicationBase,
                    ConfigurationFile = AppDomain.CurrentDomain.SetupInformation.ConfigurationFile,
                    ApplicationName = AppDomain.CurrentDomain.SetupInformation.ApplicationName,
                    LoaderOptimization = LoaderOptimization.MultiDomainHost
                });

                var type = typeof(IgniteRunner);
                Assert.IsNotNull(type.FullName);

                var runner = (IIgniteRunner)childDomain.CreateInstanceAndUnwrap(
                    type.Assembly.FullName, type.FullName);

                runner.Run(arg);
            }
            finally
            {
                if (childDomain != null)
                    AppDomain.Unload(childDomain);
            }
        }

        private interface IIgniteRunner
        {
            void Run(string arg);
        }

        private class IgniteRunner : MarshalByRefObject, IIgniteRunner
        {
            public void Run(string arg)
            {
                var ignite = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    IgniteInstanceName = "newDomainGrid",
                    Logger = null
                });

                ignite.GetCompute().ExecuteJavaTask<string>(PrintlnTask, arg);

                // Will be stopped automatically on domain unload.
            }
        }
#endif

        private class MyStringWriter : StringWriter
        {
            public static bool Throw { get; set; }

            public MyStringWriter(StringBuilder sb) : base(sb)
            {
                // No-op.
            }

            public override void Write(string value)
            {
                if (Throw)
                {
                    throw new Exception("foo");
                }

                base.Write(value);
            }
        }
    }
}
