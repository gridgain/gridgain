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
    using System.Diagnostics;
    using System.IO;
    using System.Text.RegularExpressions;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using NUnit.Framework;

    /// <summary>
    /// Starts Java server nodes.
    /// </summary>
    public static class JavaServer
    {
        /** Maven command to execute the main class. */
        private const string MavenCommandExec = "mvn compile exec:java -D\"exec.mainClass\"=\"Runner\"";

        /// <summary>
        /// Starts a server node with a given version.
        /// </summary>
        /// <param name="version">Product version.</param>
        /// <returns>Disposable object to stop the server.</returns>
        public static IDisposable Start(string version)
        {
            IgniteArgumentCheck.NotNullOrEmpty(version, "version");

            var serverSourcePath = Path.Combine(
                TestUtils.GetDotNetSourceDir().FullName, 
                "Apache.Ignite.Core.Tests", 
                "JavaServer");
            
            // Replace version in pom.xml
            var pomFile = Path.Combine(serverSourcePath, "pom.xml");
            var pomContent = File.ReadAllText(pomFile);
            pomContent = Regex.Replace(pomContent, 
                @"<version>\d\.\d\.\d</version>",
                string.Format("<version>{0}</version>", version));
            File.WriteAllText(pomFile, pomContent);

            var shell = Os.IsWindows ? "cmd.exe" : "/bin/bash";
            
            var escapedCommand = MavenCommandExec.Replace("\"", "\\\"");

            var process = new System.Diagnostics.Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = shell,
                    Arguments = string.Format("-c \"{0}\"", escapedCommand),
                    UseShellExecute = false,
                    CreateNoWindow = true,
                    WorkingDirectory = serverSourcePath,
                    RedirectStandardOutput = true
                }
            };

            process.Start();

            // Wait for node to come up with a thin client connection.
            var started = TestUtils.WaitForCondition(() =>
            {
                try
                {
                    // Port 10890 is set in Runner.java
                    using (Ignition.StartClient(GetClientConfiguration()))
                    {
                        return true;
                    }
                }
                catch (Exception)
                {
                    return false;
                }
            }, 7000);

            if (started)
            {
                return new DisposeAction(() => process.Kill());
            }
            
            process.Kill();
            var output = process.StandardOutput.ReadToEnd() + process.StandardError.ReadToEnd();
            throw new Exception("Failed to start Java node: " + output);
        }

        /// <summary>
        /// Gets client configuration to connect to the Java server.
        /// </summary>
        public static IgniteClientConfiguration GetClientConfiguration()
        {
            return new IgniteClientConfiguration("127.0.0.1:10890");
        }
    }
}