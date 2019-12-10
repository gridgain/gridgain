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
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Unmanaged;

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
            
            // TODO: Replace version in pom.xml

            var shell = Os.IsWindows ? "cmd.exe" : "/bin/bash";
            
            var escapedCommand = MavenCommandExec.Replace("\"", "\\\"");

            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = shell,
                    Arguments = string.Format("-c \"{0}\"", escapedCommand),
                    UseShellExecute = false,
                    CreateNoWindow = true,
                    WorkingDirectory = serverSourcePath
                }
            };

            process.Start();

            // Wait for node to come up using thin client
            // TODO: Use custom connector port to make sure we check the right node
            if (process.WaitForExit(5000))
            {
                
            }
            
            return new DisposeAction(() => process.Kill());
        }
    }
}