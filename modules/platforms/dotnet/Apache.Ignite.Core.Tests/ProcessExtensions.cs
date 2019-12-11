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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Threading;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Tests.Process;

    /// <summary>
    /// Process extensions.
    /// </summary>
    public static class ProcessExtensions
    {
        /** */
        private const int ThreadAccessSuspendResume = 0x2;

        /** */
        [DllImport("kernel32.dll")]
        private static extern IntPtr OpenThread(int dwDesiredAccess, bool bInheritHandle, uint dwThreadId);

        /** */
        [DllImport("kernel32.dll")]
        private static extern uint SuspendThread(IntPtr hThread);

        /** */
        [DllImport("kernel32.dll")]
        private static extern int ResumeThread(IntPtr hThread);

        /// <summary>
        /// Suspends the specified process.
        /// </summary>
        /// <param name="process">The process.</param>
        public static void Suspend(this System.Diagnostics.Process process)
        {
            foreach (var thread in process.Threads.Cast<ProcessThread>())
            {
                var pOpenThread = OpenThread(ThreadAccessSuspendResume, false, (uint)thread.Id);

                if (pOpenThread == IntPtr.Zero)
                    break;

                SuspendThread(pOpenThread);
            }
        }
        /// <summary>
        /// Resumes the specified process.
        /// </summary>
        /// <param name="process">The process.</param>
        public static void Resume(this System.Diagnostics.Process process)
        {
            foreach (var thread in process.Threads.Cast<ProcessThread>())
            {
                var pOpenThread = OpenThread(ThreadAccessSuspendResume, false, (uint)thread.Id);

                if (pOpenThread == IntPtr.Zero)
                    break;

                ResumeThread(pOpenThread);
            }
        }
        
        /// <summary>
        /// Attaches the process console reader.
        /// </summary>
        public static void AttachProcessConsoleReader(this System.Diagnostics.Process proc, 
            params IIgniteProcessOutputReader[] outReaders)
        {
            var outReader = outReaders == null || outReaders.Length == 0
                ? (IIgniteProcessOutputReader) new IgniteProcessConsoleOutputReader()
                : new IgniteProcessCompositeOutputReader(outReaders);

            Attach(proc, proc.StandardOutput, outReader, false);
            Attach(proc, proc.StandardError, outReader, true);
        }
        
        
        /// <summary>
        /// Attach output reader to the process.
        /// </summary>
        /// <param name="proc">Process.</param>
        /// <param name="reader">Process stream reader.</param>
        /// <param name="outReader">Output reader.</param>
        /// <param name="err">Whether this is error stream.</param>
        private static void Attach(System.Diagnostics.Process proc, StreamReader reader, 
            IIgniteProcessOutputReader outReader, bool err)
        {
            new Thread(() =>
            {
                while (!proc.HasExited)
                    outReader.OnOutput(proc, reader.ReadLine(), err);
            }) {IsBackground = true}.Start();
        }
               
        public static void KillTree(this System.Diagnostics.Process process)
        {
            process.KillTree(TimeSpan.FromSeconds(2));
        }

        public static void KillTree(this System.Diagnostics.Process process, TimeSpan timeout)
        {
            if (Os.IsWindows)
            {
                string stdout;
                RunProcessAndWaitForExit(
                    "taskkill",
                    string.Format("/T /F /PID {0}", process.Id),
                    timeout,
                    out stdout);
            }
            else
            {
                var children = new HashSet<int>();
                GetAllChildIdsUnix(process.Id, children, timeout);
                foreach (var childId in children)
                {
                    KillProcessUnix(childId, timeout);
                }
                KillProcessUnix(process.Id, timeout);
            }
            
            if (!process.WaitForExit(1000))
            {
                throw new Exception("Failed to kill process: " + process.Id);
            }
        }

        private static int RunProcessAndWaitForExit(string fileName, string arguments, TimeSpan timeout, 
            out string stdout)
        {
            var startInfo = new ProcessStartInfo
            {
                FileName = fileName,
                Arguments = arguments,
                RedirectStandardOutput = true,
                UseShellExecute = false
            };

            var process = new System.Diagnostics.Process {StartInfo = startInfo};

            stdout = null;
            if (process.WaitForExit((int)timeout.TotalMilliseconds))
            {
                stdout = process.StandardOutput.ReadToEnd();
            }
            else
            {
                process.Kill();
            }

            return process.ExitCode;
        }

        private static void GetAllChildIdsUnix(int parentId, ISet<int> children, TimeSpan timeout)
        {
            string stdout;
            var exitCode = RunProcessAndWaitForExit(
                "pgrep",
                string.Format("-P {0}", parentId),
                timeout,
                out stdout);

            if (exitCode == 0 && !string.IsNullOrEmpty(stdout))
            {
                using (var reader = new StringReader(stdout))
                {
                    while (true)
                    {
                        var text = reader.ReadLine();
                        if (text == null)
                        {
                            return;
                        }

                        int id;
                        if (int.TryParse(text, out id))
                        {
                            children.Add(id);
                            // Recursively get the children
                            GetAllChildIdsUnix(id, children, timeout);
                        }
                    }
                }
            }
        }

        private static void KillProcessUnix(int processId, TimeSpan timeout)
        {
            string stdout;
            RunProcessAndWaitForExit(
                "kill",
                string.Format("-TERM {0}", processId),
                timeout,
                out stdout);
        }
    }
}