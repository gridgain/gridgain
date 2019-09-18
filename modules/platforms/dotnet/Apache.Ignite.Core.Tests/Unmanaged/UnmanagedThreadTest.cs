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

namespace Apache.Ignite.Core.Tests.Unmanaged
{
    using System;
    using System.Threading;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="UnmanagedThread"/>.
    /// </summary>
    public class UnmanagedThreadTest
    {
        /// <summary>
        /// Tests that ThreadExit event fires when enabled
        /// with <see cref="UnmanagedThread.EnableCurrentThreadExitEvent"/>.
        /// </summary>
        [Test]
        public void TestThreadExitFiresWhenEnabled([Values(true, false)] bool enableThreadExitCallback)
        {
            var evt = new ManualResetEventSlim();

            Action callback = () =>
            {
                if (Thread.CurrentThread.Priority == ThreadPriority.Lowest)
                {
                    evt.Set();
                }
            };

            UnmanagedThread.ThreadExit += callback;

            try
            {
                ParameterizedThreadStart threadStart = _ =>
                {
                    if (enableThreadExitCallback)
                        UnmanagedThread.EnableCurrentThreadExitEvent();
                };

                var t = new Thread(threadStart)
                {
                    // We use thread priority as a very hacky way to identify thread.
                    // Thread.CurrentThread.ManagedThreadId can't be used, because we receive callback after
                    // "managed" part of the thread was cleared up, and ManagedThreadId changes as a result.
                    // We could use unmanaged thread ID from the OS, but it requires P/Invoke
                    // and making it work on every OS is cumbersome.
                    Priority = ThreadPriority.Lowest
                };

                t.Start();

                var threadExitCallbackCalled = evt.Wait(TimeSpan.FromSeconds(1));
                Assert.AreEqual(enableThreadExitCallback, threadExitCallbackCalled);
            }
            finally
            {
                UnmanagedThread.ThreadExit -= callback;
            }
        }
    }
}
