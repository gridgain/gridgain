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
    using System.Diagnostics;
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
        public void TestThreadExitFiresWhenEnabled()
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
                var t = new Thread(_ => UnmanagedThread.EnableCurrentThreadExitEvent())
                {
                    Priority = ThreadPriority.Lowest
                };
                t.Start();

                var threadExitCallbackCalled = evt.Wait(TimeSpan.FromSeconds(1));
                Assert.True(threadExitCallbackCalled);
            }
            finally
            {
                UnmanagedThread.ThreadExit -= callback;
            }
        }
    }
}
