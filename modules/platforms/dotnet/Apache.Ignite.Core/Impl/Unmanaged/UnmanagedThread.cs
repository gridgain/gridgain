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

namespace Apache.Ignite.Core.Impl.Unmanaged
{
    using System;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Unmanaged thread utils.
    /// </summary>
    internal static class UnmanagedThread
    {
        /** Destructor callback delegate (same for Windows and Linux). */
        private delegate void DestructorCallback(IntPtr dataPtr);

        /** Callback function pointer. */
        private static readonly IntPtr DestructorCallbackPtr =
            Marshal.GetFunctionPointerForDelegate((DestructorCallback) OnThreadExit);

        /** Storage index. */
        private static readonly int StorageIndex;

        /// <summary>
        /// Static initializer.
        /// </summary>
        static unsafe UnmanagedThread()
        {
            if (Os.IsWindows)
            {
                StorageIndex = NativeMethodsWindows.FlsAlloc(DestructorCallbackPtr);
            }
            else if (Os.IsLinux)
            {
                int tlsIndex;
                var res = NativeMethodsLinux.pthread_key_create(new IntPtr(&tlsIndex), DestructorCallbackPtr);
                CheckResult(res);

                StorageIndex = tlsIndex;
            }
            else
            {
                // TODO: Add MacOS support.
                throw new InvalidOperationException("Unsupported OS: " + Environment.OSVersion);
            }
        }

        /// <summary>
        /// Occurs just before a thread exits.
        /// Fired on that exact thread.
        /// </summary>
        public static event Action ThreadExit;

        /// <summary>
        /// Enables thread exit even for current thread.
        /// </summary>
        public static void EnableCurrentThreadExitEvent()
        {
            // Store any value so that destructor callback is fired.
            if (Os.IsWindows)
            {
                NativeMethodsWindows.FlsSetValue(StorageIndex, new IntPtr(1));
            }
            else if (Os.IsLinux)
            {
                NativeMethodsLinux.pthread_setspecific(StorageIndex, new IntPtr(1));
            }
            else
            {
                // TODO: Add MacOS support.
                throw new InvalidOperationException("Unsupported OS: " + Environment.OSVersion);
            }
        }

        /// <summary>
        /// Thread exit callback.
        /// </summary>
        private static void OnThreadExit(IntPtr dataPtr)
        {
            var handler = ThreadExit;
            if (handler != null)
            {
                handler();
            }
        }

        /// <summary>
        /// Checks native call result.
        /// </summary>
        private static void CheckResult(int res)
        {
            if (res != 0)
            {
                throw new InvalidOperationException("Native call failed: " + res);
            }
        }

        /// <summary>
        /// Windows imports.
        /// </summary>
        private static class NativeMethodsWindows
        {
            [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false,
                ThrowOnUnmappableChar = true)]
            public static extern int FlsAlloc(IntPtr destructorCallback);

            [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false,
                ThrowOnUnmappableChar = true)]
            [return: MarshalAs(UnmanagedType.Bool)]
            public static extern bool FlsSetValue(int dwFlsIndex, IntPtr lpFlsData);
        }

        /// <summary>
        /// Linux imports.
        /// </summary>
        private static class NativeMethodsLinux
        {
            // key is `typedef unsigned int pthread_key_t`
            [DllImport("libuv.so", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false,
                ThrowOnUnmappableChar = true)]

            public static extern int pthread_key_create(IntPtr key, IntPtr destructorCallback);

            [DllImport("libuv.so", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false,
                ThrowOnUnmappableChar = true)]
            public static extern int pthread_setspecific(int key, IntPtr value);
        }
    }
}
