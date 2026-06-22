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

#nullable disable

// ReSharper disable PartialTypeWithSinglePart
namespace Apache.Ignite.Core.Impl.Unmanaged.Jni
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Runtime.InteropServices;
    using System.Security;
    using System.Threading;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// JVM holder. Should exist once per domain.
    /// </summary>
    [SuppressUnmanagedCodeSecurity]
    internal sealed unsafe partial class Jvm
    {
        /** */
        // ReSharper disable once InconsistentNaming
        private const int JNI_VERSION_1_8 = 0x00010008;

        /** */
        // ReSharper disable once InconsistentNaming
        private const int JNI_VERSION_9 = 0x00090000;

        /** Options to enable startup on Java 9+. */
        public static readonly string[] Java9Options =
        {
            "--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED",
            "--add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED",
            "--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED",

            "--add-opens=java.base/jdk.internal.access=ALL-UNNAMED",
            "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED",
            "--add-opens=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED",
            "--add-opens=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED",
            "--add-opens=java.base/java.io=ALL-UNNAMED",
            "--add-opens=java.base/java.net=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/java.security.cert=ALL-UNNAMED",
            "--add-opens=java.base/java.util=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
            "--add-opens=java.base/java.math=ALL-UNNAMED",
            "--add-opens=java.base/java.time=ALL-UNNAMED",
            "--add-opens=java.base/sun.security.ssl=ALL-UNNAMED",
            "--add-opens=java.base/sun.security.x509=ALL-UNNAMED",
            "--add-opens=java.sql/java.sql=ALL-UNNAMED",
            "--add-opens=java.logging/java.util.logging=ALL-UNNAMED"
        };

        /** */
        private readonly IntPtr _jvmPtr;

        /** */
        private readonly JvmDelegates.AttachCurrentThread _attachCurrentThread;

        /** */
        private readonly MethodId _methodId;

        /** Callbacks. */
        private readonly Callbacks _callbacks;

        /** Thread exit callback id. */
        private readonly int _threadExitCallbackId;

        /** Static instance */
        private static volatile Jvm _instance;

        /** Sync. */
        private static readonly object SyncRoot = new object();

        /** Console writer. */
        private static readonly ConsoleWriter ConsoleWriter = new ConsoleWriter();

        /** Env for current thread. */
        [ThreadStatic] private static Env _env;

        /** Console writer flag. */
        private int _isConsoleWriterEnabled;

        /// <summary>
        /// Initializes a new instance of the <see cref="_instance"/> class.
        /// </summary>
        private Jvm(IntPtr jvmPtr)
        {
            Debug.Assert(jvmPtr != IntPtr.Zero);

            _jvmPtr = jvmPtr;

            var funcPtr = (JvmInterface**)jvmPtr;
            var func = **funcPtr;
            GetDelegate(func.AttachCurrentThread, out _attachCurrentThread);

            // JVM is a singleton, so this is one-time subscription.
            // This is a shortcut - we pass DetachCurrentThread pointer directly as a thread exit callback,
            // because signatures happen to match exactly.
            _threadExitCallbackId = UnmanagedThread.SetThreadExitCallback(func.DetachCurrentThread);

            var env = AttachCurrentThread();

            EnsureSupportedJdkVersion(env);

            _methodId = new MethodId(env);

            // Keep AppDomain check here to avoid JITting GetCallbacksFromDefaultDomain method on .NET Core.
            _callbacks = AppDomain.CurrentDomain.IsDefaultAppDomain()
                ? new Callbacks(env, this)
                : GetCallbacksFromDefaultDomain();
        }

        /// <summary>
        /// Gets the callbacks.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static Callbacks GetCallbacksFromDefaultDomain()
        {
            return GetCallbacksFromDefaultDomainImpl();
        }

        /// <summary>
        /// Gets or creates the JVM.
        /// </summary>
        /// <param name="options">JVM options.</param>
        public static Jvm GetOrCreate(IList<string> options)
        {
            lock (SyncRoot)
            {
                return _instance ?? (_instance = new Jvm(GetJvmPtr(options)));
            }
        }

        /// <summary>
        /// Gets the JVM.
        /// </summary>
        // ReSharper disable once ParameterOnlyUsedForPreconditionCheck.Global
        public static Jvm Get(bool ignoreMissing = false)
        {
            var res = _instance;

            if (res == null && !ignoreMissing)
            {
                throw new IgniteException("JVM has not been created.");
            }

            return res;
        }

        /// <summary>
        /// Gets the method IDs.
        /// </summary>
        public MethodId MethodId
        {
            get { return _methodId; }
        }

        /// <summary>
        /// Attaches current thread to the JVM and returns JNIEnv.
        /// </summary>
        public Env AttachCurrentThread()
        {
            if (_env == null)
            {
                IntPtr envPtr;
                var res = _attachCurrentThread(_jvmPtr, out envPtr, IntPtr.Zero);

                if (res != JniResult.Success)
                {
                    throw new IgniteException("AttachCurrentThread failed: " + res);
                }

                _env = new Env(envPtr, this);
                UnmanagedThread.EnableCurrentThreadExitEvent(_threadExitCallbackId, _jvmPtr);
            }

            return _env;
        }

        /// <summary>
        /// Attaches current thread to the JVM using known envPtr and returns JNIEnv.
        /// </summary>
        public Env AttachCurrentThread(IntPtr envPtr)
        {
            if (_env == null || _env.EnvPtr != envPtr)
            {
                _env = new Env(envPtr, this);
            }

            return _env;
        }

        /// <summary>
        /// Registers the callbacks.
        /// </summary>
        public void RegisterCallbacks(UnmanagedCallbacks cbs)
        {
            var id = _callbacks.RegisterHandlers(cbs);
            cbs.SetContext(id);
        }

        /// <summary>
        /// Releases the callbacks.
        /// </summary>
        public void ReleaseCallbacks(long igniteId)
        {
            _callbacks.ReleaseHandlers(igniteId);
        }

        /// <summary>
        /// Enables the Java console output propagation.
        /// </summary>
        public void EnableJavaConsoleWriter()
        {
            if (Interlocked.CompareExchange(ref _isConsoleWriterEnabled, 1, 0) == 0)
            {
                var writerId = _callbacks.RegisterConsoleWriter(ConsoleWriter);
                AppDomain.CurrentDomain.DomainUnload += (s, a) => _callbacks.ReleaseConsoleWriter(writerId);
            }
        }

        /// <summary>
        /// Gets the JVM pointer.
        /// </summary>
        private static IntPtr GetJvmPtr(IList<string> options)
        {
            IntPtr jvm;
            int existingJvmCount;

            // Use existing JVM if present.
            var res = JvmDll.Instance.GetCreatedJvms(out jvm, 1, out existingJvmCount);
            if (res != JniResult.Success)
            {
                throw new IgniteException("JNI_GetCreatedJavaVMs failed: " + res);
            }

            if (existingJvmCount > 0)
            {
                return jvm;
            }

            return CreateJvm(options);
        }

        /// <summary>
        /// Determines whether we are on Java 9.
        /// </summary>
        public static bool IsJava9()
        {
            var args = new JvmInitArgs
            {
                version = JNI_VERSION_9
            };

            // Returns error on Java 8 and lower.
            var res = JvmDll.Instance.GetDefaultJvmInitArgs(&args);
            return res == JniResult.Success;
        }

        /// <summary>
        /// Throws if the live JVM is older than the supported floor.
        /// The check runs after JVM creation but before any GG/Ignite class
        /// is looked up, so a misconfigured JAVA_HOME=JDK 8 produces a clear
        /// actionable error instead of a downstream "class not found".
        /// </summary>
        private static void EnsureSupportedJdkVersion(Env env)
        {
            const int minSupportedMajor = 17;

            string versionStr;
            using (var systemCls = env.FindClass("java/lang/System"))
            {
                var getPropertyId = env.GetStaticMethodId(
                    systemCls, "getProperty", "(Ljava/lang/String;)Ljava/lang/String;");

                using (var propName = env.NewString("java.specification.version"))
                {
                    long arg = propName.Target.ToInt64();
                    using (var result = env.CallStaticObjectMethod(systemCls, getPropertyId, &arg))
                    {
                        versionStr = result == null ? null : env.JStringToString(result.Target);
                    }
                }
            }

            var major = ParseJavaSpecMajor(versionStr);
            if (major < minSupportedMajor)
            {
                throw new IgniteException(
                    "Unsupported Java version: " + versionStr +
                    ". GridGain requires Java " + minSupportedMajor + " or later. " +
                    "Set JAVA_HOME (or IgniteConfiguration.JvmDllPath) to a JDK " +
                    minSupportedMajor + "+ installation.");
            }
        }

        /// <summary>
        /// Parses the major version from a java.specification.version string.
        /// Returns 8 for "1.8", 17 for "17", 21 for "21", 0 if unparseable.
        /// </summary>
        private static int ParseJavaSpecMajor(string versionStr)
        {
            if (string.IsNullOrEmpty(versionStr))
            {
                return 0;
            }

            if (versionStr.StartsWith("1.", StringComparison.Ordinal))
            {
                var dot = versionStr.IndexOf('.', 2);
                var tail = dot >= 0 ? versionStr.Substring(2, dot - 2) : versionStr.Substring(2);
                int oldFormat;
                return int.TryParse(tail, out oldFormat) ? oldFormat : 0;
            }

            int major;
            return int.TryParse(versionStr, out major) ? major : 0;
        }

        /// <summary>
        /// Creates the JVM.
        /// </summary>
        private static IntPtr CreateJvm(IList<string> options)
        {
            // The IsJava9 guard lets a misconfigured JDK 8 reach JVM creation
            // (without unknown --add-opens flags) so the post-create version
            // check in the Jvm constructor can throw an actionable error.
            if (IsJava9())
            {
                options = options == null
                    ? Java9Options.ToList()
                    : new List<string>(options.Concat(Java9Options));
            }

            var args = new JvmInitArgs
            {
                version = JNI_VERSION_1_8,
                nOptions = options.Count
            };

            var opts = GetJvmOptions(options);

            try
            {
                JniResult res;
                IntPtr jvm;

                fixed (JvmOption* optPtr = &opts[0])
                {
                    args.options = optPtr;

                    IntPtr unused;
                    res = JvmDll.Instance.CreateJvm(out jvm, out unused, &args);
                }

                if (res != JniResult.Success)
                {
                    throw new IgniteException("JNI_CreateJavaVM failed: " + res);
                }

                return jvm;
            }
            finally
            {
                foreach (var opt in opts)
                {
                    Marshal.FreeHGlobal(opt.optionString);
                }
            }
        }

        /// <summary>
        /// Gets the JVM options.
        /// </summary>
        private static JvmOption[] GetJvmOptions(IList<string> options)
        {
            var opt = new JvmOption[options.Count];

            for (var i = 0; i < options.Count; i++)
            {
                opt[i].optionString = Marshal.StringToHGlobalAnsi(options[i]);
            }

            return opt;
        }

        /// <summary>
        /// Gets the delegate.
        /// </summary>
        private static void GetDelegate<T>(IntPtr ptr, out T del)
        {
            del = (T) (object) Marshal.GetDelegateForFunctionPointer(ptr, typeof(T));
        }
    }
}
