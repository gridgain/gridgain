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

namespace Apache.Ignite.Core.Impl.Compute
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Compute.Closure;
    using Apache.Ignite.Core.Impl.Deployment;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Resource;

    /// <summary>
    /// Common compute execution logic.
    /// </summary>
    internal static class ComputeRunner
    {
        /// <summary>
        /// Performs full job execution routine: injects resources, wraps in try-catch and PeerAssemblyLoader,
        /// writes results to the stream.
        /// </summary>
        public static void ExecuteJobAndWriteResults<T>(IIgniteInternal ignite, PlatformMemoryStream stream, T job,
            Func<T, object> execFunc)
        {
            Debug.Assert(stream != null);
            Debug.Assert(ignite != null);
            Debug.Assert(job != null);
            Debug.Assert(execFunc != null);
            
            // 0. Inject resources.
            InjectResources(ignite, job);

            // 1. Execute job.
            object res;
            bool success;

            using (PeerAssemblyResolver.GetInstance(ignite, Guid.Empty))
            {
                try
                {
                    res = execFunc(job);
                    success = true;
                }
                catch (Exception e)
                {
                    res = e;
                    success = false;
                }
            }

            // 2. Try writing result to the stream.
            var writer = ignite.Marshaller.StartMarshal(stream);

            try
            {
                // 3. Marshal results.
                BinaryUtils.WriteInvocationResult(writer, success, res);
            }
            finally
            {
                // 4. Process metadata.
                ignite.Marshaller.FinishMarshal(writer);
            }
        }

        /// <summary>
        /// Performs compute-specific resource injection.
        /// </summary>
        public static void InjectResources(IIgniteInternal ignite, object job)
        {
            var injector = job as IComputeResourceInjector;

            if (injector != null)
                injector.Inject(ignite);
            else
                ResourceProcessor.Inject(job, ignite);
        }
    }
}