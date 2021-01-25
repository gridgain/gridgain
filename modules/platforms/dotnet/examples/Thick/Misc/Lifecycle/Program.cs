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

#pragma warning disable 649 // Readonly field is never assigned
namespace Apache.Ignite.Examples.Thick.Misc.Lifecycle
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Resource;
    using Apache.Ignite.Examples.Shared;

    /// <summary>
    /// This example shows how to provide your own <see cref="ILifecycleHandler"/> implementation
    /// to be able to hook into the Ignite node life cycle.
    /// </summary>
    public static class Program
    {
        public static void Main()
        {
            Console.WriteLine();
            Console.WriteLine(">>> Lifecycle example started.");

            // Create new configuration.
            var lifecycleAwareExample = new LifecycleHandlerExample();

            var cfg = new IgniteConfiguration(Utils.GetServerNodeConfiguration())
            {
                LifecycleHandlers = new[] {lifecycleAwareExample},
                IgniteInstanceName = "lifecycle-example"
            };

            // Provide lifecycle bean to configuration.
            using (Ignition.Start(cfg))
            {
                // Make sure that lifecycle bean was notified about Ignite startup.
                Console.WriteLine();
                Console.WriteLine(">>> Started (should be true): " + lifecycleAwareExample.Started);
            }

            // Make sure that lifecycle bean was notified about Ignite stop.
            Console.WriteLine();
            Console.WriteLine(">>> Started (should be false): " + lifecycleAwareExample.Started);

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        private class LifecycleHandlerExample : ILifecycleHandler
        {
            /** Auto-injected Ignite instance. */
            [InstanceResource]
            private IIgnite _ignite;

            /** <inheritDoc /> */
            public void OnLifecycleEvent(LifecycleEventType evt)
            {
                Console.WriteLine();
                Console.WriteLine(">>> Ignite lifecycle event occurred: " + evt);
                Console.WriteLine(">>> Ignite name: " + (_ignite != null ? _ignite.Name : "not available"));

                if (evt == LifecycleEventType.AfterNodeStart)
                    Started = true;
                else if (evt == LifecycleEventType.AfterNodeStop)
                    Started = false;
            }

            /// <summary>
            /// Started flag.
            /// </summary>
            public bool Started
            {
                get;
                private set;
            }
        }

    }
}
