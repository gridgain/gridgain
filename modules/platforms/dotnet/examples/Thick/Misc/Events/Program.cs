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

namespace Apache.Ignite.Examples.Thick.Misc.Events
{
    using System;
    using System.Linq;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Examples.Shared;
    using Apache.Ignite.Examples.Shared.Compute;
    using Apache.Ignite.Examples.Shared.Models;
    using LocalEventListener = Apache.Ignite.Examples.Shared.Events.LocalEventListener;

    /// <summary>
    /// This example demonstrates Ignite events.
    /// </summary>
    public static class Program
    {
        public static void Main()
        {
            using (IIgnite ignite = Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                Console.WriteLine(">>> Events example started.");
                Console.WriteLine();

                // Local listen example
                Console.WriteLine(">>> Listening for a local event...");

                ignite.GetEvents().EnableLocal(EventType.TaskExecutionAll);

                var listener = new LocalEventListener();
                ignite.GetEvents().LocalListen(listener, EventType.TaskExecutionAll);

                ExecuteTask(ignite);

                ignite.GetEvents().StopLocalListen(listener);

                Console.WriteLine(">>> Received events count: " + listener.EventsReceived);
                Console.WriteLine();
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Executes a task to generate events.
        /// </summary>
        /// <param name="ignite">Ignite instance.</param>
        private static void ExecuteTask(IIgnite ignite)
        {
            var employees = Enumerable.Range(1, 10).SelectMany(x => new[]
            {
                new Employee("Allison Mathis",
                    25300,
                    new Address("2702 Freedom Lane, San Francisco, CA", 94109),
                    new[] {"Development"}),

                new Employee("Breana Robbin",
                    6500,
                    new Address("3960 Sundown Lane, Austin, TX", 78130),
                    new[] {"Sales"})
            }).ToArray();

            ignite.GetCompute().Execute(new AverageSalaryTask(), employees);
        }
    }
}
