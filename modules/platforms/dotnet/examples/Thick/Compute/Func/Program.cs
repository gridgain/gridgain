﻿/*
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

namespace Apache.Ignite.Examples.Thick.Compute.Func
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core;
    using Apache.Ignite.Examples.Shared;
    using Apache.Ignite.Examples.Shared.Compute;

    /// <summary>
    /// This example demonstrates compute func execution.
    /// </summary>
    public static class Program
    {
        public static void Main()
        {
            using (IIgnite ignite = Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Closure execution example started.");

                // Split the string by spaces to count letters in each word in parallel.
                ICollection<string> words = "Count characters using closure".Split().ToList();

                Console.WriteLine();
                Console.WriteLine(">>> Calculating character count with manual reducing:");

                var res = ignite.GetCompute().Apply(new CharacterCountFunc(), words);

                int totalLen = res.Sum();

                Console.WriteLine(">>> Total character count: " + totalLen);
                Console.WriteLine();
                Console.WriteLine(">>> Calculating character count with reducer:");

                totalLen = ignite.GetCompute().Apply(new CharacterCountFunc(), words, new CharacterCountReducer());

                Console.WriteLine(">>> Total character count: " + totalLen);
                Console.WriteLine();
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}
