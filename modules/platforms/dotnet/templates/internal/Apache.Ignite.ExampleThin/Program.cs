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

namespace Apache.Ignite.Examples.Thin.ExampleProjectThin
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Examples.Shared;
    using Apache.Ignite.Examples.Shared.Models;

    /// <summary>
    /// TODO
    /// <para />
    /// This example requires an Ignite server node. You can start the node in any of the following ways:
    /// * docker run -p 10800:10800 apacheignite/ignite
    /// * dotnet run -p ServerNode.csproj
    /// * ignite.sh/ignite.bat from the distribution
    /// </summary>
    public static class Program
    {
        public static void Main()
        {
            using (IIgniteClient ignite = Ignition.StartClient(Utils.GetThinClientConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Example started.");

                // TODO

                Console.WriteLine();
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}
