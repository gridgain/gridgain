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

namespace Apache.Ignite.Examples.Thick.Cache.BinaryMode
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Examples.Shared;

    /// <summary>
    /// This example works with cache entirely in binary mode: no classes are needed.
    /// </summary>
    public static class Program
    {
        private const string CacheName = "dotnet_binary_cache";

        private const string PersonType = "Person";

        private const string CompanyType = "Company";

        private const string NameField = "Name";

        private const string CompanyIdField = "CompanyId";

        private const string IdField = "Id";

        public static void Main()
        {
            using (IIgnite ignite = Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Binary mode example started.");

                // Create new cache and configure queries for Person and Company binary types.
                // Note that there are no such classes defined.
                var cache0 = ignite.GetOrCreateCache<object, object>(new CacheConfiguration
                {
                    Name = CacheName,
                    QueryEntities = new[]
                    {
                        new QueryEntity
                        {
                            KeyType = typeof(int),
                            ValueTypeName = PersonType,
                            Fields = new[]
                            {
                                new QueryField(NameField, typeof(string)),
                                new QueryField(CompanyIdField, typeof(int)),
                            },
                            Indexes = new[]
                            {
                                new QueryIndex(false, QueryIndexType.FullText, NameField),
                                new QueryIndex(false, QueryIndexType.Sorted, CompanyIdField)
                            }
                        },
                        new QueryEntity
                        {
                            KeyType = typeof(int),
                            ValueTypeName = CompanyType,
                            Fields = new[]
                            {
                                new QueryField(IdField, typeof(int)),
                                new QueryField(NameField, typeof(string))
                            }
                        }
                    }
                });

                // Switch to binary mode to work with data in serialized form.
                var cache = cache0.WithKeepBinary<int, IBinaryObject>();

                // Clean up caches on all nodes before run.
                cache.Clear();

                // Populate cache with sample data entries.
                PopulateCache(cache);

                // Run read & modify example.
                ReadModifyExample(cache);

                // Run SQL query example.
                SqlQueryExample(cache);

                // Run SQL query with join example.
                SqlJoinQueryExample(cache);

                // Run full text query example.
                FullTextQueryExample(cache);

                Console.WriteLine();
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Reads binary object fields and modifies them.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void ReadModifyExample(ICache<int, IBinaryObject> cache)
        {
            const int id = 1;

            IBinaryObject person = cache[id];

            string name = person.GetField<string>(NameField);

            Console.WriteLine();
            Console.WriteLine($">>> Name of the person with id {id}: {name}");

            // Modify the binary object.
            cache[id] = person.ToBuilder().SetField("Name", $"{name} Jr.").Build();

            Console.WriteLine($">>> Modified person with id {id}: {cache[1]}");
        }

        /// <summary>
        /// Queries names for all persons.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void SqlQueryExample(ICache<int, IBinaryObject> cache)
        {
            var qry = cache.Query(new SqlFieldsQuery("select name from Person order by name"));

            Console.WriteLine();
            Console.WriteLine(">>> All person names:");

            foreach (var row in qry)
                Console.WriteLine(">>>     " + row[0]);
        }

        /// <summary>
        /// Queries persons that work for company with provided name.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void SqlJoinQueryExample(ICache<int, IBinaryObject> cache)
        {
            const string orgName = "Apache";

            var qry = cache.Query(new SqlFieldsQuery(
                "select pers.Name from Person as pers, Company as comp where pers.CompanyId = comp.Id and comp.Name = ?",
                orgName)
            {
                EnableDistributedJoins = true,
                Timeout = new TimeSpan(0, 1, 0)
            });

            Console.WriteLine();
            Console.WriteLine(">>> Persons working for " + orgName + ":");

            foreach (var entry in qry)
                Console.WriteLine(">>>     " + entry[0]);
        }

        /// <summary>
        /// Queries persons that have a specific name using full-text query API.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void FullTextQueryExample(ICache<int, IBinaryObject> cache)
        {
            var qry = cache.Query(new TextQuery(PersonType, "Peters"));

            Console.WriteLine();
            Console.WriteLine(">>> Persons named Peters:");

            foreach (var entry in qry)
                Console.WriteLine(">>>     " + entry.Value);
        }

        /// <summary>
        /// Populate cache with data for this example.
        /// </summary>
        /// <param name="cache">Cache.</param>
        private static void PopulateCache(ICache<int, IBinaryObject> cache)
        {
            IBinary binary = cache.Ignite.GetBinary();

            // Populate persons.
            cache[1] = binary.GetBuilder(PersonType)
                .SetField(NameField, "James Wilson")
                .SetField(CompanyIdField, -1)
                .Build();

            cache[2] = binary.GetBuilder(PersonType)
                .SetField(NameField, "Daniel Adams")
                .SetField(CompanyIdField, -1)
                .Build();

            cache[3] = binary.GetBuilder(PersonType)
                .SetField(NameField, "Cristian Moss")
                .SetField(CompanyIdField, -1)
                .Build();

            cache[4] = binary.GetBuilder(PersonType)
                .SetField(NameField, "Allison Mathis")
                .SetField(CompanyIdField, -2)
                .Build();

            cache[5] = binary.GetBuilder(PersonType)
                .SetField(NameField, "Breana Robbin")
                .SetField(CompanyIdField, -2)
                .Build();

            cache[6] = binary.GetBuilder(PersonType)
                .SetField(NameField, "Philip Horsley")
                .SetField(CompanyIdField, -2)
                .Build();

            cache[7] = binary.GetBuilder(PersonType)
                .SetField(NameField, "James Peters")
                .SetField(CompanyIdField, -2)
                .Build();

            // Populate companies.
            cache[-1] = binary.GetBuilder(CompanyType)
                .SetField(NameField, "Apache")
                .SetField(IdField, -1)
                .Build();

            cache[-2] = binary.GetBuilder(CompanyType)
                .SetField(NameField, "Microsoft")
                .SetField(IdField, -2)
                .Build();
        }
    }
}
