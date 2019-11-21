using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Client;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;
using Apache.Ignite.Linq;
using Apache.Ignite.NLog;

namespace test_proj
{
    public static class Program
    {
        public static async Task Main()
        {
            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[] {"127.0.0.1:47500"}
                    },
                    SocketTimeout = TimeSpan.FromSeconds(0.3)
                },
                Logger = new IgniteNLogLogger()
            };

            using (var ignite = Ignition.Start(cfg))
            {
                var cacheCfg = new CacheConfiguration(
                    "cache1",
                    new QueryEntity(typeof(int), typeof(Person)));
                
                var cache = ignite.CreateCache<int, Person>(cacheCfg);
                
                cache.Put(1, new Person(1));
                Debug.Assert(1 == cache[1].Age);

                var resPerson = cache.AsCacheQueryable()
                    .Where(e => e.Key > 0 && e.Value.Name.StartsWith("Person"))
                    .Select(e => e.Value)
                    .Single();
                Debug.Assert(1 == resPerson.Age);

                using (var igniteThin = Ignition.StartClient(new IgniteClientConfiguration("127.0.0.1")))
                {
                    var cacheThin = igniteThin.GetCache<int, Person>(cacheCfg.Name);
                    var personThin = await cacheThin.GetAsync(1);
                    Debug.Assert("Person-1" == personThin.Name);

                    var personNames = cacheThin.AsCacheQueryable()
                        .Where(e => e.Key != 2 && e.Value.Age < 10)
                        .Select(e => e.Value.Name)
                        .ToArray();
                    Debug.Assert(personNames.SequenceEqual(new[] {"Person-1"}));
                }
            }
        }
    }

    public class Person
    {
        public Person(int age)
        {
            Age = age;
            Name = $"Person-{age}";
        }

        public string Name { get; }
        
        public int Age { get; }
    }
}
