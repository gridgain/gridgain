/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

namespace Apache.Ignite.Core.Tests.ApiParity
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Client.DataStructures;
    using Apache.Ignite.Core.Client.Services;
    using NUnit.Framework;

    /// <summary>
    /// Verifies that every synchronous, IO-bound public API on the thin client interfaces has a
    /// matching <c>*Async</c> overload (and vice versa). The thin client is IO-bound, so blocking
    /// sync-only APIs are inconsistent and inefficient; this test prevents such gaps from being
    /// reintroduced.
    /// <para />
    /// Local, non-IO facade methods (e.g. <c>GetCache</c>, <c>WithKeepBinary</c>, <c>ForServers</c>)
    /// are intentionally sync-only and listed in <see cref="SyncOnlyMembers"/>.
    /// <para />
    /// Interfaces that are deliberately not covered yet (transactions, which are thread-bound, and
    /// <see cref="Apache.Ignite.Core.Client.DataStructures.IIgniteSetClient{T}"/>, which implements the
    /// BCL <c>ISet&lt;T&gt;</c>) are simply not listed in <see cref="Interfaces"/>.
    /// </summary>
    public class ClientAsyncApiParityTest
    {
        /// <summary>
        /// Client interfaces that must expose full sync + async parity.
        /// </summary>
        private static readonly Type[] Interfaces =
        {
            typeof(IIgniteClient),
            typeof(ICacheClient<,>),
            typeof(IClientCluster),
            typeof(IClientClusterGroup),
            typeof(IServicesClient),
            typeof(IAtomicLongClient)
        };

        /// <summary>
        /// Members that are intentionally sync-only because they perform no server IO
        /// (they create local wrappers, return local state, or build projections).
        /// </summary>
        private static readonly HashSet<string> SyncOnlyMembers =
        [
            // IIgniteClient: local facades and state.
            "GetCache",
            "GetCluster",
            "GetBinary",
            "GetTransactions",
            "GetConfiguration",
            "GetConnections",
            "GetCompute",
            "GetServices",
            "GetDataStreamer",

            // ICacheClient: local wrappers.
            "WithKeepBinary",
            "WithExpiryPolicy",

            // IClientClusterGroup: local projections.
            "ForAttribute",
            "ForDotNet",
            "ForServers",
            "ForPredicate",

            // IServicesClient: local proxy and wrappers.
            "GetServiceProxy",
            "WithServerKeepBinary"
        ];

        /// <summary>
        /// Sync method base names whose async counterpart intentionally has a different shape, so
        /// strict parameter/return matching does not apply. These pairs still exist, they just
        /// cannot mirror each other exactly.
        /// <para />
        /// <c>TryGet(key, out value)</c> uses an <c>out</c> parameter that has no async equivalent;
        /// its async counterpart is <c>TryGetAsync(key)</c> returning <c>CacheResult&lt;TV&gt;</c>.
        /// </summary>
        private static readonly HashSet<string> ShapeMismatchMembers = ["TryGet"];

        /// <summary>
        /// Tests that every sync IO method has a matching async overload.
        /// </summary>
        [Test]
        [TestCaseSource(nameof(Interfaces))]
        public void TestSyncMethodsHaveAsyncOverloads(Type type)
        {
            var methods = GetApiMethods(type);

            var missing = new List<string>();

            foreach (var method in methods)
            {
                if (method.Name.EndsWith("Async", StringComparison.Ordinal))
                {
                    continue;
                }

                if (SyncOnlyMembers.Contains(method.Name) || ShapeMismatchMembers.Contains(method.Name))
                {
                    continue;
                }

                var asyncName = method.Name + "Async";

                var match = methods.FirstOrDefault(m =>
                    m.Name == asyncName && ParametersMatch(method, m) && IsAsyncReturnType(method, m));

                if (match == null)
                {
                    missing.Add(GetSignature(method));
                }
            }

            if (missing.Count > 0)
            {
                var sb = new StringBuilder();
                sb.AppendLine(type + " is missing async overloads for the following sync methods:");
                foreach (var s in missing)
                {
                    sb.AppendLine("  " + s);
                }

                sb.AppendLine("Add an *Async overload, or add the method to SyncOnlyMembers if it performs no IO.");

                Assert.Fail(sb.ToString());
            }
        }

        /// <summary>
        /// Tests that every async method has a matching sync overload (no async-only IO methods).
        /// </summary>
        [Test]
        [TestCaseSource(nameof(Interfaces))]
        public void TestAsyncMethodsHaveSyncOverloads(Type type)
        {
            var methods = GetApiMethods(type);

            var missing = new List<string>();

            foreach (var method in methods)
            {
                if (!method.Name.EndsWith("Async", StringComparison.Ordinal))
                {
                    continue;
                }

                var syncName = method.Name.Substring(0, method.Name.Length - "Async".Length);

                if (ShapeMismatchMembers.Contains(syncName))
                {
                    continue;
                }

                var match = methods.FirstOrDefault(m =>
                    m.Name == syncName && ParametersMatch(m, method) && IsAsyncReturnType(m, method));

                if (match == null)
                {
                    missing.Add(GetSignature(method));
                }
            }

            if (missing.Count > 0)
            {
                var sb = new StringBuilder();
                sb.AppendLine(type + " is missing sync overloads for the following async methods:");
                foreach (var s in missing)
                {
                    sb.AppendLine("  " + s);
                }

                Assert.Fail(sb.ToString());
            }
        }

        private static MethodInfo[] GetApiMethods(Type type)
        {
            return type
                .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Where(m => !m.IsSpecialName && m.GetCustomAttribute<ObsoleteAttribute>() == null)
                .ToArray();
        }

        private static bool ParametersMatch(MethodInfo sync, MethodInfo async)
        {
            var syncParams = sync.GetParameters();
            var asyncParams = async.GetParameters();

            if (syncParams.Length != asyncParams.Length)
            {
                return false;
            }

            return !syncParams.Where((t, i) => t.ParameterType != asyncParams[i].ParameterType).Any();
        }

        private static bool IsAsyncReturnType(MethodInfo sync, MethodInfo async)
        {
            var asyncReturn = async.ReturnType;

            if (sync.ReturnType == typeof(void))
            {
                return asyncReturn == typeof(Task);
            }

            return asyncReturn.IsGenericType && asyncReturn.GetGenericTypeDefinition() == typeof(Task<>);
        }

        private static string GetSignature(MethodInfo method)
        {
            var args = string.Join(", ", method.GetParameters().Select(p => p.ParameterType.Name + " " + p.Name));
            return method.ReturnType.Name + " " + method.Name + "(" + args + ")";
        }
    }
}
