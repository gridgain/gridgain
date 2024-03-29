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

namespace Apache.Ignite.Core.Tests.Binary
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
#if NETCOREAPP
    using System.Reflection.Emit;
#endif
#if !NETCOREAPP
    using System.Linq;
#endif
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using NUnit.Framework;

    /// <summary>
    /// <see cref="TypeResolver"/> tests.
    /// </summary>
    public class TypeResolverTest
    {
        /// <summary>
        /// Tests basic types.
        /// </summary>
        [Test]
        public void TestBasicTypes()
        {
            var resolver = new TypeResolver();

            Assert.AreEqual(typeof(int), resolver.ResolveType("System.Int32"));
            Assert.AreEqual(GetType(), resolver.ResolveType(GetType().FullName));

            Assert.IsNull(resolver.ResolveType("invalidType"));
        }

        /// <summary>
        /// Tests basic types.
        /// </summary>
        [Test]
        public void TestBasicTypesSimpleMapper()
        {
            var resolver = new TypeResolver();
            var mapper = BinaryBasicNameMapper.SimpleNameInstance;

            Assert.AreEqual(typeof(int), resolver.ResolveType("Int32", nameMapper: mapper));
            Assert.AreEqual(GetType(), resolver.ResolveType("TypeResolverTest", nameMapper: mapper));

            Assert.IsNull(resolver.ResolveType("invalidType", nameMapper: mapper));
        }

        /// <summary>
        /// Tests generic type resolve.
        /// </summary>
        [Test]
        public void TestGenerics()
        {
            var testTypes = new[]
            {
                typeof (TestGenericBinarizable<int>),
                typeof (TestGenericBinarizable<string>),
                typeof (TestGenericBinarizable<TestGenericBinarizable<int>>),
                typeof (TestGenericBinarizable<List<Tuple<int, string>>>),
                typeof (TestGenericBinarizable<List<TestGenericBinarizable<List<Tuple<int, string>>>>>),
                typeof (List<TestGenericBinarizable<List<TestGenericBinarizable<List<Tuple<int, string>>>>>>),
                typeof (TestGenericBinarizable<int, string>),
                typeof (TestGenericBinarizable<int, TestGenericBinarizable<string>>),
                typeof (TestGenericBinarizable<int, string, Type>),
                typeof (TestGenericBinarizable<int, string, TestGenericBinarizable<int, string, Type>>)
            };

            foreach (var type in testTypes)
            {
                // Without assembly
                var resolvedType = new TypeResolver().ResolveType(type.FullName);
                Assert.AreEqual(type.FullName, resolvedType.FullName);

                // With assembly
                resolvedType = new TypeResolver().ResolveType(type.FullName, type.Assembly.FullName);
                Assert.AreEqual(type.FullName, resolvedType.FullName);

                // Assembly-qualified
                resolvedType = new TypeResolver().ResolveType(type.AssemblyQualifiedName);
                Assert.AreEqual(type.FullName, resolvedType.FullName);
            }
        }

        /// <summary>
        /// Tests generic type resolve.
        /// </summary>
        [Test]
        public void TestGenericsSimpleName()
        {
            var resolver = new TypeResolver();
            var mapper = BinaryBasicNameMapper.SimpleNameInstance;

            Assert.AreEqual(typeof(TestGenericBinarizable<int>),
                resolver.ResolveType("TestGenericBinarizable`1[[Int32]]", nameMapper: mapper));

            Assert.IsNull(resolver.ResolveType("TestGenericBinarizable`1[[Invalid-Type]]", nameMapper: mapper));

            var testTypes = new[]
            {
                typeof (TestGenericBinarizable<int>),
                typeof (TestGenericBinarizable<string>),
                typeof (TestGenericBinarizable<TestGenericBinarizable<int>>),
                typeof (TestGenericBinarizable<List<Tuple<int, string>>>),
                typeof (TestGenericBinarizable<List<TestGenericBinarizable<List<Tuple<int, string>>>>>),
                typeof (List<TestGenericBinarizable<List<TestGenericBinarizable<List<Tuple<int, string>>>>>>),
                typeof (TestGenericBinarizable<int, string>),
                typeof (TestGenericBinarizable<int, TestGenericBinarizable<string>>),
                typeof (TestGenericBinarizable<int, string, Type>),
                typeof (TestGenericBinarizable<int, string, TestGenericBinarizable<int, string, Type>>)
            };

            foreach (var type in testTypes)
            {
                var typeName = mapper.GetTypeName(type.AssemblyQualifiedName);
                var resolvedType = resolver.ResolveType(typeName, nameMapper: mapper);
                Assert.AreEqual(type, resolvedType);
            }
        }

        /// <summary>
        /// Tests array type resolve.
        /// </summary>
        [Test]
        public void TestArrays()
        {
            var resolver = new TypeResolver();

            Assert.AreEqual(typeof(int[]), resolver.ResolveType("System.Int32[]"));
            Assert.AreEqual(typeof(int[][]), resolver.ResolveType("System.Int32[][]"));
            Assert.AreEqual(typeof(int[,,][,]), resolver.ResolveType("System.Int32[,][,,]"));

            Assert.AreEqual(typeof(int).MakeArrayType(1), resolver.ResolveType("System.Int32[*]"));

            Assert.AreEqual(typeof(TestGenericBinarizable<TypeResolverTest>[]),
                resolver.ResolveType("Apache.Ignite.Core.Tests.TestGenericBinarizable`1" +
                                     "[[Apache.Ignite.Core.Tests.Binary.TypeResolverTest]][]"));
        }

        /// <summary>
        /// Tests array type resolve.
        /// </summary>
        [Test]
        public void TestArraysSimpleName()
        {
            var resolver = new TypeResolver();
            var mapper = BinaryBasicNameMapper.SimpleNameInstance;

            Assert.AreEqual(typeof(int[]), resolver.ResolveType("Int32[]", nameMapper: mapper));
            Assert.AreEqual(typeof(int[][]), resolver.ResolveType("Int32[][]", nameMapper: mapper));
            Assert.AreEqual(typeof(int[,,][,]), resolver.ResolveType("Int32[,][,,]", nameMapper: mapper));

            Assert.AreEqual(typeof(int).MakeArrayType(1), resolver.ResolveType("Int32[*]", nameMapper: mapper));

            Assert.AreEqual(typeof(TestGenericBinarizable<TypeResolverTest>[]),
                resolver.ResolveType("TestGenericBinarizable`1[[TypeResolverTest]][]", nameMapper: mapper));
        }

#if NETCOREAPP
        /// <summary>
        /// Tests that types from dynamic assemblies can be resolved.
        /// </summary>
        [Test]
        public void TestDynamicallyGeneratedType()
        {
            var typeName = nameof(TestDynamicallyGeneratedType) + new Random().Next();

            var assemblyName = new AssemblyName("DynamicAssembly1");
            var assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);
            var moduleBuilder = assemblyBuilder.DefineDynamicModule("DynamicModule1");
            var typeAttributes = TypeAttributes.Public | TypeAttributes.Class;
            var typeBuilder = moduleBuilder.DefineType(typeName, typeAttributes);
            var generatedType = typeBuilder.CreateType();

            var resolver = new TypeResolver();
            var resolvedType = resolver.ResolveType(typeName);

            Assert.AreEqual(generatedType, resolvedType);
        }
#endif

#if !NETCOREAPP
        /// <summary>
        /// Tests loading a type from referenced assembly that is not yet loaded.
        /// </summary>
        [Test]
        public void TestReferencedAssemblyLoading()
        {
            const string dllName = "Apache.Ignite.Core.Tests.TestDll,";

            const string typeName = "Apache.Ignite.Core.Tests.TestDll.TestClass";

            // Check that the dll is not yet loaded
            Assert.IsFalse(AppDomain.CurrentDomain.GetAssemblies().Any(x => x.FullName.StartsWith(dllName)));

            // Check that the dll is referenced by current assembly
            Assert.IsTrue(Assembly.GetExecutingAssembly().GetReferencedAssemblies()
                .Any(x => x.FullName.StartsWith(dllName)));

            // Check resolver
            var type = new TypeResolver().ResolveType(typeName);

            Assert.IsNotNull(type);
            Assert.AreEqual(typeName, type.FullName);
            Assert.IsNotNull(Activator.CreateInstance(type));

            // At this moment the dll should be loaded
            Assert.IsTrue(AppDomain.CurrentDomain.GetAssemblies().Any(x => x.FullName.StartsWith(dllName)));
        }

        /// <summary>
        /// Unused method that forces C# compiler to include TestDll assembly reference.
        /// Without this, compiler will remove the reference as unused.
        /// However, since it is never called, TestDll does not get loaded.
        /// </summary>
        public void UnusedMethod()
        {
            Assert.IsNotNull(typeof(TestDll.TestClass));
        }
#endif
    }
}
