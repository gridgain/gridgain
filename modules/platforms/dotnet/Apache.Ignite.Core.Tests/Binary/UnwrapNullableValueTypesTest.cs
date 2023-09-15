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
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="BinaryConfiguration.UnwrapNullableValueTypes"/>.
    /// </summary>
    public class UnwrapNullableValueTypesTest : TestBase
    {
        private const string PlatformNullablePrimitivesTask =
            "org.apache.ignite.platform.PlatformNullablePrimitivesTask";

        protected override IgniteConfiguration GetConfig() =>
            new IgniteConfiguration(base.GetConfig())
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    UnwrapNullableValueTypes = true,
                    ForceTimestamp = true,
                    TypeConfigurations = new[]
                    {
                        new BinaryTypeConfiguration(typeof(NullableValueTypes2))
                        {
                            Serializer = new BinaryReflectiveSerializer
                            {
                                UnwrapNullableValueTypes = false
                            }
                        }
                    },
                    NameMapper = new BinaryBasicNameMapper { IsSimpleName = true }
                }
            };

        [Test]
        public void TestPrimitiveFields([Values(true, false)] bool nullValues)
        {
            var cache = Ignite.GetOrCreateCache<int, NullableValueTypes>(TestUtils.TestName);

            var primitives = nullValues
                ? new NullableValueTypes()
                : new NullableValueTypes
                {
                    Byte = 1,
                    Bytes = new byte?[] { 2 },
                    SByte = 3,
                    SBytes = new sbyte?[] { 4 },
                    Bool = true,
                    Bools = new bool?[] { false },
                    Char = 'a',
                    Chars = new char?[] { 'b' },
                    Short = 5,
                    Shorts = new short?[] { 6 },
                    UShort = 7,
                    UShorts = new ushort?[] { 8 },
                    Int = 9,
                    Ints = new int?[] { 10 },
                    UInt = 11,
                    UInts = new uint?[] { 12 },
                    Long = 13,
                    Longs = new long?[] { 14 },
                    ULong = 15,
                    ULongs = new ulong?[] { 16 },
                    Float = 17,
                    Floats = new float?[] { 18 },
                    Double = 19,
                    Doubles = new double?[] { 20 },
                    Decimal = 21,
                    Decimals = new decimal?[] { 22 },
                    Guid = Guid.NewGuid(),
                    Guids = new Guid?[] { Guid.NewGuid() },
                    DateTime = DateTime.UtcNow,
                    DateTimes = new DateTime?[] { DateTime.UtcNow }
                };

            cache[1] = primitives;

            var res = cache[1];
            var binaryType = Ignite.GetBinary().GetBinaryType(typeof(NullableValueTypes));

            AssertExtensions.ReflectionEqual(primitives, res);

            Assert.AreEqual(BinaryTypeNames.TypeNameByte, binaryType.GetFieldTypeName(nameof(NullableValueTypes.Byte)));
            Assert.AreEqual(BinaryTypeNames.TypeNameByte, binaryType.GetFieldTypeName(nameof(NullableValueTypes.SByte)));
            Assert.AreEqual(BinaryTypeNames.TypeNameBool, binaryType.GetFieldTypeName(nameof(NullableValueTypes.Bool)));
            Assert.AreEqual(BinaryTypeNames.TypeNameChar, binaryType.GetFieldTypeName(nameof(NullableValueTypes.Char)));
            Assert.AreEqual(BinaryTypeNames.TypeNameShort, binaryType.GetFieldTypeName(nameof(NullableValueTypes.Short)));
            Assert.AreEqual(BinaryTypeNames.TypeNameShort, binaryType.GetFieldTypeName(nameof(NullableValueTypes.UShort)));
            Assert.AreEqual(BinaryTypeNames.TypeNameInt, binaryType.GetFieldTypeName(nameof(NullableValueTypes.Int)));
            Assert.AreEqual(BinaryTypeNames.TypeNameInt, binaryType.GetFieldTypeName(nameof(NullableValueTypes.UInt)));
            Assert.AreEqual(BinaryTypeNames.TypeNameLong, binaryType.GetFieldTypeName(nameof(NullableValueTypes.Long)));
            Assert.AreEqual(BinaryTypeNames.TypeNameLong, binaryType.GetFieldTypeName(nameof(NullableValueTypes.ULong)));
            Assert.AreEqual(BinaryTypeNames.TypeNameFloat, binaryType.GetFieldTypeName(nameof(NullableValueTypes.Float)));
            Assert.AreEqual(BinaryTypeNames.TypeNameDouble, binaryType.GetFieldTypeName(nameof(NullableValueTypes.Double)));
            Assert.AreEqual(BinaryTypeNames.TypeNameDecimal, binaryType.GetFieldTypeName(nameof(NullableValueTypes.Decimal)));
            Assert.AreEqual(BinaryTypeNames.TypeNameGuid, binaryType.GetFieldTypeName(nameof(NullableValueTypes.Guid)));
            Assert.AreEqual(BinaryTypeNames.TypeNameTimestamp, binaryType.GetFieldTypeName(nameof(NullableValueTypes.DateTime)));
        }

        [Test]
        public void TestPrimitiveFieldsUnwrapDisabled([Values(true, false)] bool nullValues)
        {
            // Separate class to avoid meta conflict.
            var cache = Ignite.GetOrCreateCache<int, NullableValueTypes2>(TestUtils.TestName);

            var primitives = nullValues
                ? new NullableValueTypes2()
                : new NullableValueTypes2
                {
                    Byte = 1,
                    Bytes = new byte?[] { 2 },
                    SByte = 3,
                    SBytes = new sbyte?[] { 4 },
                    Bool = true,
                    Bools = new bool?[] { false },
                    Char = 'a',
                    Chars = new char?[] { 'b' },
                    Short = 5,
                    Shorts = new short?[] { 6 },
                    UShort = 7,
                    UShorts = new ushort?[] { 8 },
                    Int = 9,
                    Ints = new int?[] { 10 },
                    UInt = 11,
                    UInts = new uint?[] { 12 },
                    Long = 13,
                    Longs = new long?[] { 14 },
                    ULong = 15,
                    ULongs = new ulong?[] { 16 },
                    Float = 17,
                    Floats = new float?[] { 18 },
                    Double = 19,
                    Doubles = new double?[] { 20 },
                    Decimal = 21,
                    Decimals = new decimal?[] { 22 },
                    Guid = Guid.NewGuid(),
                    Guids = new Guid?[] { Guid.NewGuid() },
                    DateTime = DateTime.UtcNow,
                    DateTimes = new DateTime?[] { DateTime.UtcNow }
                };

            cache[1] = primitives;

            var res = cache[1];
            var binaryType = Ignite.GetBinary().GetBinaryType(typeof(NullableValueTypes2));

            AssertExtensions.ReflectionEqual(primitives, res);

            foreach (var field in binaryType.Fields)
            {
                if (field.Last() != 's' && field != nameof(NullableValueTypes2.Guid))
                {
                    Assert.AreEqual(BinaryTypeNames.TypeNameObject, binaryType.GetFieldTypeName(field), field);
                }
            }
        }

        [Test]
        public void TestArrayFields()
        {
            Assert.Fail("TODO");
        }

        [Test]
        public void TestJavaWriteDotNetRead([Values(true, false)] bool nullValues)
        {
            var cache = Ignite.GetOrCreateCache<int, JavaNullableValueTypes>(TestUtils.TestName);
            ExecuteJavaTask(cache.Name, JavaTaskCommand.Put, nullValues);
            cache[2] = new JavaNullableValueTypes2();

            // Get binary type from Java.
            var javaBinaryType = Ignite.GetBinary().GetBinaryType(nameof(JavaNullableValueTypes));

            // Initialize .NET binary type.
            var dotNetBinaryType = Ignite.GetBinary().GetBinaryType(typeof(JavaNullableValueTypes2));

            // Compare .NET and Java behavior for two different types with the same field set.
            Assert.AreNotEqual(javaBinaryType.TypeId, dotNetBinaryType.TypeId);
            CollectionAssert.AreEquivalent(javaBinaryType.Fields, dotNetBinaryType.Fields);

            foreach (var field in javaBinaryType.Fields)
            {
                Assert.AreEqual(javaBinaryType.GetFieldTypeName(field), dotNetBinaryType.GetFieldTypeName(field), field);
            }

            var res = cache[1];
            Assert.AreEqual(1, res.Byte);
            Assert.AreEqual(1, res.Bytes[0]);
            Assert.AreEqual(1, res.SByte);
            Assert.AreEqual(1, res.SBytes[0]);
            Assert.AreEqual(true, res.Bool);
            Assert.AreEqual(true, res.Bools[0]);
            Assert.AreEqual('a', res.Char);
            Assert.AreEqual('a', res.Chars[0]);
            Assert.AreEqual(1, res.Short);
            Assert.AreEqual(1, res.Shorts[0]);
            Assert.AreEqual(1, res.UShort);
            Assert.AreEqual(1, res.UShorts[0]);
            Assert.AreEqual(1, res.Int);
            Assert.AreEqual(1, res.Ints[0]);
            Assert.AreEqual(1, res.UInt);
            Assert.AreEqual(1, res.UInts[0]);
            Assert.AreEqual(1, res.Long);
            Assert.AreEqual(1, res.Longs[0]);
            Assert.AreEqual(1, res.ULong);
            Assert.AreEqual(1, res.ULongs[0]);
            Assert.AreEqual(1, res.Float);
            Assert.AreEqual(1, res.Floats[0]);
            Assert.AreEqual(1, res.Double);
            Assert.AreEqual(1, res.Doubles[0]);
            Assert.AreEqual(1, res.Decimal);
            Assert.AreEqual(1, res.Decimals[0]);
            Assert.AreEqual(new Guid(0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2), res.Guid);
            Assert.AreEqual(new Guid(0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2), res.Guids[0]);
            Assert.AreEqual(DateTime.FromBinary(5233041986428617904), res.DateTime);
            Assert.AreEqual(DateTime.FromBinary(5233041986428617904), res.DateTimes[0]);
        }

        [Test]
        public void TestDotNetWriteJavaRead()
        {
            var cache = Ignite.GetOrCreateCache<int, JavaNullableValueTypes>(TestUtils.TestName);
            cache[1] = new JavaNullableValueTypes
            {
                Byte = 1,
                Bytes = new byte?[] { 2 },
                SByte = 3,
                SBytes = new sbyte?[] { 4 },
                Bool = true,
                Bools = new bool?[] { false },
                Char = 'a',
                Chars = new char?[] { 'b' },
                Short = 5,
                Shorts = new short?[] { 6 },
                UShort = 7,
                UShorts = new ushort?[] { 8 },
                Int = 9,
                Ints = new int?[] { 10 },
                UInt = 11,
                UInts = new uint?[] { 12 },
                Long = 13,
                Longs = new long?[] { 14 },
                ULong = 15,
                ULongs = new ulong?[] { 16 },
                Float = 17,
                Floats = new float?[] { 18 },
                Double = 19,
                Doubles = new double?[] { 20 },
                Decimal = 21,
                Decimals = new decimal?[] { 22 },
                Guid = Guid.NewGuid(),
                Guids = new Guid?[] { Guid.NewGuid() },
                DateTime = DateTime.UtcNow,
                DateTimes = new DateTime?[] { DateTime.UtcNow }
            };

            ExecuteJavaTask(cache.Name, JavaTaskCommand.Get);
        }

        private void ExecuteJavaTask(string cacheName, JavaTaskCommand command, bool nullValues = false)
        {
            Ignite.GetCompute().ExecuteJavaTask<object>(
                PlatformNullablePrimitivesTask, $"{command}|{cacheName}|{nullValues}");
        }

        private enum JavaTaskCommand
        {
            Put,
            Get
        }

        private class NullableValueTypes
        {
            public byte? Byte { get; set; }
            public byte?[] Bytes { get; set; }
            public sbyte? SByte { get; set; }
            public sbyte?[] SBytes { get; set; }
            public bool? Bool { get; set; }
            public bool?[] Bools { get; set; }
            public char? Char { get; set; }
            public char?[] Chars { get; set; }
            public short? Short { get; set; }
            public short?[] Shorts { get; set; }
            public ushort? UShort { get; set; }
            public ushort?[] UShorts { get; set; }
            public int? Int { get; set; }
            public int?[] Ints { get; set; }
            public uint? UInt { get; set; }
            public uint?[] UInts { get; set; }
            public long? Long { get; set; }
            public long?[] Longs { get; set; }
            public ulong? ULong { get; set; }
            public ulong?[] ULongs { get; set; }
            public float? Float { get; set; }
            public float?[] Floats { get; set; }
            public double? Double { get; set; }
            public double?[] Doubles { get; set; }
            public decimal? Decimal { get; set; }
            public decimal?[] Decimals { get; set; }
            public Guid? Guid { get; set; }
            public Guid?[] Guids { get; set; }
            public DateTime? DateTime { get; set; }
            public DateTime?[] DateTimes { get; set; }
        }

        private class NullableValueTypes2 : NullableValueTypes
        { }

        private class JavaNullableValueTypes : NullableValueTypes
        { }

        private class JavaNullableValueTypes2 : JavaNullableValueTypes
        { }
    }
}
