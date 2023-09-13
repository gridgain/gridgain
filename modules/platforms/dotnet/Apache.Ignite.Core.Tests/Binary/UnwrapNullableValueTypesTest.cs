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
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="BinaryConfiguration.UnwrapNullableValueTypes"/>.
    /// </summary>
    public class UnwrapNullableValueTypesTest : TestBase
    {
        protected override IgniteConfiguration GetConfig() =>
            new IgniteConfiguration(base.GetConfig())
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    UnwrapNullableValueTypes = true,
                    TypeConfigurations = new[]
                    {
                        new BinaryTypeConfiguration(typeof(Primitives2))
                        {
                            Serializer = new BinaryReflectiveSerializer
                            {
                                UnwrapNullableValueTypes = false
                            }
                        }
                    }
                }
            };

        [Test]
        public void TestPrimitiveFields([Values(true, false)] bool nullValues)
        {
            var cache = Ignite.GetOrCreateCache<int, Primitives>(TestUtils.TestName);

            var primitives = nullValues
                ? new Primitives()
                : new Primitives
                {
                    Byte = 1,
                    Bytes = new byte?[] { 2 },
                    Sbyte = 3,
                    Sbytes = new sbyte?[] { 4 },
                    Bool = true,
                    Bools = new bool?[] { false },
                    Char = 'a',
                    Chars = new char?[] { 'b' },
                    Short = 5,
                    Shorts = new short?[] { 6 },
                    Ushort = 7,
                    Ushorts = new ushort?[] { 8 },
                    Int = 9,
                    Ints = new int?[] { 10 },
                    Uint = 11,
                    Uints = new uint?[] { 12 },
                    Long = 13,
                    Longs = new long?[] { 14 },
                    Ulong = 15,
                    Ulongs = new ulong?[] { 16 },
                    Float = 17,
                    Floats = new float?[] { 18 },
                    Double = 19,
                    Doubles = new double?[] { 20 },
                    Decimal = 21,
                    Decimals = new decimal?[] { 22 },
                    Guid = Guid.NewGuid(),
                    Guids = new Guid?[] { Guid.NewGuid() },
                    DateTime = DateTime.Now,
                    DateTimes = new DateTime?[] { DateTime.Now }
                };

            cache[1] = primitives;

            var res = cache[1];
            var binaryType = Ignite.GetBinary().GetBinaryType(typeof(Primitives));

            AssertExtensions.ReflectionEqual(primitives, res);

            Assert.AreEqual(BinaryTypeNames.TypeNameByte, binaryType.GetFieldTypeName(nameof(Primitives.Byte)));
            Assert.AreEqual(BinaryTypeNames.TypeNameByte, binaryType.GetFieldTypeName(nameof(Primitives.Sbyte)));
            Assert.AreEqual(BinaryTypeNames.TypeNameBool, binaryType.GetFieldTypeName(nameof(Primitives.Bool)));
            Assert.AreEqual(BinaryTypeNames.TypeNameChar, binaryType.GetFieldTypeName(nameof(Primitives.Char)));
            Assert.AreEqual(BinaryTypeNames.TypeNameShort, binaryType.GetFieldTypeName(nameof(Primitives.Short)));
            Assert.AreEqual(BinaryTypeNames.TypeNameShort, binaryType.GetFieldTypeName(nameof(Primitives.Ushort)));
            Assert.AreEqual(BinaryTypeNames.TypeNameInt, binaryType.GetFieldTypeName(nameof(Primitives.Int)));
            Assert.AreEqual(BinaryTypeNames.TypeNameInt, binaryType.GetFieldTypeName(nameof(Primitives.Uint)));
            Assert.AreEqual(BinaryTypeNames.TypeNameLong, binaryType.GetFieldTypeName(nameof(Primitives.Long)));
            Assert.AreEqual(BinaryTypeNames.TypeNameLong, binaryType.GetFieldTypeName(nameof(Primitives.Ulong)));
            Assert.AreEqual(BinaryTypeNames.TypeNameFloat, binaryType.GetFieldTypeName(nameof(Primitives.Float)));
            Assert.AreEqual(BinaryTypeNames.TypeNameDouble, binaryType.GetFieldTypeName(nameof(Primitives.Double)));
            Assert.AreEqual(BinaryTypeNames.TypeNameDecimal, binaryType.GetFieldTypeName(nameof(Primitives.Decimal)));
            Assert.AreEqual(BinaryTypeNames.TypeNameGuid, binaryType.GetFieldTypeName(nameof(Primitives.Guid)));
            Assert.AreEqual(BinaryTypeNames.TypeNameTimestamp, binaryType.GetFieldTypeName(nameof(Primitives.DateTime)));
        }

        [Test]
        public void TestPrimitiveFieldsUnwrapDisabled([Values(true, false)] bool nullValues)
        {
            var cache = Ignite.GetOrCreateCache<int, Primitives2>(TestUtils.TestName);

            var primitives = nullValues
                ? new Primitives2()
                : new Primitives2
                {
                    Byte = 1,
                    Bytes = new byte?[] { 2 },
                    Sbyte = 3,
                    Sbytes = new sbyte?[] { 4 },
                    Bool = true,
                    Bools = new bool?[] { false },
                    Char = 'a',
                    Chars = new char?[] { 'b' },
                    Short = 5,
                    Shorts = new short?[] { 6 },
                    Ushort = 7,
                    Ushorts = new ushort?[] { 8 },
                    Int = 9,
                    Ints = new int?[] { 10 },
                    Uint = 11,
                    Uints = new uint?[] { 12 },
                    Long = 13,
                    Longs = new long?[] { 14 },
                    Ulong = 15,
                    Ulongs = new ulong?[] { 16 },
                    Float = 17,
                    Floats = new float?[] { 18 },
                    Double = 19,
                    Doubles = new double?[] { 20 },
                    Decimal = 21,
                    Decimals = new decimal?[] { 22 },
                    Guid = Guid.NewGuid(),
                    Guids = new Guid?[] { Guid.NewGuid() },
                    DateTime = DateTime.Now,
                    DateTimes = new DateTime?[] { DateTime.Now }
                };

            cache[1] = primitives;

            var res = cache[1];
            var binaryType = Ignite.GetBinary().GetBinaryType(typeof(Primitives2));

            AssertExtensions.ReflectionEqual(primitives, res);

            foreach (var field in binaryType.Fields)
            {
                if (field.Last() != 's' && field != nameof(Primitives2.Guid))
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
        public void TestJavaInterop()
        {
            Assert.Fail("TODO: Same model in Java - check roundtrip in both directions.");
        }

        private class Primitives
        {
            public byte? Byte { get; set; }
            public byte?[] Bytes { get; set; }
            public sbyte? Sbyte { get; set; }
            public sbyte?[] Sbytes { get; set; }
            public bool? Bool { get; set; }
            public bool?[] Bools { get; set; }
            public char? Char { get; set; }
            public char?[] Chars { get; set; }
            public short? Short { get; set; }
            public short?[] Shorts { get; set; }
            public ushort? Ushort { get; set; }
            public ushort?[] Ushorts { get; set; }
            public int? Int { get; set; }
            public int?[] Ints { get; set; }
            public uint? Uint { get; set; }
            public uint?[] Uints { get; set; }
            public long? Long { get; set; }
            public long?[] Longs { get; set; }
            public ulong? Ulong { get; set; }
            public ulong?[] Ulongs { get; set; }
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

        private class Primitives2 : Primitives
        { }
    }
}
