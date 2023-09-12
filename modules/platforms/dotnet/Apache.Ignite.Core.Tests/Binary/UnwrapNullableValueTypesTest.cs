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
    using Apache.Ignite.Core.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="BinaryConfiguration.UnwrapNullableValueTypes"/>.
    /// </summary>
    public class UnwrapNullableValueTypesTest
    {
        [Test]
        public void TestPrimitiveKeyVal()
        {

        }

        [Test]
        public void TestPrimitiveFields()
        {

        }

        [Test]
        public void TestArrayFields()
        {

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
    }
}
