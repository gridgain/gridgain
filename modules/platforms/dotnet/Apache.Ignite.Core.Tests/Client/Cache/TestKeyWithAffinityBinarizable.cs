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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using Apache.Ignite.Core.Binary;

    public sealed class TestKeyWithAffinityBinarizable : IBinarizable
    {
        private int _i;

        private string _s;

        private readonly bool _skipKey;

        public TestKeyWithAffinityBinarizable(int i, string s, bool skipKey = false)
        {
            _i = i;
            _s = s;
            _skipKey = skipKey;
        }

        private bool Equals(TestKeyWithAffinityBinarizable other)
        {
            return _i == other._i && string.Equals(_s, other._s);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((TestKeyWithAffinityBinarizable) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                // ReSharper disable NonReadonlyMemberInGetHashCode (effectively readonly).
                return (_i * 397) ^ (_s != null ? _s.GetHashCode() : 0);
            }
        }

        public void WriteBinary(IBinaryWriter writer)
        {
            writer.WriteString("str", _s);

            if (!_skipKey)
            {
                writer.WriteInt("id", _i);
            }
        }

        public void ReadBinary(IBinaryReader reader)
        {
            _s = reader.ReadString("str");
            _i = reader.ReadInt("id");
        }
    }
}
