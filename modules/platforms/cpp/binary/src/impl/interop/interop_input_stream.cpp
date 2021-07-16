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

#include <cstring>

#include <ignite/ignite_error.h>

#include <ignite/impl/interop/interop_utils.h>
#include "ignite/impl/interop/interop_input_stream.h"


namespace ignite
{
    namespace impl
    {
        namespace interop 
        {
            InteropInputStream::InteropInputStream(InteropMemory* mem) :
                mem(mem),
                data(mem->Data()),
                len(mem->Length()),
                pos(0)
            {
                // No-op.
            }

            int8_t InteropInputStream::ReadInt8()
            {
                return ReadAndShift<int8_t>();
            }

            int32_t InteropInputStream::ReadInt8(int32_t pos)
            {
                return Read<int8_t>(pos);
            }

            void InteropInputStream::ReadInt8Array(int8_t* res, const int32_t len)
            {
                ReadArrayAndShift<int8_t>(res, len);
            }

            bool InteropInputStream::ReadBool()
            {
                return ReadInt8() == 1;
            }

            void InteropInputStream::ReadBoolArray(bool* res, const int32_t len)
            {
                EnsureEnoughData(pos + len);

                for (int32_t i = 0; i < len; ++i)
                    res[i] = utils::RawReadPrimitive<bool>(data + pos + i);

                Shift(len);
            }

            int16_t InteropInputStream::ReadInt16()
            {
                return ReadAndShift<int16_t>();
            }

            int32_t InteropInputStream::ReadInt16(int32_t pos)
            {
                return Read<int16_t>(pos);
            }

            void InteropInputStream::ReadInt16Array(int16_t* const res, const int32_t len)
            {
                ReadArrayAndShift<int16_t>(res, len);
            }

            uint16_t InteropInputStream::ReadUInt16()
            {
                return ReadAndShift<uint16_t>();
            }

            void InteropInputStream::ReadUInt16Array(uint16_t* const res, const int32_t len)
            {
                ReadArrayAndShift<uint16_t>(res, len);
            }

            int32_t InteropInputStream::ReadInt32()
            {
                return ReadAndShift<int32_t>();
            }

            int32_t InteropInputStream::ReadInt32(int32_t pos)
            {
                return Read<int32_t>(pos);
            }

            void InteropInputStream::ReadInt32Array(int32_t* const res, const int32_t len)
            {
                ReadArrayAndShift<int32_t>(res, len);
            }

            int64_t InteropInputStream::ReadInt64()
            {
                return ReadAndShift<int64_t>();
            }

            void InteropInputStream::ReadInt64Array(int64_t* const res, const int32_t len)
            {
                ReadArrayAndShift<int64_t>(res, len);
            }

            float InteropInputStream::ReadFloat()
            {
                return ReadAndShift<float>();
            }

            void InteropInputStream::ReadFloatArray(float* const res, const int32_t len)
            {
                ReadArrayAndShift<float>(res, len);
            }

            double InteropInputStream::ReadDouble()
            {
                return ReadAndShift<double>();
            }

            void InteropInputStream::ReadDoubleArray(double* const res, const int32_t len)
            {
                ReadArrayAndShift<double>(res, len);
            }

            int32_t InteropInputStream::Remaining() const
            {
                return len - pos;
            }

            int32_t InteropInputStream::Position() const
            {
                return pos;
            }

            void InteropInputStream::Position(int32_t pos)
            {
                if (pos <= len)
                    this->pos = pos;
                else {
                    IGNITE_ERROR_FORMATTED_3(IgniteError::IGNITE_ERR_MEMORY, "Requested input stream position is out of bounds",
                        "memPtr", mem->PointerLong(), "len", len, "pos", pos);
                }
            }

            void InteropInputStream::Ignore(int32_t cnt)
            {
                Shift(cnt);
            }

            void InteropInputStream::Synchronize()
            {
                data = mem->Data();
                len = mem->Length();
            }

            void InteropInputStream::EnsureEnoughData(int32_t cnt) const
            {
                if (len >= cnt)
                    return;
                else {
                    IGNITE_ERROR_FORMATTED_4(IgniteError::IGNITE_ERR_MEMORY, "Not enough data in the stream",
                        "memPtr", mem->PointerLong(), "len", len, "pos", pos, "requested", cnt);
                }
            }

            template<typename T>
            inline void InteropInputStream::ReadArrayAndShift(T* dest, int32_t cnt)
            {
                const int32_t bytesToRead = cnt * utils::PrimitiveMeta<T>::SIZE;

                EnsureEnoughData(pos + bytesToRead);
#ifdef AI_LITTLE_ENDIAN
                // Optimization for little endian - we can simply copy data.
                memcpy(dest, data + pos, bytesToRead);
#else
                for (int32_t i = 0; i < cnt; ++i)
                    dest[i] = utils::RawReadPrimitive<T>(data + pos + (i * utils::PrimitiveMeta<T>::SIZE));
#endif
                Shift(bytesToRead);
            }

            inline void InteropInputStream::Shift(int32_t cnt)
            {
                pos += cnt;
            }

            template<typename T>
            inline T InteropInputStream::Read(int32_t pos0)
            {
                EnsureEnoughData(pos0 + utils::PrimitiveMeta<T>::SIZE);

                T res = utils::RawReadPrimitive<T>(data + pos0);

                return res;
            }

            template<typename T>
            inline T InteropInputStream::ReadAndShift()
            {
                T res = Read<T>(pos);

                Shift(utils::PrimitiveMeta<T>::SIZE);

                return res;
            }
        }
    }
}
