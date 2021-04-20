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

#include "ignite/impl/interop/interop_utils.h"
#include "ignite/impl/interop/interop_output_stream.h"

namespace ignite
{
    namespace impl
    {
        namespace interop 
        {
            InteropOutputStream::InteropOutputStream(InteropMemory* mem) :
                mem(mem),
                data(mem->Data()),
                cap(mem->Capacity()),
                pos(0)
            {
                // No-op.
            }

            void InteropOutputStream::WriteInt8(const int8_t val)
            {
                WriteAndShift<int8_t>(val);
            }

            void InteropOutputStream::WriteInt8(const int8_t val, const int32_t pos)
            {
                Write<int8_t>(pos, val);
            }

            void InteropOutputStream::WriteInt8Array(const int8_t* val, const int32_t len)
            {
                WriteArrayAndShift<int8_t>(val, len);
            }

            void InteropOutputStream::WriteBool(const bool val)
            {
                WriteAndShift<bool>(val);
            }

            void InteropOutputStream::WriteBoolArray(const bool* val, const int32_t len)
            {
                for (int i = 0; i < len; i++)
                    WriteBool(*(val + i));
            }

            void InteropOutputStream::WriteInt16(const int16_t val)
            {
                WriteAndShift<int16_t>(val);
            }

            void InteropOutputStream::WriteInt16(const int32_t pos, const int16_t val)
            {
                Write<int16_t>(pos, val);
            }

            void InteropOutputStream::WriteInt16Array(const int16_t* val, const int32_t len)
            {
                WriteArrayAndShift<int16_t>(val, len);
            }

            void InteropOutputStream::WriteUInt16(const uint16_t val)
            {
                WriteAndShift<uint16_t>(val);
            }

            void InteropOutputStream::WriteUInt16Array(const uint16_t* val, const int32_t len)
            {
                WriteArrayAndShift<uint16_t>(val, len);
            }

            void InteropOutputStream::WriteInt32(const int32_t val)
            {
                WriteAndShift<int32_t>(val);
            }

            void InteropOutputStream::WriteInt32(const int32_t pos, const int32_t val)
            {
                Write<int32_t>(pos, val);
            }

            void InteropOutputStream::WriteInt32Array(const int32_t* val, const int32_t len)
            {
                WriteArrayAndShift<int32_t>(val, len);
            }

            void InteropOutputStream::WriteInt64(const int64_t val)
            {
                WriteAndShift<int64_t>(val);
            }

            void InteropOutputStream::WriteInt64(const int32_t pos, const int64_t val)
            {
                Write<int64_t>(pos, val);
            }

            void InteropOutputStream::WriteInt64Array(const int64_t* val, const int32_t len)
            {
                WriteArrayAndShift<int64_t>(val, len);
            }

            void InteropOutputStream::WriteFloat(const float val)
            {
                WriteAndShift<float>(val);
            }

            void InteropOutputStream::WriteFloatArray(const float* val, const int32_t len)
            {
                WriteArrayAndShift<float>(val, len);
            }

            void InteropOutputStream::WriteDouble(const double val)
            {
                WriteAndShift<double>(val);
            }

            void InteropOutputStream::WriteDoubleArray(const double* val, const int32_t len)
            {
                WriteArrayAndShift<double>(val, len);
            }

            int32_t InteropOutputStream::Position() const
            {
                return pos;
            }

            void InteropOutputStream::Position(const int32_t val)
            {
                EnsureCapacity(val);

                pos = val;
            }

            int32_t InteropOutputStream::Reserve(int32_t num)
            {
                EnsureCapacity(pos + num);

                int32_t res = pos;

                Shift(num);

                return res;
            }

            void InteropOutputStream::Synchronize()
            {
                mem->Length(pos);
            }

            InteropMemory* InteropOutputStream::GetMemory()
            {
                return mem;
            }

            void InteropOutputStream::EnsureCapacity(int32_t reqCap)
            {
                if (reqCap > cap)
                {
                    int newCap = cap << 1;

                    if (newCap < reqCap)
                        newCap = reqCap;

                    mem->Reallocate(newCap);
                    data = mem->Data();
                    cap = newCap;
                }
            }

            inline void InteropOutputStream::Shift(int32_t cnt)
            {
                pos += cnt;
            }

            template<typename T>
            inline void InteropOutputStream::WriteArrayAndShift(const T* src, int32_t len)
            {
                const int32_t bytesToWrite = len * utils::PrimitiveMeta<T>::SIZE;

                EnsureCapacity(pos + bytesToWrite);
#ifdef AI_LITTLE_ENDIAN
                // Optimization for little endian - we can simply copy data.
                memcpy(data + pos, src, bytesToWrite);
#else
                for (int32_t i = 0; i < len; ++i)
                    utils::RawWritePrimitive<T>(data + pos + (i * utils::PrimitiveMeta<T>::SIZE), src[i]);
#endif
                Shift(bytesToWrite);
            }

            template<typename T>
            inline void InteropOutputStream::Write(int32_t pos0, T val)
            {
                EnsureCapacity(pos0 + utils::PrimitiveMeta<T>::SIZE);

                utils::RawWritePrimitive<T>(data + pos0, val);
            }

            template<typename T>
            inline void InteropOutputStream::WriteAndShift(T val)
            {
                Write<T>(pos, val);

                Shift(utils::PrimitiveMeta<T>::SIZE);
            }
        }
    }
}

