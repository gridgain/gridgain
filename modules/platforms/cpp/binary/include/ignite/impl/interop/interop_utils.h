/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

#include <stdint.h>

#ifdef _WIN32
#   define AI_LITTLE_ENDIAN
#else
#   ifdef __LITTLE_ENDIAN__
#       define AI_LITTLE_ENDIAN
#   else
#       if __BYTE_ORDER == __LITTLE_ENDIAN
#           define AI_LITTLE_ENDIAN
#       endif
#   endif
#endif

namespace ignite
{
    namespace impl
    {
        namespace interop
        {
            namespace utils
            {
                union ConvertInt32Float
                {
                    int32_t i;
                    float f;
                };

                union ConvertInt64Double
                {
                    int64_t i;
                    double d;
                };

                /**
                 * Values for interop primitives.
                 * @tparam T Type.
                 */
                template<typename T>
                struct PrimitiveMeta
                {
                    // Empty.
                };

                template<>
                struct PrimitiveMeta<int8_t>
                {
                    enum { SIZE = 1 };
                };

                template<>
                struct PrimitiveMeta<uint16_t>
                {
                    enum { SIZE = 2 };
                };

                template<>
                struct PrimitiveMeta<int16_t>
                {
                    enum { SIZE = 2 };
                };

                template<>
                struct PrimitiveMeta<int32_t>
                {
                    enum { SIZE = 4 };
                };

                template<>
                struct PrimitiveMeta<int64_t>
                {
                    enum { SIZE = 8 };
                };

                template<>
                struct PrimitiveMeta<bool>
                {
                    enum { SIZE = 1 };
                };

                template<>
                struct PrimitiveMeta<float>
                {
                    enum { SIZE = 4 };
                };

                template<>
                struct PrimitiveMeta<double>
                {
                    enum { SIZE = 8 };
                };

                /**
                 * Reads int8 from memory assuming it's in the little-endian (Ignite binary byte order).
                 * Resulting value is valid on any platform.
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @param mem Memory to read from.
                 * @return Value.
                 */
                inline int8_t RawReadInt8(const void* mem)
                {
                    const uint8_t* ptr = static_cast<const uint8_t*>(mem);

                    return static_cast<int8_t>(ptr[0]);
                }

                /**
                 * Writes int8 to memory in the little-endian (Ignite binary byte order).
                 * Written value is in little endian no matter on which platform operation was performed.
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @param mem Memory to write value to.
                 * @param value Value to write.
                 */
                inline void RawWriteInt8(void* mem, int8_t value)
                {
                    int8_t* ptr = static_cast<int8_t*>(mem);

                    ptr[0] = static_cast<int8_t>(value);
                }

                /**
                 * Reads uint16 from memory assuming it's in the little-endian (Ignite binary byte order).
                 * Resulting value is valid on any platform.
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @param mem Memory to read from.
                 * @return Value.
                 */
                inline uint16_t RawReadUInt16(const void* mem)
                {
                    const uint8_t* ptr = static_cast<const uint8_t*>(mem);

                    return static_cast<uint16_t>(ptr[0]) |
                           static_cast<uint16_t>(ptr[1]) << 8;
                }

                /**
                 * Writes uint16 to memory in the little-endian (Ignite binary byte order).
                 * Written value is in little endian no matter on which platform operation was performed.
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @param mem Memory to write value to.
                 * @param value Value to write.
                 */
                inline void RawWriteUInt16(void* mem, uint16_t value)
                {
                    uint8_t* ptr = static_cast<uint8_t*>(mem);

                    ptr[0] = static_cast<uint8_t>(value & 0xFF);
                    ptr[1] = static_cast<uint8_t>((value >> 8) & 0xFF);
                }

                /**
                 * Reads int16 from memory assuming it's in the little-endian (Ignite binary byte order).
                 * Resulting value is valid on any platform.
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @param mem Memory to read from.
                 * @return Value.
                 */
                inline int16_t RawReadInt16(const void* mem)
                {
                    return static_cast<int16_t>(RawReadUInt16(mem));
                }

                /**
                 * Writes int16 to memory in the little-endian (Ignite binary byte order).
                 * Written value is in little endian no matter on which platform operation was performed.
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @param mem Memory to write value to.
                 * @param value Value to write.
                 */
                inline void RawWriteInt16(void* mem, int16_t value)
                {
                    RawWriteUInt16(mem, static_cast<uint16_t>(value));
                }

                /**
                 * Reads int32 from memory assuming it's in the little-endian (Ignite binary byte order).
                 * Resulting value is valid on any platform.
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @param mem Memory to read from.
                 * @return Value.
                 */
                inline int32_t RawReadInt32(const void* mem)
                {
                    const uint8_t* ptr = static_cast<const uint8_t*>(mem);

                    return static_cast<int32_t>(
                            static_cast<uint32_t>(ptr[0])       |
                            static_cast<uint32_t>(ptr[1]) << 8  |
                            static_cast<uint32_t>(ptr[2]) << 16 |
                            static_cast<uint32_t>(ptr[3]) << 24
                    );
                }

                /**
                 * Writes int32 to memory in the little-endian (Ignite binary byte order).
                 * Written value is in little endian no matter on which platform operation was performed.
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @param mem Memory to write value to.
                 * @param value Value to write.
                 */
                inline void RawWriteInt32(void* mem, int32_t value)
                {
                    uint8_t* ptr = static_cast<uint8_t*>(mem);
                    uint32_t uValue = static_cast<uint16_t>(value);

                    ptr[0] = static_cast<uint8_t>(uValue & 0xFF);
                    ptr[1] = static_cast<uint8_t>((uValue >> 8) & 0xFF);
                    ptr[2] = static_cast<uint8_t>((uValue >> 16) & 0xFF);
                    ptr[3] = static_cast<uint8_t>((uValue >> 24) & 0xFF);
                }

                /**
                 * Reads int64 from memory assuming it's in the little-endian (Ignite binary byte order).
                 * Resulting value is valid on any platform.
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @param mem Memory to read from.
                 * @return Value.
                 */
                inline int64_t RawReadInt64(const void* mem)
                {
                    const uint8_t* ptr = static_cast<const uint8_t*>(mem);

                    return static_cast<int64_t>(
                            static_cast<uint64_t>(ptr[0])       |
                            static_cast<uint64_t>(ptr[1]) << 8  |
                            static_cast<uint64_t>(ptr[2]) << 16 |
                            static_cast<uint64_t>(ptr[3]) << 24 |
                            static_cast<uint64_t>(ptr[4]) << 32 |
                            static_cast<uint64_t>(ptr[5]) << 40 |
                            static_cast<uint64_t>(ptr[6]) << 48 |
                            static_cast<uint64_t>(ptr[7]) << 56
                    );
                }

                /**
                 * Writes int64 to memory in the little-endian (Ignite binary byte order).
                 * Written value is in little endian no matter on which platform operation was performed.
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @param mem Memory to write value to.
                 * @param value Value to write.
                 */
                inline void RawWriteInt64(void* mem, int64_t value)
                {
                    uint8_t* ptr = static_cast<uint8_t*>(mem);
                    uint64_t uValue = static_cast<uint16_t>(value);

                    ptr[0] = static_cast<uint8_t>(uValue & 0xFF);
                    ptr[1] = static_cast<uint8_t>((uValue >> 8) & 0xFF);
                    ptr[2] = static_cast<uint8_t>((uValue >> 16) & 0xFF);
                    ptr[3] = static_cast<uint8_t>((uValue >> 24) & 0xFF);
                    ptr[4] = static_cast<uint8_t>((uValue >> 32) & 0xFF);
                    ptr[5] = static_cast<uint8_t>((uValue >> 40) & 0xFF);
                    ptr[6] = static_cast<uint8_t>((uValue >> 48) & 0xFF);
                    ptr[7] = static_cast<uint8_t>((uValue >> 56) & 0xFF);
                }

                /**
                 * Reads bool from memory assuming it's in the little-endian (Ignite binary byte order).
                 * Resulting value is valid on any platform.
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @param mem Memory to read from.
                 * @return Value.
                 */
                inline bool RawReadBool(const void* mem)
                {
                    return RawReadInt8(mem) != 0;
                }

                /**
                 * Writes bool to memory in the little-endian (Ignite binary byte order).
                 * Written value is in little endian no matter on which platform operation was performed.
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @param mem Memory to write value to.
                 * @param value Value to write.
                 */
                inline void RawWriteBool(void* mem, bool value)
                {
                    RawWriteInt8(mem, value ? 1 : 0);
                }

                /**
                 * Reads float from memory assuming it's in the little-endian (Ignite binary byte order).
                 * Resulting value is valid on any platform.
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @param mem Memory to read from.
                 * @return Value.
                 */
                inline float RawReadFloat(const void* mem)
                {
                    ConvertInt32Float u = {};
                    u.i = RawReadInt32(mem);

                    return u.f;
                }

                /**
                 * Writes float to memory in the little-endian (Ignite binary byte order).
                 * Written value is in little endian no matter on which platform operation was performed.
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @param mem Memory to write value to.
                 * @param value Value to write.
                 */
                inline void RawWriteFloat(void* mem, float value)
                {
                    ConvertInt32Float u = {};
                    u.f = value;

                    RawWriteInt32(mem, u.i);
                }

                /**
                 * Reads float from memory assuming it's in the little-endian (Ignite binary byte order).
                 * Resulting value is valid on any platform.
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @param mem Memory to read from.
                 * @return Value.
                 */
                inline double RawReadDouble(const void* mem)
                {
                    ConvertInt64Double u = {};
                    u.i = RawReadInt64(mem);

                    return u.d;
                }

                /**
                 * Writes double to memory in the little-endian (Ignite binary byte order).
                 * Written value is in little endian no matter on which platform operation was performed.
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @param mem Memory to write value to.
                 * @param value Value to write.
                 */
                inline void RawWriteDouble(void* mem, double value)
                {
                    ConvertInt64Double u = {};
                    u.d = value;

                    RawWriteInt64(mem, u.i);
                }

                /**
                 * Templated function to read primitive from assuming it's in the little-endian (Ignite binary byte
                 * order).
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @tparam T Primitive type.
                 * @param mem Memory to read from.
                 * @return Value.
                 */
                template<typename T>
                T RawReadPrimitive(const void* mem);

                template<>
                inline int8_t RawReadPrimitive(const void* mem)
                {
                    return RawReadInt8(mem);
                }

                template<>
                inline int16_t RawReadPrimitive(const void* mem)
                {
                    return RawReadInt16(mem);
                }

                template<>
                inline uint16_t RawReadPrimitive(const void* mem)
                {
                    return RawReadUInt16(mem);
                }

                template<>
                inline int32_t RawReadPrimitive(const void* mem)
                {
                    return RawReadInt32(mem);
                }

                template<>
                inline int64_t RawReadPrimitive(const void* mem)
                {
                    return RawReadInt64(mem);
                }

                template<>
                inline bool RawReadPrimitive(const void* mem)
                {
                    return RawReadBool(mem);
                }

                template<>
                inline float RawReadPrimitive(const void* mem)
                {
                    return RawReadFloat(mem);
                }

                template<>
                inline double RawReadPrimitive(const void* mem)
                {
                    return RawReadDouble(mem);
                }

                /**
                 * Writes int64 to memory in the little-endian (Ignite binary byte order).
                 * Written value is in little endian no matter on which platform operation was performed.
                 *
                 * @warning No boundary checks for the memory are conducted.
                 * @param mem Memory to write value to.
                 * @param value Value to write.
                 */
                template<typename T>
                void RawWritePrimitive(void* mem, T value);

                template<>
                inline void RawWritePrimitive(void* mem, int8_t value)
                {
                    RawWriteInt8(mem, value);
                }

                template<>
                inline void RawWritePrimitive(void* mem, int16_t value)
                {
                    RawWriteInt16(mem, value);
                }

                template<>
                inline void RawWritePrimitive(void* mem, uint16_t value)
                {
                    RawWriteUInt16(mem, value);
                }

                template<>
                inline void RawWritePrimitive(void* mem, int32_t value)
                {
                    RawWriteInt32(mem, value);
                }

                template<>
                inline void RawWritePrimitive(void* mem, int64_t value)
                {
                    RawWriteInt64(mem, value);
                }

                template<>
                inline void RawWritePrimitive(void* mem, bool value)
                {
                    RawWriteBool(mem, value);
                }

                template<>
                inline void RawWritePrimitive(void* mem, float value)
                {
                    RawWriteFloat(mem, value);
                }

                template<>
                inline void RawWritePrimitive(void* mem, double value)
                {
                    RawWriteDouble(mem, value);
                }
            } // namespace utils
        } // namespace interop
    } // namespace impl
} // namespace ignite