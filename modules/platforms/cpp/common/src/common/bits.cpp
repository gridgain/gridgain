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

#include <algorithm>
#include <cassert>

#include "ignite/common/bits.h"

namespace ignite
{
    namespace common
    {
        namespace bits
        {
            int32_t NumberOfTrailingZerosU32(uint32_t i) {
                int32_t c = 32;
                if (!i)
                    return c;

#if defined(__GNUC__) || defined(__clang__)
                return __builtin_ctz(i);
#else
                // See https://graphics.stanford.edu/~seander/bithacks.html#ZerosOnRightParallel
                // for details.
                i &= ~i + 1u;

                if (i)
                    c--;

                if (i & 0x0000FFFF)
                    c -= 16;

                if (i & 0x00FF00FF)
                    c -= 8;

                if (i & 0x0F0F0F0F)
                    c -= 4;

                if (i & 0x33333333)
                    c -= 2;

                if (i & 0x55555555)
                    c -= 1;

                return c;
#endif
            }

            int32_t NumberOfTrailingZerosI32(int32_t i)
            {
                return NumberOfTrailingZerosU32(static_cast<uint32_t>(i));
            }

            int32_t NumberOfLeadingZerosI32(int32_t i)
            {
                return NumberOfLeadingZerosU32(static_cast<uint32_t>(i));
            }

            int32_t NumberOfLeadingZerosU32(uint32_t i)
            {
                if (!i)
                    return 32;

#if defined(__GNUC__) || defined(__clang__)
                return __builtin_clz(i);
#else
                int32_t n = 1;

                if (i >> 16 == 0) {
                    n += 16;
                    i <<= 16;
                }

                if (i >> 24 == 0) {
                    n += 8;
                    i <<= 8;
                }

                if (i >> 28 == 0) {
                    n += 4;
                    i <<= 4;
                }

                if (i >> 30 == 0) {
                    n += 2;
                    i <<= 2;
                }

                return n - static_cast<int32_t>(i >> 31);
#endif
            }

            int32_t NumberOfLeadingZerosI64(int64_t i)
            {
                return NumberOfLeadingZerosU64(static_cast<uint64_t>(i));
            }

            int32_t NumberOfLeadingZerosU64(uint64_t i)
            {
                if (!i)
                    return 64;
#if defined(__GNUC__) || defined(__clang__)
                return __builtin_clzll(i);
#else

                int32_t n = 1;

                uint32_t x = static_cast<uint32_t>(i >> 32);

                if (x == 0) {
                    n += 32;
                    x = static_cast<uint32_t>(i);
                }

                if (x >> 16 == 0) {
                    n += 16;
                    x <<= 16;
                }

                if (x >> 24 == 0) {
                    n += 8;
                    x <<= 8;
                }

                if (x >> 28 == 0) {
                    n += 4;
                    x <<= 4;
                }

                if (x >> 30 == 0) {
                    n += 2;
                    x <<= 2;
                }

                n -= static_cast<int32_t>(x >> 31);

                return n;
#endif
            }

            int32_t BitCountI32(int32_t i)
            {
#if defined(__GNUC__) || defined(__clang__)
                return __builtin_popcount(i);
#else
                uint32_t ui = static_cast<uint32_t>(i);

                ui -= (ui >> 1) & 0x55555555;
                ui = (ui & 0x33333333) + ((ui >> 2) & 0x33333333);
                ui = (ui + (ui >> 4)) & 0x0f0f0f0f;
                ui += ui >> 8;
                ui += ui >> 16;

                return static_cast<int32_t>(ui & 0x3f);
#endif
            }

            int32_t BitLengthI32(int32_t i)
            {
                return 32 - NumberOfLeadingZerosI32(i);
            }

            int32_t BitLengthU32(uint32_t i)
            {
                return 32 - NumberOfLeadingZerosU32(i);
            }

            int32_t GetCapasityForSize(int32_t size)
            {
                assert(size > 0);

                if (size <= 8)
                    return 8;

                int32_t bl = BitLengthI32(size);

                if (bl > 30)
                    return INT32_MAX;

                int32_t res = 1 << bl;

                return size > res ? res << 1 : res;
            }

            int32_t DigitLength(uint64_t x)
            {
                // See http://graphics.stanford.edu/~seander/bithacks.html
                // for the details on the algorithm.

                if (x < 10)
                    return 1;

                int32_t r = ((64 - NumberOfLeadingZerosU64(x) + 1) * 1233) >> 12;

                assert(r <= UINT64_MAX_PRECISION);

                return (r == UINT64_MAX_PRECISION || x < TenPowerU64(r)) ? r : r + 1;
            }

            uint64_t TenPowerU64(int32_t n)
            {
                static const uint64_t TEN_POWERS_TABLE[UINT64_MAX_PRECISION] = {
                    1U,                     // 0  / 10^0
                    10U,                    // 1  / 10^1
                    100U,                   // 2  / 10^2
                    1000U,                  // 3  / 10^3
                    10000U,                 // 4  / 10^4
                    100000U,                // 5  / 10^5
                    1000000U,               // 6  / 10^6
                    10000000U,              // 7  / 10^7
                    100000000U,             // 8  / 10^8
                    1000000000U,            // 9  / 10^9
                    10000000000U,           // 10 / 10^10
                    100000000000U,          // 11 / 10^11
                    1000000000000U,         // 12 / 10^12
                    10000000000000U,        // 13 / 10^13
                    100000000000000U,       // 14 / 10^14
                    1000000000000000U,      // 15 / 10^15
                    10000000000000000U,     // 16 / 10^16
                    100000000000000000U,    // 17 / 10^17
                    1000000000000000000U,   // 18 / 10^18
                    10000000000000000000U   // 19 / 10^19
                };

                assert(n >= 0 && n < UINT64_MAX_PRECISION);

                return TEN_POWERS_TABLE[n];
            }
        }
    }
}
