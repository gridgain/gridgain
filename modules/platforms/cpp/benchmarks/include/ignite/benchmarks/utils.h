/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

#ifndef IGNITE_BENCHMARKS_UTILS_H
#define IGNITE_BENCHMARKS_UTILS_H

#ifdef _WIN32
#   include <windows.h>
#else
#   include <thread>
#   include <sys/time.h>
#   include <unistd.h>
#endif

#include <stdint.h>
#include <cstdlib>

#include <string>
#include <sstream>

namespace utils
{

#ifdef _WIN32

    uint64_t GetMicros()
    {
        FILETIME ft;
        GetSystemTimeAsFileTime(&ft);
        uint64_t tt = ft.dwHighDateTime;
        tt <<=32;
        tt |= ft.dwLowDateTime;

        return tt / 10;
    }

#else // NOT _WIN32

    uint64_t GetMicros()
    {
        struct timeval now;

        gettimeofday(&now, NULL);

        return (now.tv_sec) * 1000000 + now.tv_usec;
    }

    void Sleep(unsigned ms)
    {
        usleep(ms * 1000);
    }

#endif // _WIN32

    //inline unsigned GetRandomUnsigned(unsigned max)
    //{
    //    return static_cast<unsigned>(rand()) % max;
    //}

    //inline char GetRandomChar()
    //{
    //    static std::string alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    //    return alphabet[static_cast<size_t>(rand()) % alphabet.size()];
    //}

    //std::string GetRandomString(size_t size)
    //{
    //    std::string res;

    //    res.reserve(size + 2);

    //    res.push_back('\'');

    //    for (size_t i = 0; i < size; ++i)
    //        res.push_back(GetRandomChar());

    //    res.push_back('\'');

    //    return res;
    //}
}

#endif // IGNITE_BENCHMARKS_UTILS_H
