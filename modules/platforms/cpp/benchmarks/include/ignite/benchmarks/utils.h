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
#include <iostream>

namespace utils
{

template<typename TO, typename TI>
TO LexicalCast(const TI& in)
{
    TO out;

    std::stringstream converter;
    converter << in;
    converter >> out;

    return out;
}

template<>
std::string LexicalCast(const std::string& in)
{
    return in;
}

template<typename OutType = std::string>
OutType GetEnvVar(const std::string& name)
{
    char* var = std::getenv(name.c_str());

    if (!var)
        throw std::runtime_error("Environment variable '" + name + "' is not set");

    std::string varStr(var);
    std::cout << name << "=" << varStr << std::endl;

    return LexicalCast<OutType>(varStr);
}

template<typename OutType = std::string>
OutType GetEnvVar(const std::string& name, OutType dflt)
{
    char* var = std::getenv(name.c_str());

    if (!var)
    {
        std::cout << name << "=" << dflt << std::endl;

        return dflt;
    }

    std::string varStr(var);
    std::cout << name << "=" << varStr << std::endl;

    return LexicalCast<OutType>(varStr);
}

} // namespace utils

#endif // IGNITE_BENCHMARKS_UTILS_H
