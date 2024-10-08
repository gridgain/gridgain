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
#ifndef _IGNITE_COMMON_COMMON
#define _IGNITE_COMMON_COMMON

#define IGNITE_EXPORT __declspec(dllexport)
#define IGNITE_IMPORT __declspec(dllimport)
#define IGNITE_CALL __stdcall

#define IGNITE_IMPORT_EXPORT IGNITE_EXPORT

#include <iostream>

#define IGNITE_TRACE_ALLOC(addr) \
    std::cout << "ALLOC " << __FILE__ << "(" << __LINE__ << "): 0x" << (void*)addr << std::endl;

/**
 * Common construction to disable copy constructor and assignment for class.
 */
#define IGNITE_NO_COPY_ASSIGNMENT(cls) \
    cls(const cls& src); \
    cls& operator= (const cls& other);

#if (__cplusplus >= 201103L)
#   define IGNITE_NO_THROW noexcept
#else
#   define IGNITE_NO_THROW throw()
#endif

#define IGNITE_UNUSED(x) ((void) x)

#define IGNORE_SIGNED_OVERFLOW
#define IGNORE_FALSE_UNDEFINED

#endif //_IGNITE_COMMON_COMMON