/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

#ifndef _IGNITE_COMMON_FACTORY
#define _IGNITE_COMMON_FACTORY

#include <ignite/common/concurrent.h>

namespace ignite
{
    namespace common
    {
        /**
         * Factory class.
         *
         * @tparam T Instances of this type factory builds.
         */
        template<typename T>
        class Factory
        {
        public:
            /**
             * Destructor.
             */
            virtual ~Factory()
            {
                // No-op.
            }

            /**
             * Build instance.
             *
             * @return New instance of type @c T.
             */
            virtual common::concurrent::SharedPointer<T> Build() = 0;
        };
    }
}

#endif //_IGNITE_COMMON_FACTORY