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

#ifndef _IGNITE_IMPL_THIN_COPYABLE
#define _IGNITE_IMPL_THIN_COPYABLE

#include <ignite/binary/binary_raw_writer.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /**
             * Abstraction to any type that can be copied.
             *
             * @tparam T Type of returned copy.
             */
            template<typename T>
            class Copyable
            {
            public:
                /** Type of returned copy. */
                typedef T CopyType;

                /**
                 * Destructor.
                 */
                virtual ~Copyable()
                {
                    // No-op.
                }

                /**
                 * Copy value.
                 *
                 * @return A copy of the object.
                 */
                virtual CopyType* Copy() const = 0;
            };

            /**
             * Implementation of the Copyable class template for a concrete type.
             *
             * @tparam T Type of returned copy.
             */
            template<typename T>
            class CopyableImpl : public Copyable<T>
            {
            public:
                /** Type of returned copy. */
                typedef T CopyType;

                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                CopyableImpl(const CopyType& value) :
                    value(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~CopyableImpl()
                {
                    // No-op.
                }

                /**
                 * Copy value.
                 *
                 * @return A copy of the object.
                 */
                virtual CopyType* Copy() const
                {
                    return new CopyType(value);
                }

            private:
                /** Value. */
                const CopyType& value;
            };
        }
    }
}

#endif // _IGNITE_IMPL_THIN_COPYABLE
