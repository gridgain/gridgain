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
#ifndef _IGNITE_COMMON_DYNAMIC_SIZE_ARRAY
#define _IGNITE_COMMON_DYNAMIC_SIZE_ARRAY

#include <stdint.h>
#include <cassert>

#include <utility>

#include <ignite/common/common.h>
#include <ignite/common/default_allocator.h>
#include <ignite/common/bits.h>

namespace ignite
{
    namespace common
    {
        /**
         * Dynamic size array is safe container abstraction with a dynamic size.
         * This is the analogue of the standard vector. It is needed to be used
         * in exported classes as we can't export standard library classes.
         */
        template<typename T, typename A = DefaultAllocator<T> >
        class IGNITE_IMPORT_EXPORT DynamicSizeArray
        {
        public:
            typedef T ValueType;
            typedef A AllocatorType;
            typedef typename AllocatorType::SizeType SizeType;
            typedef typename AllocatorType::PointerType PointerType;
            typedef typename AllocatorType::ConstPointerType ConstPointerType;
            typedef typename AllocatorType::ReferenceType ReferenceType;
            typedef typename AllocatorType::ConstReferenceType ConstReferenceType;

            /**
             * Default constructor.
             *
             * Constructs zero-size and zero-capacity array.
             */
            DynamicSizeArray(const AllocatorType& allocator = AllocatorType()) :
                alloc(allocator),
                size(0),
                capacity(0),
                data(0)
            {
                // No-op.
            }

            /**
             * Constructor.
             * Constructs an empty array with the specified capacity.
             *
             * @param len Array length.
             * @param allocator Allocator.
             */
            DynamicSizeArray(SizeType len, const AllocatorType& allocator = AllocatorType()) :
                alloc(allocator),
                size(0),
                capacity(bits::GetCapacityForSize(len)),
                data(alloc.Allocate(capacity))
            {
                // No-op.
            }

            /**
             * Raw array constructor.
             *
             * @param arr Raw array.
             * @param len Array length in elements.
             */
            DynamicSizeArray(ConstPointerType arr, SizeType len,
                const AllocatorType& allocator = AllocatorType()) :
                alloc(allocator),
                size(0),
                capacity(0),
                data(0)
            {
                Assign(arr, len);
            }

            /**
             * Copy constructor.
             *
             * @param other Other instance.
             */
            DynamicSizeArray(const DynamicSizeArray<T>& other) :
                alloc(),
                size(0),
                capacity(0),
                data(0)
            {
                Assign(other);
            }

            /**
             * Destructor.
             */
            ~DynamicSizeArray()
            {
                Deallocate();
            }

            /**
             * Assignment operator.
             *
             * @param other Other instance.
             * @return Reference to this instance.
             */
            DynamicSizeArray<T>& operator=(const DynamicSizeArray<T>& other)
            {
                Assign(other);

                return *this;
            }

            /**
             * Assign new value to the array.
             *
             * @param other Another array instance.
             */
            void Assign(const DynamicSizeArray<T>& other)
            {
                if (this != &other)
                {
                    alloc = other.alloc;

                    Assign(other.GetData(), other.GetSize());
                }
            }

            /**
             * Assign new value to the array.
             *
             * @param src Raw array.
             * @param len Array length in elements.
             */
            void Assign(ConstPointerType src, SizeType len)
            {
                assert(src);

                if (data == src) {
                    assert(len <= size);

                    Resize(len);
                    return;
                }

                if (capacity < len)
                {
                    Deallocate();

                    capacity = bits::GetCapacityForSize(len);
                    data = alloc.Allocate(capacity);

                    assert(data);
                }

                for (SizeType i = 0; i < len; ++i) {
                    if (i < size) {
                        data[i] = src[i];
                    } else {
                        alloc.Construct(data + i, src[i]);
                    }
                }

                size = len;
            }

            /**
             * Append several values to the array.
             *
             * @param src Raw array.
             * @param len Array length in elements.
             */
            void Append(ConstPointerType src, SizeType len)
            {
                assert(src);

                Reserve(size + len);

                for (SizeType i = 0; i < len; ++i)
                    alloc.Construct(data + size + i, src[i]);

                size += len;
            }

            /**
             * Swap the content of the array with another instance.
             *
             * @param other Instance to swap with.
             */
            void Swap(DynamicSizeArray<T>& other)
            {
                if (this != &other)
                {
                    std::swap(alloc, other.alloc);
                    std::swap(size, other.size);
                    std::swap(capacity, other.capacity);
                    std::swap(data, other.data);
                }
            }

            /**
             * Get data pointer.
             *
             * @return Data pointer.
             */
            PointerType GetData()
            {
                return data;
            }

            /**
             * Get data pointer.
             *
             * @return Data pointer.
             */
            ConstPointerType GetData() const
            {
                return data;
            }

            /**
             * Get array size.
             *
             * @return Array size.
             */
            SizeType GetSize() const
            {
                return size;
            }

            /**
             * Get capacity.
             *
             * @return Array capacity.
             */
            SizeType Getcapacity() const
            {
                return capacity;
            }

            /**
             * Element access operator.
             *
             * @param idx Element index.
             * @return Element reference.
             */
            ReferenceType operator[](SizeType idx)
            {
                assert(idx < size);

                return data[idx];
            }

            /**
             * Element access operator.
             *
             * @param idx Element index.
             * @return Element reference.
             */
            ConstReferenceType operator[](SizeType idx) const
            {
                assert(idx < size);

                return data[idx];
            }

            /**
             * Check if the array is empty.
             *
             * @return True if the array is empty.
             */
            bool IsEmpty() const
            {
                return size == 0;
            }

            /**
             * Clears the array.
             */
            void Clear()
            {
                for (PointerType it = data; it != data + size; ++it)
                    alloc.Destruct(it);

                size = 0;
            }

            /**
             * Reserves not less than specified elements number so array is not
             * going to grow on append.
             *
             * @param newCapacity Desired capacity.
             */
            void Reserve(SizeType newCapacity)
            {
                if (capacity < newCapacity)
                {
                    DynamicSizeArray<T> tmp(newCapacity);

                    tmp.Assign(*this);

                    Swap(tmp);
                }
            }

            /**
             * Resizes the array. Destructs elements if the specified size is less
             * than the array's size. Default-constructs elements if the
             * specified size is more than the array's size.
             *
             * @param newSize Desired size.
             */
            void Resize(SizeType newSize)
            {
                if (capacity < newSize)
                    Reserve(newSize);

                if (newSize > size)
                {
                    for (PointerType it = data + size; it < data + newSize; ++it)
                        alloc.Construct(it, ValueType());
                }
                else
                {
                    for (PointerType it = data + newSize; it < data + size; ++it)
                        alloc.Destruct(it);
                }

                size = newSize;
            }

            /**
             * Get last element.
             *
             * @return Last element reference.
             */
            const ValueType& Back() const
            {
                assert(size > 0);

                return data[size - 1];
            }

            /**
             * Get last element.
             *
             * @return Last element reference.
             */
            ValueType& Back()
            {
                assert(size > 0);

                return data[size - 1];
            }

            /**
             * Get first element.
             *
             * @return First element reference.
             */
            const ValueType& Front() const
            {
                assert(size > 0);

                return data[0];
            }

            /**
             * Get first element.
             *
             * @return First element reference.
             */
            ValueType& Front()
            {
                assert(size > 0);

                return data[0];
            }

            /**
             * Pushes new value to the back of the array, effectively increasing
             * array size by one.
             *
             * @param val Value to push.
             */
            void PushBack(ConstReferenceType val)
            {
                Resize(size + 1);

                Back() = val;
            }

        private:
            /**
             * Deallocate the internal buffer leaving the array in the valid state.
             */
            void Deallocate() {
                Clear();

                alloc.Deallocate(data, capacity);
                capacity = 0;
            }

            /** Allocator */
            AllocatorType alloc;

            /** Array size. */
            SizeType size;

            /** Array capacity. */
            SizeType capacity;

            /** Data. */
            PointerType data;
        };
    }
}

#endif // _IGNITE_COMMON_DYNAMIC_SIZE_ARRAY
