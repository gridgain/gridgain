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

/**
 * @file
 * Declares ignite::impl::binary::BinaryObjectHeader class.
 */

#ifndef _IGNITE_IMPL_BINARY_BINARY_OBJECT_HEADER
#define _IGNITE_IMPL_BINARY_BINARY_OBJECT_HEADER

#include <stdint.h>

#include <ignite/common/common.h>

#include <ignite/impl/binary/binary_utils.h>
#include <ignite/impl/binary/binary_common.h>
#include <ignite/impl/interop/interop_memory.h>

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            /**
             * Binary object header class.
             *
             * @note Most methods are defined in header to encourage inlining.
             */
            class IGNITE_IMPORT_EXPORT BinaryObjectHeader
            {
            public:
                // Header size in bytes.
                enum { SIZE = IGNITE_DFLT_HDR_LEN };

                /**
                 * Create from InteropMemory instance.
                 * @throw IgniteError if the memory at the specified offset
                 *    is not a valid BinaryObject.
                 *
                 * @param mem Memory.
                 * @param offset Offset in memory.
                 * @return New BinaryObjectHeader instance.
                 */
                static BinaryObjectHeader FromMemory(interop::InteropMemory& mem, int32_t offset);

                /**
                 * Constructor.
                 *
                 * @param mem Pointer to header memory.
                 */
                explicit BinaryObjectHeader(interop::InteropMemory* mem, int32_t offset) :
                    mem(mem),
                    offset(offset)
                {
                    // No-op.
                }

                /**
                 * Copy constructor.
                 *
                 * @param other Instance to copy.
                 */
                BinaryObjectHeader(const BinaryObjectHeader& other) : 
                    mem(other.mem),
                    offset(other.offset)
                {
                    // No-op.
                }

                /**
                 * Assignment operator.
                 *
                 * @param other Other instance.
                 * @return Reference to this.
                 */
                BinaryObjectHeader& operator=(const BinaryObjectHeader& other)
                {
                    if (this != &other)
                    {
                        mem = other.mem;
                        offset = other.offset;
                    }

                    return *this;
                }

                /**
                 * Get header type.
                 *
                 * @return Header type.
                 */
                int8_t GetType() const
                {
                    return BinaryUtils::ReadInt8(*mem, offset);
                }

                /**
                 * Get version.
                 *
                 * @return Binary object layout version.
                 */
                int8_t GetVersion() const
                {
                    return BinaryUtils::ReadInt8(*mem, offset + IGNITE_OFFSET_PROTO_VER);
                }

                /**
                 * Get flags.
                 *
                 * @return Flags.
                 */
                int16_t GetFlags() const
                {
                    return BinaryUtils::ReadInt16(*mem, offset + IGNITE_OFFSET_FLAGS);
                }

                /**
                 * Get type ID.
                 *
                 * @return Type ID.
                 */
                int32_t GetTypeId() const
                {
                    return BinaryUtils::ReadInt32(*mem, offset + IGNITE_OFFSET_TYPE_ID);
                }

                /**
                 * Get hash code.
                 *
                 * @return Hash code.
                 */
                int32_t GetHashCode() const
                {
                    return BinaryUtils::ReadInt32(*mem, offset + IGNITE_OFFSET_HASH_CODE);
                }

                /**
                 * Get object length.
                 *
                 * @return Object length.
                 */
                int32_t GetLength() const
                {
                    return BinaryUtils::ReadInt32(*mem, offset + IGNITE_OFFSET_LEN);
                }

                /**
                 * Get schema ID.
                 *
                 * @return Schema ID.
                 */
                int32_t GetSchemaId() const
                {
                    return BinaryUtils::ReadInt32(*mem, offset + IGNITE_OFFSET_SCHEMA_ID);
                }

                /**
                 * Get schema offset.
                 *
                 * @return Schema offset.
                 */
                int32_t GetSchemaOffset() const
                {
                    return BinaryUtils::ReadInt32(*mem, offset + IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
                }

                /**
                 * Check if the binary object has schema.
                 *
                 * @return True if the binary object has schema.
                 */
                bool HasSchema() const
                {
                    return (GetFlags() & IGNITE_BINARY_FLAG_HAS_SCHEMA) != 0;
                }

                /**
                 * Check if the binary object is of user-defined type.
                 *
                 * @return True if the binary object is of user-defined type.
                 */
                bool IsUserType() const
                {
                    return (GetFlags() & IGNITE_BINARY_FLAG_USER_TYPE) != 0;
                }

                /**
                 * Get footer offset.
                 *
                 * @return Footer offset.
                 */
                int32_t GetFooterOffset() const
                {
                    // No schema: all we have is data. There is no offset in last 4 bytes.
                    if (!HasSchema())
                        return GetLength();

                    // There is schema. Regardless of raw data presence, footer starts with schema.
                    return GetSchemaOffset();
                }

                /**
                 * Get footer length.
                 *
                 * @return Footer length.
                 */
                int32_t GetFooterLength() const
                {
                    if (!HasSchema())
                        return 0;

                    return GetLength() - GetSchemaOffset();
                }

                /**
                 * Get size of data without header and footer.
                 *
                 * @return Data length.
                 */
                int32_t GetDataLength() const
                {
                    return GetFooterOffset() - SIZE;
                }

                /**
                 * Get underlying memory.
                 *
                 * @return Underlying memory.
                 */
                int8_t* GetMem();

            private:
                /** Memory containing binary object */
                interop::InteropMemory* mem;

                /** Position offset from the beginning of the memory. */
                int32_t offset;
            };
        }
    }
}

#endif //_IGNITE_IMPL_BINARY_BINARY_OBJECT_HEADER
