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
 * Declares ignite::cache::query::ScanQuery class.
 */

#ifndef _IGNITE_CACHE_QUERY_QUERY_SCAN
#define _IGNITE_CACHE_QUERY_QUERY_SCAN

#include <stdint.h>
#include <string>

#include "ignite/binary/binary_raw_writer.h"

namespace ignite
{
    namespace cache
    {
        namespace query
        {
            /**
             * Scan query.
             */
            class ScanQuery
            {
            public:
                /**
                 * Default constructor.
                 */
                ScanQuery() : part(-1), pageSize(1024), loc(false)
                {
                    // No-op.
                }

                /**
                 * Constructor.
                 *
                 * @param part Partition.
                 */
                ScanQuery(int32_t part) : part(part), pageSize(1024), loc(false)
                {
                    // No-op.
                }

                /**
                 * Get partition to scan.
                 *
                 * @return Partition to scan.
                 */
                int32_t GetPartition() const
                {
                    return part;
                }

                /**
                 * Set partition to scan.
                 *
                 * @param part Partition to scan.
                 */
                void SetPartition(int32_t part)
                {
                    this->part = part;
                }

                /**
                 * Get page size.
                 *
                 * @return Page size.
                 */
                int32_t GetPageSize() const
                {
                    return pageSize;
                }

                /**
                 * Set page size.
                 *
                 * @param pageSize Page size.
                 */
                void SetPageSize(int32_t pageSize)
                {
                    this->pageSize = pageSize;
                }

                /**
                 * Get local flag.
                 *
                 * @return Local flag.
                 */
                bool IsLocal() const
                {
                    return loc;
                }

                /**
                 * Set local flag.
                 *
                 * @param val Value of the flag. If true, query will be
                 *     executed only on local node, so only local entries
                 *     will be returned as query result.
                 */
                void SetLocal(bool loc)
                {
                    this->loc = loc;
                }

                /**
                 * Write query info to the stream.
                 *
                 * @param writer Writer.
                 */
                void Write(binary::BinaryRawWriter& writer) const
                {
                    writer.WriteBool(loc);
                    writer.WriteInt32(pageSize);

                    if (part < 0)
                        writer.WriteBool(false);
                    else
                    {
                        writer.WriteBool(true);
                        writer.WriteInt32(part);
                    }

                    writer.WriteNull(); // Predicates are not supported yet.
                }

            private:
                /** Partition. */
                int32_t part;

                /** Page size. */
                int32_t pageSize;

                /** Local flag. */
                bool loc;
            };
        }
    }    
}

#endif //_IGNITE_CACHE_QUERY_QUERY_SCAN