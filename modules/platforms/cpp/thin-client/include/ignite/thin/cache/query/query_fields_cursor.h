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
 * Declares ignite::thin::cache::query::QueryFieldsCursor class.
 */

#ifndef _IGNITE_THIN_CACHE_QUERY_QUERY_FIELDS_CURSOR
#define _IGNITE_THIN_CACHE_QUERY_QUERY_FIELDS_CURSOR

#include <vector>

#include <ignite/common/concurrent.h>

#include <ignite/ignite_error.h>
#include <ignite/thin/cache/query/query_fields_row.h>

namespace ignite
{
    namespace thin
    {
        namespace cache
        {
            namespace query
            {
                /**
                 * Query fields cursor.
                 *
                 * This class is implemented as a reference to an implementation so copying of this class instance will
                 * only create another reference to the same underlying object. Underlying object will be released
                 * automatically once all the instances are destructed.
                 */
                class IGNITE_IMPORT_EXPORT QueryFieldsCursor
                {
                public:
                    /**
                     * Constructor.
                     *
                     * Internal method. Should not be used by user.
                     *
                     * @param impl Implementation.
                     */
                    explicit QueryFieldsCursor(const common::concurrent::SharedPointer<void>& impl);

                    /**
                     * Check whether next entry exists.
                     *
                     * @return True if next entry exists.
                     *
                     * @throw IgniteError class instance in case of failure.
                     */
                    bool HasNext();

                    /**
                     * Get next entry.
                     *
                     * @return Next entry.
                     *
                     * @throw IgniteError class instance in case of failure.
                     */
                    QueryFieldsRow GetNext();

                    /**
                     * Get column names.
                     *
                     * @return Column names.
                     */
                    const std::vector<std::string>& GetColumnNames() const;

                private:
                    /** Implementation delegate. */
                    common::concurrent::SharedPointer<void> impl;
                };
            }
        }
    }
}

#endif //_IGNITE_THIN_CACHE_QUERY_QUERY_FIELDS_CURSOR
