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

#include <ignite/thin/cache/query/query_fields_cursor.h>

#include "impl/cache/query/query_fields_cursor_impl.h"

namespace
{
    using namespace ignite::common::concurrent;
    using namespace ignite::impl::thin::cache::query;

    QueryFieldsCursorImpl& GetQueryFieldsCursorImpl(SharedPointer<void>& ptr)
    {
        return *reinterpret_cast<QueryFieldsCursorImpl*>(ptr.Get());
    }

    const QueryFieldsCursorImpl& GetQueryFieldsCursorImpl(const SharedPointer<void>& ptr)
    {
        return *reinterpret_cast<const QueryFieldsCursorImpl*>(ptr.Get());
    }
}

namespace ignite
{
    namespace thin
    {
        namespace cache
        {
            namespace query
            {
                QueryFieldsCursor::QueryFieldsCursor(const common::concurrent::SharedPointer<void> &impl) :
                    impl(impl)
                {
                    // No-op.
                }

                bool query::QueryFieldsCursor::HasNext()
                {
                    return GetQueryFieldsCursorImpl(impl).HasNext();
                }

                QueryFieldsRow QueryFieldsCursor::GetNext()
                {
                    return GetQueryFieldsCursorImpl(impl).GetNext();
                }

                const std::vector<std::string>& QueryFieldsCursor::GetColumnNames() const
                {
                    return GetQueryFieldsCursorImpl(impl).GetColumns();
                }
            }
        }
    }
}
