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

#include <ignite/thin/cache/query/query_fields_row.h>

#include "impl/cache/query/query_fields_row_impl.h"

namespace
{
    using namespace ignite::common::concurrent;
    using namespace ignite::impl::thin::cache::query;

    QueryFieldsRowImpl& GetQueryFieldsRowImpl(SharedPointer<void>& ptr)
    {
        return *reinterpret_cast<QueryFieldsRowImpl*>(ptr.Get());
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
                QueryFieldsRow::QueryFieldsRow(const common::concurrent::SharedPointer<void>& impl) :
                    impl(impl)
                {
                    // No-op.
                }

                bool QueryFieldsRow::HasNext()
                {
                    return GetQueryFieldsRowImpl(impl).HasNext();
                }

                void QueryFieldsRow::InternalGetNext(impl::thin::Readable &readable)
                {
                    GetQueryFieldsRowImpl(impl).GetNext(readable);
                }
            }
        }
    }    
}
