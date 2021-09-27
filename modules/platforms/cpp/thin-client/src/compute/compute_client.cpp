/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

#include <ignite/impl/thin/writable.h>
#include <ignite/impl/thin/readable.h>
#include <ignite/thin/compute/compute_client.h>

#include "impl/compute/compute_client_impl.h"

using namespace ignite::impl::thin;
using namespace compute;
using namespace ignite::common::concurrent;

namespace
{
    ComputeClientImpl& GetComputeClientImpl(SharedPointer<void>& ptr)
    {
        return *reinterpret_cast<ComputeClientImpl*>(ptr.Get());
    }
}

namespace ignite
{
    namespace thin
    {
        namespace compute
        {
            void ComputeClient::InternalExecuteJavaTask(const std::string& taskName, Writable& wrArg, Readable& res)
            {
                GetComputeClientImpl(impl).ExecuteJavaTask(flags, timeout, taskName, wrArg, res);
            }
        }
    }
}
