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

#include "impl/compute/compute_client_impl.h"
#include "impl/message.h"

using namespace ignite::common::concurrent;

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace compute
            {
                void ComputeClientImpl::ExecuteJavaTask(int8_t flags, int64_t timeout, const std::string& taskName,
                    Writable& wrArg, Readable& res)
                {
                    ComputeTaskExecuteRequest req(flags, timeout, taskName, wrArg);
                    ComputeTaskFinishedNotification notification(res);

                    router.Get()->SyncMessageWithNotification(req, notification);

                    if (notification.IsFailure())
                        throw IgniteError(IgniteError::IGNITE_ERR_COMPUTE_TASK_CANCELLED,
                            notification.GetErrorMessage().c_str());

                }
            }
        }
    }
}
