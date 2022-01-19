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

#include "impl/data_channel.h"
#include "impl/message.h"

#include "impl/cache/query/continuous/continuous_query_notification_handler.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace cache
            {
                namespace query
                {
                    namespace continuous
                    {
                        ContinuousQueryNotificationHandler::ContinuousQueryNotificationHandler(DataChannel& channel,
                            const SP_ContinuousQueryClientHolderBase& continuousQuery) :
                            continuousQuery(continuousQuery),
                            channel(channel)
                        {
                            // No-op.
                        }

                        ContinuousQueryNotificationHandler::~ContinuousQueryNotificationHandler()
                        {
                            // No-op.
                        }

                        void ContinuousQueryNotificationHandler::OnNotification(const network::DataBuffer& msg)
                        {
                            ClientCacheEntryEventNotification notification(*continuousQuery.Get());
                            channel.DeserializeMessage(msg, notification);
                        }

                        void ContinuousQueryNotificationHandler::OnDisconnected()
                        {
                            continuousQuery.Get()->OnDisconnected();
                        }
                    }
                }
            }
        }
    }
}
