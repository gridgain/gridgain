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

package org.apache.ignite.internal.processors.rest.handlers.redis;

import java.util.Collection;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.util.nio.GridNioSession;

/**
 * Command handler.
 */
public interface GridRedisCommandHandler {
    /**
     * @return Collection of supported commands.
     */
    public Collection<GridRedisCommand> supportedCommands();

    /**
     * @param ses Session.
     * @param msg Request message.
     * @return Future.
     */
    public IgniteInternalFuture<GridRedisMessage> handleAsync(GridNioSession ses, GridRedisMessage msg);
}
