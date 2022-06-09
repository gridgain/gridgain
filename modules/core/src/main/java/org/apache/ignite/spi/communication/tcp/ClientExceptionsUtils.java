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

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.IgniteSpiException;

/**
 * Utils to analyze client-related exceptions.
 */
public class ClientExceptionsUtils {
    /**
     * Returns {@code true} if the exception relates to cluster topology change that prevents a connection, AND the given node is client.
     *
     * @param t    The exception we analyze.
     * @param node Node to which we tried to send a message, but the send produced the given exception.
     * @return {@code true} if the exception relates to cluster topology change that prevents a connection, AND the given node is client.
     */
    public static boolean isClientNodeTopologyException(Throwable t, ClusterNode node) {
        ClusterTopologyCheckedException ex = X.cause(t, ClusterTopologyCheckedException.class);

        return ex != null && node.isClient();
    }

    /**
     * Returns {@code true} if the exception that is provided is thrown because an attempt to open a direct connection
     * was made while only inverse connections are allowed.
     *
     * @param t Exception to inspect.
     * @return {@code true} if the exception that is provided is thrown because an attempt to open a direct connection
     *     was made while only inverse connections are allowed.
     */
    public static boolean isAttemptToEstablishDirectConnectionWhenOnlyInverseIsAllowed(Throwable t) {
        IgniteSpiException igniteSpiException = X.cause(t, IgniteSpiException.class);

        return igniteSpiException != null && igniteSpiException.getMessage() != null
            && igniteSpiException.getMessage().contains(
                "because it is started in 'forceClientToServerConnections' mode; inverse connection will be requested");
    }
}
