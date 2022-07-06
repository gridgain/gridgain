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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.DiscoverySpi;

import static org.apache.ignite.internal.IgniteFeatures.HAS_INVALID_USER_COMMAND_EXCEPTION;

/**
 * Utils to work with exceptions related to user-supplied commands.
 */
public class UserCommandExceptions {
    /**
     * Returns either {@link InvalidUserCommandException} (if every node in the cluster has this exception class) or
     * {@link IgniteException} (otherwise) with the given message.
     *
     * @param message Exception message.
     * @param discoverySpi SPI to use to determine whether the exception class is supported.
     * @return Created exception.
     */
    public static IgniteException invalidUserCommandException(String message, DiscoverySpi discoverySpi) {
        if (IgniteFeatures.allNodesSupport(null, discoverySpi, HAS_INVALID_USER_COMMAND_EXCEPTION))
            return new InvalidUserCommandException(message);
        else
            return new IgniteException(message);
    }

    /**
     * Returns either {@link InvalidUserCommandException} (if every node in the cluster has this exception class) or
     * {@link IgniteException} (otherwise) with the given message.
     *
     * @param message Exception message.
     * @param ctx     Context to use to determine whether the exception class is supported.
     * @return Created exception.
     */
    public static IgniteException invalidUserCommandException(String message, GridKernalContext ctx) {
        return invalidUserCommandException(message, ctx.config().getDiscoverySpi());
    }

    /**
     * Returns either {@link InvalidUserCommandCheckedException} (if every node in the cluster has this exception class)
     * or {@link IgniteCheckedException} (otherwise) with the given message.
     *
     * @param message Exception message.
     * @param discoverySpi SPI to use to determine whether the exception class is supported.
     * @return Created exception.
     */
    public static IgniteCheckedException invalidUserCommandCheckedException(String message, DiscoverySpi discoverySpi) {
        if (IgniteFeatures.allNodesSupport(null, discoverySpi, HAS_INVALID_USER_COMMAND_EXCEPTION))
            return new InvalidUserCommandCheckedException(message);
        else
            return new IgniteCheckedException(message);
    }

    /**
     * Returns either {@link InvalidUserCommandCheckedException} (if every node in the cluster has this exception class)
     * or {@link IgniteCheckedException} (otherwise) with the given message.
     *
     * @param message Exception message.
     * @param ctx     Context to use to determine whether the exception class is supported.
     * @return Created exception.
     */
    public static IgniteCheckedException invalidUserCommandCheckedException(String message, GridKernalContext ctx) {
        return invalidUserCommandCheckedException(message, ctx.config().getDiscoverySpi());
    }

    /**
     * Returns {@code true} if the given exception is caused by {@link InvalidUserCommandException} or
     * {@link InvalidUserCommandCheckedException}.
     *
     * @param ex The exception to check.
     * @return {@code true} if the given exception is an instance of {@link InvalidUserCommandException} or
     *     {@link InvalidUserCommandCheckedException}.
     */
    public static boolean causedByUserCommandException(Throwable ex) {
        return X.hasCause(ex, InvalidUserCommandException.class)
                || X.hasCause(ex, InvalidUserCommandCheckedException.class);
    }
}
