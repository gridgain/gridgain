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

/**
 * Thrown to indicate that the requested operation cannot be executed NOT due to some system problem (like a disk
 * failure or internal storage state being corrupted), but because the operation logic forbids the execution.
 * Such exceptions should not be logged by the 'system' loggers with ERROR level (or above).
 * For example, such exception might be thrown if a user command has invalid parameters, or if it's invalid for the
 * current system state.
 */
public class InvalidUserCommandCheckedException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates a new instance.
     *
     * @param msg Error message.
     */
    public InvalidUserCommandCheckedException(String msg) {
        super(msg);
    }
}
