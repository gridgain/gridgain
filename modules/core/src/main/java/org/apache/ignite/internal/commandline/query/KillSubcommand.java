/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.commandline.query;

import org.apache.ignite.mxbean.ClientProcessorMXBean;
import org.apache.ignite.mxbean.ComputeMXBean;
import org.apache.ignite.mxbean.QueryMXBean;
import org.apache.ignite.mxbean.ServiceMXBean;
import org.apache.ignite.mxbean.TransactionsMXBean;

/**
 * Subcommands of the kill command.
 *
 * @see QueryMXBean
 * @see ComputeMXBean
 * @see TransactionsMXBean
 * @see ServiceMXBean
 * @see ClientProcessorMXBean
 */
public enum KillSubcommand {
    /** Kill compute task. */
    COMPUTE,
    /** Kill transaction. */
    TRANSACTION,
    /** Kill service. */
    SERVICE,
    /** Kill sql query. */
    SQL,
    /** Kill compute task. */
    CONTINUOUS,
    /** Kill scan query. */
    SCAN,
    /** Kill client connection. */
    CLIENT
}
