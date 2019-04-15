/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.commandline;

import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.cache.CacheCommand;
import org.apache.ignite.internal.commandline.cache.argument.DistributionCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.ListCommandArg;
import org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg;

/**
 * Command arguments factory used for tests.
 */
public class CommandArgFactory {
    /**
     * @param cmd Cache command.
     * @return Possible arguments for passed cache command {@code cmd} or empty array, if arguments not defined for
     * cache command.
     */
    public static CommandArg[] getArgs(CacheCommand cmd) {
        switch (cmd) {
            case RESET_LOST_PARTITIONS:
            case CONTENTION:
            case HELP:
                return new CommandArg[0];

            case DISTRIBUTION:
                return DistributionCommandArg.values();
            case IDLE_VERIFY:
                return IdleVerifyCommandArg.values();

            case LIST:
                return ListCommandArg.values();

            case VALIDATE_INDEXES:
                return ValidateIndexesCommandArg.values();
        }

        throw new IllegalArgumentException("Unknown cache command " + cmd);
    }

    /** Private constructor */
    private CommandArgFactory() {
        /* No-op. */
    }
}
