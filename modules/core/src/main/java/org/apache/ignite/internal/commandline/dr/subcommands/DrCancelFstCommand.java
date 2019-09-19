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

package org.apache.ignite.internal.commandline.dr.subcommands;

import java.util.logging.Logger;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.dr.DrSubCommandsList;
import org.apache.ignite.internal.visor.dr.VisorDrCancelFstTaskArgs;
import org.apache.ignite.internal.visor.dr.VisorDrCancelFstTaskResult;
import org.apache.ignite.lang.IgniteUuid;

/** */
public class DrCancelFstCommand extends
    DrAbstractRemoteSubCommand<VisorDrCancelFstTaskArgs, VisorDrCancelFstTaskResult, DrCancelFstCommand.DrCancelFstArguments>
{
    /** {@inheritDoc} */
    @Override protected String visorTaskName() {
        return "org.gridgain.grid.internal.visor.dr.console.VisorDrCancelFstTask";
    }

    /** {@inheritDoc} */
    @Override public DrCancelFstArguments parseArguments0(CommandArgIterator argIter) {
        String fstId = argIter.nextArg("Transfer id expected.");

        try {
            IgniteUuid.fromString(fstId);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Transfer id must be a ignite UUID (<hex-number>-<UUID>).", e);
        }

        return new DrCancelFstArguments(fstId);
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return "Warning: this command will cancel data center full state transfer with given id.";
    }

    /** {@inheritDoc} */
    @Override protected void printResult(VisorDrCancelFstTaskResult res, Logger log) {
        if (res.message() != null)
            log.info(res.message());
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return DrSubCommandsList.FULL_STATE_TRANSFER.text();
    }

    /** */
    @SuppressWarnings("PublicInnerClass")
    public static class DrCancelFstArguments implements DrAbstractRemoteSubCommand.Arguments<VisorDrCancelFstTaskArgs> {
        /** Id. */
        private final String fstId;

        /**
         * @param fstId Id.
         */
        public DrCancelFstArguments(String fstId) {
            this.fstId = fstId;
        }

        /** {@inheritDoc} */
        @Override public VisorDrCancelFstTaskArgs toVisorArgs() {
            return new VisorDrCancelFstTaskArgs(fstId);
        }
    }
}
