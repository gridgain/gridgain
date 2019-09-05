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
import org.apache.ignite.internal.visor.dr.VisorDrStateTaskArgs;
import org.apache.ignite.internal.visor.dr.VisorDrStateTaskResult;

/** */
public class DrStateCommand extends
    DrAbstractSubCommand<VisorDrStateTaskArgs, VisorDrStateTaskResult, DrStateCommand.DrStateArguments>
{
    /** {@inheritDoc} */
    @Override protected String visorTaskName() {
        return "org.gridgain.grid.internal.visor.dr.console.VisorDrStateTask";
    }

    /** {@inheritDoc} */
    @Override protected DrStateArguments parseArguments0(CommandArgIterator argIter) {
        boolean verbose = false;

        if ("--verbose".equalsIgnoreCase(argIter.peekNextArg())) {
            argIter.nextArg("--verbose is expected");

            verbose = true;
        }

        return new DrStateArguments(verbose);
    }

    /** {@inheritDoc} */
    @Override protected void printResult(VisorDrStateTaskResult res, Logger log) {

    }

    /** {@inheritDoc} */
    @Override public String name() {
        return DrSubCommandsList.STATE.text();
    }

    /** */
    @SuppressWarnings("PublicInnerClass")
    public static class DrStateArguments implements DrAbstractSubCommand.Arguments<VisorDrStateTaskArgs> {
        /** */
        public final boolean verbose;

        /** */
        public DrStateArguments(boolean verbose) {
            this.verbose = verbose;
        }

        /** {@inheritDoc} */
        @Override public VisorDrStateTaskArgs toVisorArgs() {
            return new VisorDrStateTaskArgs(verbose);
        }
    }
}
