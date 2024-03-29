/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.commandline.persistence;

import java.util.List;

/**
 * Arguments of "persistence cleaning" command.
 */
public class PersistenceArguments {
    /** */
    private PersistenceSubcommands cmd;

    /** */
    private CleanAndBackupSubcommandArg cleanArg;

    /** */
    private List<String> cachesList;

    /**
     * @param cmd
     */
    public PersistenceArguments(PersistenceSubcommands cmd, CleanAndBackupSubcommandArg cleanArg, List<String> cachesList) {
        this.cmd = cmd;
        this.cleanArg = cleanArg;
        this.cachesList = cachesList;
    }

    /** */
    public PersistenceSubcommands subcommand() {
        return cmd;
    }

    /** */
    public CleanAndBackupSubcommandArg cleanArg() {
        return cleanArg;
    }

    /** */
    public List<String> cachesList() {
        return cachesList;
    }

    /** Builder of {@link PersistenceArguments}. */
    public static class Builder {
        /** */
        private PersistenceSubcommands subCmd;

        /** */
        private CleanAndBackupSubcommandArg cleanSubCmdArg;

        /** */
        private List<String> cacheNames;

        /**
         * @param subCmd Subcommand.
         */
        public Builder(PersistenceSubcommands subCmd) {
            this.subCmd = subCmd;
        }

        /** */
        public Builder withCleanAndBackupSubcommandArg(CleanAndBackupSubcommandArg cleanSubCmdArg) {
            this.cleanSubCmdArg = cleanSubCmdArg;

            return this;
        }

        public Builder withCacheNames(List<String> cacheNames) {
            this.cacheNames = cacheNames;

            return this;
        }

        public PersistenceArguments build() {
            return new PersistenceArguments(
                subCmd,
                cleanSubCmdArg,
                cacheNames
            );
        }
    }
}
