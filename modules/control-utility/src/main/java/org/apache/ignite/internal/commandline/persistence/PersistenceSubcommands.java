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

import org.apache.ignite.internal.visor.persistence.PersistenceOperation;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public enum PersistenceSubcommands {
    /** Collects information about corrupted caches and cache groups and their file system paths. */
    INFO("info", PersistenceOperation.INFO),

    /** Cleans partition files of corrupted caches and cache groups. */
    CLEAN("clean", PersistenceOperation.CLEAN),

    /** */
    BACKUP("backup", PersistenceOperation.BACKUP);

    /** Subcommand name. */
    private final String name;

    /** Operation this subcommand triggers. */
    private final PersistenceOperation operation;

    /**
     * @param name String representation of subcommand.
     * @param operation Operation this command triggers.
     */
    PersistenceSubcommands(String name, PersistenceOperation operation) {
        this.name = name;
        this.operation = operation;
    }

    /**
     * @param strRep String representation of subcommand.
     * @return Subcommand for its string representation.
     */
    public static @Nullable PersistenceSubcommands of(String strRep) {
        for (PersistenceSubcommands cmd : values()) {
            if (cmd.text().equals(strRep))
                return cmd;
        }

        return null;
    }

    /** */
    public String text() {
        return name;
    }

    /** */
    public PersistenceOperation operation() {
        return operation;
    }
}
