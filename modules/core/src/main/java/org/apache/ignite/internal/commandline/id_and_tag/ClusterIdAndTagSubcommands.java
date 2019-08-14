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

package org.apache.ignite.internal.commandline.id_and_tag;

import org.apache.ignite.internal.visor.id_and_tag.VisorIdAndTagOperation;

/**
 *
 */
public enum ClusterIdAndTagSubcommands {
    /** */
    VIEW("view", VisorIdAndTagOperation.VIEW),

    /** */
    CHANGE_TAG("change-tag", VisorIdAndTagOperation.CHANGE_TAG);

    /** */
    private String name;

    /** */
    private VisorIdAndTagOperation op;

    /**
     * @param name Name.
     * @param op Op.
     */
    ClusterIdAndTagSubcommands(String name, VisorIdAndTagOperation op) {
        this.name = name;
        this.op = op;
    }

    /** */
    public static ClusterIdAndTagSubcommands of(String name) {
        for (ClusterIdAndTagSubcommands subCmd : ClusterIdAndTagSubcommands.values()) {
            if (subCmd.name.equalsIgnoreCase(name))
                return subCmd;
        }

        return null;
    }

    /** */
    public String text() {
        return name;
    }

    /** */
    public VisorIdAndTagOperation operation() {
        return op;
    }
}
