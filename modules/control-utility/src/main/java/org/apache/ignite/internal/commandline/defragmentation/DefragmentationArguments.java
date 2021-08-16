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

package org.apache.ignite.internal.commandline.defragmentation;

import java.util.List;

/** */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class DefragmentationArguments {
    /** */
    private final DefragmentationSubcommands subcmd;

    /** */
    private List<String> nodeIds;

    /** */
    private List<String> cacheNames;

    /** */
    public DefragmentationArguments(DefragmentationSubcommands subcmd) {
        this.subcmd = subcmd;
    }

    /** */
    public DefragmentationSubcommands subcommand() {
        return subcmd;
    }

    /** */
    public void setNodeIds(List<String> nodeIds) {
        this.nodeIds = nodeIds;
    }

    /** */
    public List<String> nodeIds() {
        return nodeIds;
    }

    /** */
    public void setCacheNames(List<String> cacheNames) {
        this.cacheNames = cacheNames;
    }

    /** */
    public List<String> cacheNames() {
        return cacheNames;
    }
}
