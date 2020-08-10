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

package org.apache.ignite.internal.commandline.meta.subcommands;

import java.util.logging.Logger;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.meta.MetadataSubCommandsList;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataInfoTask;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataListResult;

/** */
public class MetadataListCommand
    extends MetadataAbstractSubCommand<VoidDto, MetadataListResult>
{
    /** {@inheritDoc} */
    @Override protected String taskName() {
        return MetadataInfoTask.class.getName();
    }

    /** {@inheritDoc} */
    @Override public VoidDto parseArguments0(CommandArgIterator argIter) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void printResult(MetadataListResult res, Logger log) {
        for (BinaryMetadata m : res.metadata()) {
            log.info("typeId=" + printInt(m.typeId()) +
                ", typeName=" + m.typeName() +
                ", fields=" + m.fields().size() +
                ", schemas=" + m.schemas().size() +
                ", isEnum=" + m.isEnum());
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return MetadataSubCommandsList.LIST.text();
    }
}
