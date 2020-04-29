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

package org.apache.ignite.internal.visor.cache.index;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.h2.index.Index;
import org.h2.table.Column;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.commandline.CommandHandler.EMPTY_GROUP_NAME;

/**
 * Task that collects indexes information.
 */
@GridInternal
public class IndexListTask extends VisorOneNodeTask<IndexListTaskArg, Set<IndexListInfoContainer>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected IndexListJob job(IndexListTaskArg arg) {
        return new IndexListJob(arg, debug);
    }

    /** */
    private static class IndexListJob extends VisorJob<IndexListTaskArg, Set<IndexListInfoContainer>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected IndexListJob(@Nullable IndexListTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Set<IndexListInfoContainer> run(@Nullable IndexListTaskArg arg) throws IgniteException {
            if (arg == null)
                throw new IgniteException("IndexListTaskArg is null");

            String indexesRegEx = arg.indexesRegEx();
            String groupsRegEx = arg.groupsRegEx();
            String cachesRegEx = arg.cachesRegEx();

            Set<IndexListInfoContainer> idxInfos = new HashSet<>();

            GridQueryProcessor qry = ignite.context().query();

            IgniteH2Indexing indexing = (IgniteH2Indexing)qry.getIndexing();

            for (GridCacheContext ctx : ignite.context().cache().context().cacheContexts()) {
                final String cacheName = ctx.name();

                final String grpName = ctx.config().getGroupName();
                final String grpNameToValidate = grpName == null ? EMPTY_GROUP_NAME : grpName;

                if (!isNameValid(groupsRegEx, grpNameToValidate))
                    continue;

                if (!isNameValid(cachesRegEx, cacheName))
                    continue;

                for (GridQueryTypeDescriptor type : qry.types(cacheName)) {
                    GridH2Table gridH2Tbl = indexing.schemaManager().dataTable(cacheName, type.tableName());

                    if (gridH2Tbl == null)
                        continue;

                    for (Index idx : gridH2Tbl.getIndexes()) {
                        if (!isNameValid(indexesRegEx, idx.getName()))
                            continue;

                        if (idx instanceof H2TreeIndexBase)
                            idxInfos.add(constructContainer(ctx, idx));
                    }
                }
            }

            return idxInfos;
        }

        /** */
        private static IndexListInfoContainer constructContainer(GridCacheContext ctx, Index idx) {
            return new IndexListInfoContainer(
                ctx,
                idx.getName(),
                Arrays.stream(idx.getColumns()).map(Column::getName).collect(Collectors.toList()),
                idx.getTable().getName()
            );
        }

        /** */
        private static boolean isNameValid(String regEx, String name) {
            if (regEx == null || regEx.equalsIgnoreCase(name))
                return true;

            return Pattern.compile(regEx.toLowerCase()).matcher(name.toLowerCase()).find();
        }
    }
}
