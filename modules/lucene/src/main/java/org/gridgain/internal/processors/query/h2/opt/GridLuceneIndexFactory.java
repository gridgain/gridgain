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

package org.gridgain.internal.processors.query.h2.opt;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.LuceneIndex;
import org.apache.ignite.internal.processors.query.h2.opt.LuceneIndexExtension;
import org.apache.ignite.internal.processors.query.h2.opt.LuceneIndexFactory;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.nonNull;

/**
 *  Factory for {@link LuceneIndex} implementations.
 *  If there is no extensions defined, returns {@link GridLuceneTextIndex}.
 */
public class GridLuceneIndexFactory implements LuceneIndexFactory {
    /** Context. */
    private final GridKernalContext ctx;

    private final IgniteLogger log;

    private final LuceneIndexExtension luceneIndexExt;

    /**
     * @param ctx Context.
     */
    public GridLuceneIndexFactory(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(GridLuceneIndexFactory.class);
        this.luceneIndexExt = loadLuceneIndexExtension(ctx);
    }

    private LuceneIndexExtension loadLuceneIndexExtension(GridKernalContext ctx) {
        if (nonNull(ctx.plugins())) {
            LuceneIndexExtension[] ext = ctx.plugins().extensions(LuceneIndexExtension.class);

            if (nonNull(ext)) {
                if (ext.length > 1)
                    log.info("More than one LuceneIndex extension is defined.");

                return ext[0];
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    public LuceneIndex createIndex(
        @Nullable String cacheName,
        GridQueryTypeDescriptor type
    ) throws IgniteException {
        if (luceneIndexExt != null)
            return luceneIndexExt.createIndex(ctx, cacheName, type);

        return new GridLuceneTextIndex(ctx, cacheName, type);
    }
}
