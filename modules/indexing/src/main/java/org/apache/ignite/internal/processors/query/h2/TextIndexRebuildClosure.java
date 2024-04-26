/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query.h2;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneIndex;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;

/**
 * Closure to rebuild text index.
 */
public class TextIndexRebuildClosure implements SchemaIndexCacheVisitorClosure {
    /** Query processor. */
    private final GridQueryProcessor qryProc;

    /** Cache context. */
    private final GridCacheContext cctx;

    /** Map of lucene fulltext index by table name. */
    private final Map<String, GridLuceneIndex> luceneIdxByTblName;

    /** Lucene fulltext index. */
    private final GridLuceneIndex luceneIdx;

    /**
     * @param qryProc Query processor.
     * @param cctx Cache context.
     * @param descriptors Collection of table descriptors.
     */
    public TextIndexRebuildClosure(
        GridQueryProcessor qryProc,
        GridCacheContext cctx,
        Collection<H2TableDescriptor> descriptors
    ) {
        this.qryProc = qryProc;
        this.cctx = cctx;

        assert descriptors != null;

        if (descriptors.size() == 1) {
            luceneIdx = F.first(descriptors).luceneIndex();
            luceneIdxByTblName = null;
        }
        else {
            luceneIdx = null;
            luceneIdxByTblName = unmodifiableMap(descriptors.stream()
                .collect(toMap(H2TableDescriptor::tableName, H2TableDescriptor::luceneIndex)));
        }
    }

    /** {@inheritDoc} */
    @Override public void apply(CacheDataRow row) throws IgniteCheckedException {
        GridLuceneIndex luceneIndex0;

        if (luceneIdx != null)
            luceneIndex0 = luceneIdx;
        else {
            @Nullable QueryTypeDescriptorImpl type = qryProc.typeByValue(cctx.name(), cctx.cacheObjectContext(),
                row.key(), row.value(), false);

            if (type == null)
                return;

            luceneIndex0 = luceneIdxByTblName.get(type.tableName());
        }

        if (luceneIndex0 == null)
            return;

        long expireTime = row.expireTime();

        if (expireTime == 0L)
            expireTime = Long.MAX_VALUE;

        luceneIndex0.store(row.key(), row.value(), row.version(), expireTime);
    }
}
