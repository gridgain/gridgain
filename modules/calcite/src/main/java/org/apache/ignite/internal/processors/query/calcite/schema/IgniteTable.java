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

package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.Iterator;
import java.util.function.Function;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.util.ScanIterator;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UNDEFINED_CACHE_ID;

/** */
public class IgniteTable extends AbstractTable implements TableDescriptor {
    private final GridQueryTypeDescriptor typeDescriptor;
    private final GridCacheContextInfo cacheInfo;
    private final GridQueryProperty[] props;

    IgniteTable(GridQueryTypeDescriptor typeDescriptor, GridCacheContextInfo cacheInfo) {
        this.typeDescriptor = typeDescriptor;
        this.cacheInfo = cacheInfo;

        props = typeDescriptor.properties().values().toArray(new GridQueryProperty[0]);
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        for (GridQueryProperty prop : props) {
            builder.add(prop.name(), typeFactory.createJavaType(prop.type()));
        }
        return builder.build();
    }

    @Override public boolean partitioned() {
        return cacheInfo.config().getCacheMode() == CacheMode.PARTITIONED;
    }

    @Override public boolean replicated() {
        return cacheInfo.config().getCacheMode() == CacheMode.REPLICATED;
    }

    @Override public <C> C unwrap(Class<C> aClass) {
        if (aClass.isInstance(typeDescriptor))
            return aClass.cast(typeDescriptor);
        else if (aClass.isInstance(cacheInfo))
            return aClass.cast(cacheInfo);
        else
            return super.unwrap(aClass);
    }

    public Enumerable<Object[]> enumerable() {
        return Linq4j.asEnumerable(() -> cacheIterator(this::asArray));
    }

    /** */
    private <T> GridCloseableIterator<T> cacheIterator(Function<CacheDataRow,T> converter) {
        GridCacheContext ctx = cacheInfo.cacheContext();

        int cacheId = cacheInfo.cacheContext().group().sharedGroup() ? cacheInfo.cacheId() : UNDEFINED_CACHE_ID;
        Iterator<GridDhtLocalPartition> parts = ctx.topology().localPartitions().iterator();

        return new ScanIterator<>(cacheId, parts, converter, this::matchRow);
    }

    /** */
    private boolean matchRow(CacheDataRow cacheDataRow) {
        return typeDescriptor.matchType(cacheDataRow.value());
    }

    /** */
    private Object[] asArray(CacheDataRow row) {
        try {
            Object[] vals = new Object[props.length];
            for (int i = 0; i < props.length; i++) {
                vals[i] = props[i].value(row.key(), row.value());
            }
            return vals;
        } catch (IgniteCheckedException e) {
            throw new IgniteSQLException("Failed to unwrap cache data row.", e);
        }
    }
}
