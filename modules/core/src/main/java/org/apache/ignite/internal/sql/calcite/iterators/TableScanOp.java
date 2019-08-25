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
package org.apache.ignite.internal.sql.calcite.iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.sql.calcite.Column;
import org.apache.ignite.internal.sql.calcite.IgniteTable;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * TODO: Add class description.
 */
public class TableScanOp extends PhysicalOperator {

    final IgniteTable tbl;

    final IgniteInternalCache cache;

    public TableScanOp(IgniteTable tbl, IgniteInternalCache cache) {
        this.tbl = tbl;
        this.cache = cache;
    }

    @Override public Iterator<List<?>> iterator(List<List<?>> ... input) {
        try {
            Iterator<CacheDataRow> it = cache.context().offheap().cacheIterator(cache.context().cacheId(),
                true, false, cache.context().topology().readyTopologyVersion(),
                null, false);

            return new TableScanIterator(it, tbl.columns(), tbl.typeId()); // TODO: CODE: implement.
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    @Override public void init() {
        execute(null);
    }

    private class TableScanIterator implements Iterator<List<?>>{

        private final Iterator<CacheDataRow> it;

        final List<Column> cols;

        final int typeId;

        List<?> next = null;

        private TableScanIterator(Iterator<CacheDataRow> it, List<Column> cols, int typeId) {
            this.it = it;
            this.cols = cols;
            this.typeId = typeId;
        }

        @Override public boolean hasNext() {
            if (next == null)
                next = findNext();

            return next != null;
        }

        @Override public List<?> next() {
            if (next == null)
                next = findNext();

            if (next == null)
                throw new NoSuchElementException();

            List<?> res = next;
            next = null;

            System.out.println("Table scan locNode=" + cache.context().localNodeId().toString().substring(0,2) + ", next=" + res);

            return res;
        }

        private List<?>  findNext() {
            while (it.hasNext()) {
                CacheDataRow e = it.next();

                BinaryObject val = (BinaryObject)e.value();

                if (val.type().typeId() != typeId) {// TODO backup filter
                    continue;
                }

                List<Object> row = new ArrayList<>(cols.size());

                Object key = e.key().value(null, false);

                row.add(key);

                for (int i = 1; i < cols.size(); i++) { // Key is the first column, so skip it as we added it to the list before.
                    Column col = cols.get(i);

                    row.add(val.field(col.name));
                }

                return row;
            }

            return null;
        }
    }


}
