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
package org.apache.ignite.internal.sql.calcite.physical;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.sql.calcite.Column;
import org.apache.ignite.internal.sql.calcite.IgniteTable;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * TODO: Add class description.
 */
public class TableScan extends PhysicalOperator {

    final IgniteTable tbl;

    final IgniteInternalCache cache;

    public TableScan(IgniteTable tbl, IgniteInternalCache cache) {
        this.tbl = tbl;
        this.cache = cache;
    }

    @Override public Iterator<List<?>> iterator() {
        try {
            Iterator<Cache.Entry<Object, BinaryObject>> it = cache.scanIterator(true, null);

            return new TableScanIterator(it, tbl.columns()); // TODO: CODE: implement.
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    private static class TableScanIterator implements Iterator<List<?>>{

        private final Iterator<Cache.Entry<Object, BinaryObject>> it;

        final List<Column> cols;

        private TableScanIterator(Iterator<Cache.Entry<Object, BinaryObject>> it, List<Column> cols) {
            this.it = it;
            this.cols = cols;
        }

        @Override public boolean hasNext() {
            return it.hasNext();
        }

        @Override public List<?> next() {
            Cache.Entry<Object, BinaryObject> e = it.next();

            Object key = e.getKey();
            BinaryObject val = e.getValue();

            List<Object> row = new ArrayList<>(cols.size());

            row.add(key);

            for (int i = 1; i < cols.size(); i++) { // Key is the first column, so skip it as we added it to the list before.
                Column col = cols.get(i);

                row.add(val.field(col.name));
            }

            return row;
        }
    }


}
