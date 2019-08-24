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

import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Query tree output.
 */
public class OutputOp extends PhysicalOperator {

    PhysicalOperator rowsSrc;

    public OutputOp(PhysicalOperator rowsSrc) {
        this.rowsSrc = rowsSrc;
    }

    @Override Iterator<List<?>> iterator(List<List<?>>... input) {
        throw new UnsupportedOperationException();
    }

    @Override public void init() {
        rowsSrc.listen(new IgniteInClosure<IgniteInternalFuture<List<List<?>>>>() {
            @Override public void apply(IgniteInternalFuture<List<List<?>>> future) {
                try {
                    OutputOp.this.onDone(rowsSrc.get());
                }
                catch (IgniteCheckedException e) {
                    onDone(e);
                }
            }
        });

        rowsSrc.init();
    }
}
