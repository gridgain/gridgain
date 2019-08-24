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

/**
 * Receiver. Childless operator, like a table scan, but the result source is the another node.
 */
public class ReceiverOp extends PhysicalOperator {

    private int linkId;

    private int responsesCntr;

    private List<List<?>> accumulatedRes = new ArrayList<>();

    public ReceiverOp(int cntr, int linkId) {
        responsesCntr = cntr;
        this.linkId = linkId;
    }

    public void onResult(List<List<?>> res) {
        synchronized (this) {
            accumulatedRes.addAll(res);

            if (--responsesCntr > 0)
                execute(res); // All responses have arrived.
        }
    }

    @Override Iterator<List<?>> iterator(List<List<?>>... input) {
        return input[0].iterator();
    }

    @Override public void init() {
        // No-op
    }

    public int linkId() {
        return linkId;
    }
}
