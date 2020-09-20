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

package org.apache.ignite.compatibility.sql.runner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Simple query supplier that returns preconfigured queries one by one.
 */
public class PredefinedQueriesSupplier implements Supplier<QueryWithParams> {
    /** */
    private final Collection<QueryWithParams> qrys;

    /** */
    private final boolean runOnce;

    /** */
    private Iterator<QueryWithParams> it;

    /**
     * @param qrys Collection of the queries this supplier would return.
     * @param runOnce Flag indicates whether supplier should iterates through
     * provided collection only once or it should iterates on manner of cyclic
     * buffer.
     */
    public PredefinedQueriesSupplier(Collection<QueryWithParams> qrys, boolean runOnce) {
        A.notEmpty(qrys, "qrys");

        this.qrys = new ArrayList<>(qrys);
        this.runOnce = runOnce;

        it = qrys.iterator();
    }

    /** {@inheritDoc} */
    @Override public synchronized QueryWithParams get() {
        if (!it.hasNext()) {
            if (runOnce)
                return null;
            else
                it = qrys.iterator();
        }

        return it.next();
    }
}
