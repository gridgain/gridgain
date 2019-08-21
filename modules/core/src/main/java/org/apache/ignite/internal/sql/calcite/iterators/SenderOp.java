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
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.sql.calcite.executor.ExecutorOfGovnoAndPalki;
import org.apache.ignite.internal.sql.calcite.plan.SenderNode;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * TODO: Add class description.
 */
public class SenderOp extends PhysicalOperator {

    public SenderOp(PhysicalOperator rowsSrc, SenderNode.SenderType type, int linkId, T2<UUID, Long> qryId,
        ExecutorOfGovnoAndPalki exec) {

        rowsSrc.listen(new IgniteInClosure<IgniteInternalFuture<List<List<?>>>>() {
            @Override public void apply(IgniteInternalFuture<List<List<?>>> fut) {
                try {
                    List<List<?>> res = fut.get();

                    exec.sendResult(res, type, linkId, qryId);
                }
                catch (IgniteCheckedException e) {
                    onDone(e); // TODO send error back to the reducer
                }
            }
        });
    }

    @Override Iterator<List<?>> iterator(List<List<?>>... input) {
        throw new UnsupportedOperationException();
    }
}
