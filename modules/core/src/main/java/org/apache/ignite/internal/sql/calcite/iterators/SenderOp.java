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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.sql.calcite.executor.ExecutorOfGovnoAndPalki;
import org.apache.ignite.internal.sql.calcite.plan.SenderNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.internal.sql.calcite.plan.SenderNode.SenderType.HASH;
import static org.apache.ignite.internal.sql.calcite.plan.SenderNode.SenderType.SINGLE;

/**
 * TODO: Add class description.
 */
public class SenderOp extends PhysicalOperator {
    private final PhysicalOperator rowsSrc;
    private final SenderNode.SenderType type;
    private final int linkId;
    private final T2<UUID, Long> qryId;
    private final List<Integer> distKeys;
    private final ExecutorOfGovnoAndPalki exec;


    public SenderOp(PhysicalOperator rowsSrc, SenderNode.SenderType type, int linkId, T2<UUID, Long> qryId,
        List<Integer> distKeys, ExecutorOfGovnoAndPalki exec) {
        this.rowsSrc = rowsSrc;
        this.type = type;
        this.linkId = linkId;
        this.qryId = qryId;
        this.distKeys = distKeys;
        this.exec = exec;

    }

    @Override Iterator<List<?>> iterator(List<List<?>>... input) {
        throw new UnsupportedOperationException();
    }

    @Override public void init() {
        rowsSrc.listen(new IgniteInClosure<IgniteInternalFuture<List<List<?>>>>() {
            @Override public void apply(IgniteInternalFuture<List<List<?>>> fut) {
                try {
                    List<List<?>> rows = fut.get();

                    if (type == SINGLE)
                        exec.sendResult(rows, linkId, qryId, qryId.getKey());
                    else if (type == HASH) {
                        assert !F.isEmpty(distKeys) && distKeys.size() == 1; // Only one key distribution supported at the moment

                        AffinityTopologyVersion curTopVer = exec.firstUserCache().affinity().affinityTopologyVersion();

                        Map<UUID, List<List<?>>> mapping = new HashMap<>();

                        for (ClusterNode node : exec.context().discovery().aliveServerNodes())
                            mapping.put(node.id(), new ArrayList<>());

                        Integer hashKey = distKeys.get(0);

                        for (List<?> row : rows) {
                            ClusterNode node =  exec.firstUserCache().affinity().nodesByKey(row.get(hashKey), curTopVer).get(0);

                            List<List<?>> mappedRows = mapping.computeIfAbsent(node.id(), k -> new ArrayList<>());

                            mappedRows.add(row);
                        }

                        System.out.println("mapping=" + mapping);

                        for (Map.Entry<UUID, List<List<?>>> e : mapping.entrySet())
                            exec.sendResult(e.getValue(), linkId, qryId, e.getKey());
                    }
                    else
                        throw new UnsupportedOperationException("unsupported yet");
                }
                catch (Exception e) {
                    System.out.println("Sender error=" + X.getFullStackTrace(e));
                    onDone(e); // TODO send error back to the reducer
                }
            }
        });

        rowsSrc.init();
    }
}
