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

package org.apache.ignite.console.repositories;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.ClusterInfo;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Repository to work with clusters.
 */
@Repository
public class ClusterInfoRepository {
    /** Tx manager. */
    private final TransactionManager txMgr;

    /** Clusters collection. */
    private Table<ClusterInfo> clustersInfoTbl;

    /** Clusters index. */
    private OneToManyIndex<UUID> clustersInfoIdx;

    /**
     * @param ignite Ignite.
     * @param txMgr Tx manager.
     */
    public ClusterInfoRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

        txMgr.registerStarter("clusters_info", () -> {
            clustersInfoTbl = new Table<>(ignite, "wc_clusters_info");

            clustersInfoIdx = new OneToManyIndex<>(ignite, "wc_account_clusters_info_idx",
                (key) -> "TODO: GG-19573");
        });
    }

    /**
     * @param clusterInfo Cluster info.
     */
    public ClusterInfo save(ClusterInfo clusterInfo) {
        return txMgr.doInTransaction(() -> clustersInfoTbl.save(clusterInfo));
    }

    /**
     * @return List of accounts.
     */
    public List<ClusterInfo> list() {
        try {
            return clustersInfoTbl.loadAll();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * @param clusterId Cluster id.
     */
    public ClusterInfo delete(UUID clusterId) {
        return clustersInfoTbl.delete(clusterId);
    }

    /**
     * Remove by ids set.
     */
    public void deleteAll(Set<UUID> clusterIds) {
        clustersInfoTbl.deleteAll(clusterIds);
    }
}
