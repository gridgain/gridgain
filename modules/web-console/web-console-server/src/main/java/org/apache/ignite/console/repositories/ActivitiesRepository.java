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

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Activity;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.console.web.model.ActivityRequest;
import org.apache.ignite.transactions.Transaction;
import org.springframework.stereotype.Repository;

/**
 * Repository to work with activities.
 */
@Repository
public class ActivitiesRepository {
    /** */
    private final TransactionManager txMgr;

    /** */
    private final Table<Activity> activitiesTbl;

    /** */
    private final OneToManyIndex activitiesIdx;

    /**
     * @param ignite Ignite.
     * @param txMgr Transactions manager.
     */
    public ActivitiesRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

        activitiesTbl = new Table<>(ignite, "wc_activities");

        activitiesIdx = new OneToManyIndex(ignite, "wc_account_activities_idx");
    }

    /**
     * Save notebook.
     *
     * @param accId Account ID.
     * @param req Activity update request.
     */
    public void save(UUID accId, ActivityRequest req) {
        try (Transaction tx = txMgr.txStart()) {
//            activitiesIdx.validateSave(accId, notebook.getId(), activitiesTbl);
//
//            activitiesTbl.save(notebook);
//
//            activitiesIdx.add(accId, notebook.getId());

            tx.commit();
        }
    }
}
