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

package org.apache.ignite.console.migration;

import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.dto.Activity;
import org.apache.ignite.console.dto.ActivityKey;
import org.apache.ignite.console.repositories.ActivitiesRepository;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.transactions.Transaction;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

/**
 * Repository to work with activities during migration.
 */
@Repository
@Qualifier("migration")
public class MigrateActivitiesRepository extends ActivitiesRepository {
    /**
     * @param ignite Ignite.
     * @param txMgr Transactions manager.
     */
    public MigrateActivitiesRepository(Ignite ignite, TransactionManager txMgr) {
        super(ignite, txMgr);
    }

    /**
     * @param activityKey Activity key.
     * @param activity Activity to save.
     */
    public void migrateActivity(ActivityKey activityKey, Activity activity) {
        try (Transaction tx = txMgr.txStart()) {
            Set<UUID> ids = activitiesIdx.load(activityKey);

            activitiesTbl.save(activity);

            ids.add(activity.getId());

            activitiesIdx.addAll(activityKey, ids);

            tx.commit();
        }
    }
}
