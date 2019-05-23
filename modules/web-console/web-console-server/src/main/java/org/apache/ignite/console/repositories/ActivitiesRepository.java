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

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Activity;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.transactions.Transaction;
import org.springframework.stereotype.Repository;

import static java.time.ZoneOffset.UTC;

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
     * Save activity.
     *
     * @param accId Account ID.
     * @param grp Activity group.
     * @param act Activity action.
     */
    public void save(UUID accId, String grp, String act) {
        try (Transaction tx = txMgr.txStart()) {
            TreeSet<UUID> ids = activitiesIdx.load(accId);

            Collection<Activity> activities = activitiesTbl.loadAll(ids);

            // Activity period is the current year and month.
            long date = LocalDate.now().atStartOfDay(UTC).withDayOfMonth(1).toInstant().toEpochMilli();

            Activity activity = activities
                .stream()
                .filter(item ->
                    item.getDate() == date &&
                    item.getGroup().equals(grp) &&
                    item.getAction().equals(act)
                )
                .findFirst()
                .orElse(new Activity(
                    UUID.randomUUID(),
                    accId,
                    date,
                    grp,
                    act,
                    0));

            activity.increment();

            activitiesTbl.save(activity);

            activitiesIdx.add(accId, activity.getId());

            tx.commit();
        }
    }

    /**
     * @param accId Account ID.
     * @param startDate Start date.
     * @param endDate End date.
     */
    public Collection<Activity> details(UUID accId, long startDate, long endDate) {
        return new ArrayList<>();
    }
}
