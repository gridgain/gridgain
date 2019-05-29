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

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.CacheHolder;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Activity;
import org.apache.ignite.console.dto.ActivityKey;
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
    private final CacheHolder<ActivityKey, Set<UUID>> activitiesIdx;

    /**
     * @param ignite Ignite.
     * @param txMgr Transactions manager.
     */
    public ActivitiesRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

        activitiesTbl = new Table<>(ignite, "wc_activities");

        activitiesIdx = new CacheHolder<>(ignite, "wc_account_activities_idx");
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
            // Activity period is the current year and month.
            long date = LocalDate.now().atStartOfDay(UTC).withDayOfMonth(1).toInstant().toEpochMilli();

            ActivityKey activityKey = new ActivityKey(accId, date);

            Set<UUID> ids = activitiesIdx.cache().get(activityKey);

            if (ids == null)
                ids = Collections.emptySet();

            Collection<Activity> activities = activitiesTbl.loadAll(ids);

            Activity activity = activities
                .stream()
                .filter(item -> item.getGroup().equals(grp) && item.getAction().equals(act))
                .findFirst()
                .orElse(new Activity(UUID.randomUUID(), grp, act, 0));

            activity.increment(1);

            activitiesTbl.save(activity);

            ids.add(activity.getId());

            activitiesIdx.cache().put(activityKey, ids);

            tx.commit();
        }
    }

    /**
     * @param accId Account ID.
     * @param startDate Start date.
     * @param endDate End date.
     */
    public Collection<Activity> activitiesForPeriod(UUID accId, long startDate, long endDate) {
        try (Transaction ignored = txMgr.txStart()) {
            ZonedDateTime dtStart = Instant.ofEpochMilli(startDate).atZone(UTC);
            ZonedDateTime dtEnd = Instant.ofEpochMilli(endDate).atZone(UTC);

            Map<String, Activity> totals = new HashMap<>();

            while (dtStart.isBefore(dtEnd)) {
                ActivityKey key = new ActivityKey(accId, dtStart.toInstant().toEpochMilli());

                Set<UUID> ids = activitiesIdx.cache().get(key);

                if (ids != null) {
                    Collection<Activity> activities = activitiesTbl.loadAll(ids);

                    activities.forEach(activity -> {
                        Activity total = totals.get(activity.getAction());

                        if (total != null)
                            total.increment(activity.getAmount());
                        else
                            totals.put(activity.getAction(), activity);
                    });
                }

                dtStart = dtStart.plusMonths(1);
            }

            return totals.values();
        }
    }
}
