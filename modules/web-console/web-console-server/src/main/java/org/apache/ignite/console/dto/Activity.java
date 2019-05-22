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

package org.apache.ignite.console.dto;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Activity info.
 */
public class Activity extends AbstractDto {
    /** */
    private UUID owner;

    /** */
    private long date;

    /** */
    private String grp;

    /** */
    private String act;

    /** */
    private int amount;

    /**
     * Default constructor for serialization.
     */
    public Activity() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param id Activity ID.
     * @param owner Owner ID.
     * @param date Activity period (year and month).
     * @param grp Group.
     * @param act Activity.
     * @param amount Number of times activity was executed in current period..
     */
    public Activity(UUID id, UUID owner, long date, String grp, String act, int amount) {
        super(id);

        this.owner = owner;
        this.date = date;
        this.grp = grp;
        this.act = act;
        this.amount = amount;
    }

    /**
     * @return Owner.
     */
    public UUID getOwner() {
        return owner;
    }

    /**
     * @param owner Owner.
     */
    public void setOwner(UUID owner) {
        this.owner = owner;
    }

    /**
     * @return Activity period.
     */
    public long getDate() {
        return date;
    }

    /**
     * @param date Activity period.
     */
    public void setDate(long date) {
        this.date = date;
    }

    /**
     * @return Activity group.
     */
    public String getGroup() {
        return grp;
    }

    /**
     * @param grp Activity group.
     */
    public void setGroup(String grp) {
        this.grp = grp;
    }

    /**
     * @return Activity action.
     */
    public String getAction() {
        return act;
    }

    /**
     * @param act Activity action.
     */
    public void setAction(String act) {
        this.act = act;
    }

    /**
     * @return Number of times activity was executed in current period.
     */
    public int getAmount() {
        return amount;
    }

    /**
     * @param amount Number of times activity was executed in current period.
     */
    public void setAmount(int amount) {
        this.amount = amount;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Activity.class, this);
    }

    /**
     * Increment number of activity usages in current period.
     */
    public void increment() {
        amount++;
    }
}
