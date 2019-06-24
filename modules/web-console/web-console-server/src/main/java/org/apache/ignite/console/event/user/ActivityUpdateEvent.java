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

package org.apache.ignite.console.event.user;

import org.springframework.context.ApplicationEvent;

import java.util.UUID;

/**
 *
 */
public class ActivityUpdateEvent extends ApplicationEvent {
    /** */
    private UUID accId;

    /** */
    private String grp;

    /**  */
    private String act;

    /**
     * @param accId Acc id.
     * @param grp Group.
     * @param act Action.
     */
    public ActivityUpdateEvent(UUID accId, String grp, String act) {
        super(new Object());

        this.accId = accId;
        this.grp = grp;
        this.act = act;
    }

    /**
     * @return Acc id.
     */
    public UUID accId() {
        return accId;
    }

    /**
     * @return Group.
     */
    public String group() {
        return grp;
    }

    /**
     * @return Action.
     */
    public String action() {
        return act;
    }
}
