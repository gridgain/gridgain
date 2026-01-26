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

package org.apache.ignite.events;

import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CONTROL_CENTER_ACTION_EVT;

/**
 * Event type indicating that the management action is started.
 *
 * @see EventType#EVT_CONTROL_CENTER_ACTION_EVT
 */
public class ControlCenterActionStartedEvent extends EventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final String actionName;

    /** */
    private final String actionClassName;

    /** Subject ID. */
    @Nullable
    private final UUID subjId;

    /** Username in Control Center. */
    @Nullable
    private final String ccUsername;


    /** {@inheritDoc} */
    @Override public String shortDisplay() {
        return name() + ": actionName=" + actionName;
    }

    /**
     * Creates action event with given parameters.
     *
     * @param node Node.
     * @param actionName Action name.
     * @param actionClassName Action class name.
     * @param subjId Security subject ID.
     * @param ccUsername Username in Control Center.
     */
    public ControlCenterActionStartedEvent(ClusterNode node, String actionName, String actionClassName, @Nullable UUID subjId, @Nullable String ccUsername) {
        super(node, null, EVT_CONTROL_CENTER_ACTION_EVT);

        this.actionName = actionName;
        this.actionClassName = actionClassName;

        this.subjId = subjId;
        this.ccUsername = ccUsername;
    }

    /**
     * Gets name of the action that triggered the event.
     *
     * @return Name of the action that triggered the event.
     */
    public String actionName() {
        return actionName;
    }

    /**
     * Gets name of action class that triggered this event.
     *
     * @return Name of the action class that triggered the event.
     */
    public String actionClassName() {
        return actionClassName;
    }

    /**
     * Gets security subject ID.
     *
     * @return Security subject ID.
     */
    @Nullable
    public UUID subjectId() {
        return subjId;
    }

    /**
     * Gets username in Control Center.
     *
     * @return Username in Control Center.
     */
    @Nullable
    public String ccUsername() {
        return ccUsername;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ControlCenterActionStartedEvent.class, this,
            "nodeId8", U.id8(node().id()),
            "type", name(),
            "tstamp", timestamp());
    }
}