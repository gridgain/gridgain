/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.mxbean;

import org.apache.ignite.spi.systemview.view.ContinuousQueryView;
import org.apache.ignite.spi.systemview.view.SqlQueryView;

/**
 * Query MXBean interface.
 */
public interface QueryMXBean {
    /**
     * Kills continuous query by the identifier.
     *
     * @param routineId Continuous query id.
     * @see ContinuousQueryView#routineId()
     */
    @MXBeanDescription("Kills continuous query by the identifier.")
    void cancelContinuous(
        @MXBeanParameter(name = "originNodeId", description = "Originating node ID.") String originNodeId,
        @MXBeanParameter(name = "routineId", description = "Continuous query id.") String routineId
    );

    /**
     * Kills SQL query by the identifier.
     *
     * @param id SQL query id.
     * @see SqlQueryView#queryId()
     */
    @MXBeanDescription("Kills SQL query by the identifier.")
    void cancelSQL(
        @MXBeanParameter(name = "id", description = "SQL query id.") String id
    );

}
