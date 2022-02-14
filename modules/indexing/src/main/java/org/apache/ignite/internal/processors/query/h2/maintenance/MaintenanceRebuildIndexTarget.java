/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query.h2.maintenance;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.maintenance.MaintenanceTask;

/**
 * {@link RebuildIndexAction}'s parameters.
 */
public class MaintenanceRebuildIndexTarget {
    /** Separator for index rebuild maintenance task parameters. */
    public static final String INDEX_REBUILD_PARAMETER_SEPARATOR = "/";

    /** Cache id. */
    private final int cacheId;

    /** Name of the index. */
    private final String idxName;

    /**
     * Constructor.
     *
     * @param id Cache id.
     * @param name Name of the index.
     */
    public MaintenanceRebuildIndexTarget(int id, String name) {
        cacheId = id;
        idxName = name;
    }

    /**
     * @return Cache id.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @return Name of the index.
     */
    public String idxName() {
        return idxName;
    }

    /**
     * Parses {@link MaintenanceTask#parameters()} to a list of a MaintenanceRebuildIndexTargets.
     *
     * @param parameters Task's parameters.
     * @return List of MaintenanceRebuildIndexTargets.
     */
    public static List<MaintenanceRebuildIndexTarget> parseMaintenanceTaskParameters(String parameters) {
        String[] parametersArray = parameters.split(INDEX_REBUILD_PARAMETER_SEPARATOR);

        assert (parametersArray.length % 2) == 0;

        List<MaintenanceRebuildIndexTarget> params = new ArrayList<>(parametersArray.length / 2);

        for (int i = 0; i < parametersArray.length; i += 2)
            params.add(new MaintenanceRebuildIndexTarget(Integer.parseInt(parametersArray[i]), parametersArray[i + 1]));

        return params;
    }
}
