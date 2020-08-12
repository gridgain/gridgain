/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToStringDumpHelper;

/**
 *
 */
public class SharedPageLockTrackerDump {

    /** */
    public final Map<Integer, String> structureIdToStrcutureName;

    /** */
    public final List<ThreadPageLockState> threadPageLockStates;

    /** */
    public final long time;

    /** */
    public SharedPageLockTrackerDump(
        long time,
        Map<Integer, String> structureIdToStrcutureName,
        List<ThreadPageLockState> threadPageLockStates
    ) {
        this.time = time;
        this.structureIdToStrcutureName = structureIdToStrcutureName;
        this.threadPageLockStates = threadPageLockStates;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return ToStringDumpHelper.toStringDump(this);
    }
}
