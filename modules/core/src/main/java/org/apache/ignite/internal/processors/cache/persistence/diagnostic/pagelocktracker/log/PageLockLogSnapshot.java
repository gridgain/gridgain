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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log;

import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockDump;

/**
 * Page lock log snapshot.
 */
public class PageLockLogSnapshot extends PageLockDump {
    /** Page lock log name. */
    public final String name;

    /** Head position. */
    public final int headIdx;

    /** List of log entries. */
    public final List<LogEntry> locklog;

    /** Next operation. */
    public final int nextOp;

    /** Next data structure. */
    public final int nextOpStructureId;

    /** Next page id. */
    public final long nextOpPageId;

    /**
     *
     */
    public PageLockLogSnapshot(
        String name,
        long time,
        int headIdx,
        List<LogEntry> locklog,
        int nextOp,
        int nextOpStructureId,
        long nextOpPageId
    ) {
        super(time);
        this.name = name;
        this.headIdx = headIdx;
        this.locklog = locklog;
        this.nextOp = nextOp;
        this.nextOpStructureId = nextOpStructureId;
        this.nextOpPageId = nextOpPageId;
    }
}
