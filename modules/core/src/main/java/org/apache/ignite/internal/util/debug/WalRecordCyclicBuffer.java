/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.util.debug;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;

public class WalRecordCyclicBuffer extends ConcurrentCyclicBuffer<WALRecord> {
    public WalRecordCyclicBuffer(int size) {
        super(size);
    }

    public List<WALRecord> getSortedList(long absSegIdx) {
        return stream()
            .filter(walRecord -> ((FileWALPointer)walRecord.position()).index() == absSegIdx)
            .sorted(Comparator.comparingInt(walRecord -> ((FileWALPointer)walRecord.position()).fileOffset()))
            .collect(Collectors.toList());
    }
}
