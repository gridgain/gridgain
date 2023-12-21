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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.lang.IgniteBiTuple;

public class WalRecordCyclicBufferByThread {
    private final Map<Thread, WalRecordCyclicBuffer> bufferByThread = new ConcurrentHashMap<>();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    private final int bufferSize;

    public WalRecordCyclicBufferByThread(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void add(WALRecord rec) {
        if (!busyLock.enterBusy())
            return;

        try {
            bufferByThread.computeIfAbsent(Thread.currentThread(), thread -> new WalRecordCyclicBuffer(bufferSize)).add(rec);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    public void stop() {
        if (!stopGuard.compareAndSet(false, true))
            return;

        busyLock.block();
    }

    public List<IgniteBiTuple<Thread, WALRecord>> getSortedListWalRecords(long absSegIdx) {
        List<IgniteBiTuple<Thread, WALRecord>> res = new ArrayList<>();

        for (Map.Entry<Thread, WalRecordCyclicBuffer> entry : bufferByThread.entrySet()) {
            for (WALRecord rec : entry.getValue().getSortedList(absSegIdx)) {
                res.add(new IgniteBiTuple<>(entry.getKey(), rec));
            }
        }

        res.sort(Comparator.comparingInt(value -> ((FileWALPointer)value.get2().position()).fileOffset()));

        return res;
    }
}
