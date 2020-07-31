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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.SharedPageLockTracker;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.SharedPageLockTrackerDump;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static java.nio.file.Paths.get;

/**
 * Unit tests for {@link ToStringDumpHelper}.
 */
public class ToStringDumpHelperTest extends GridCommonAbstractTest {

    /** */
    @Test
    public void toStringSharedPageLockTrackerTest() throws Exception {
        SharedPageLockTracker pageLockTracker = new SharedPageLockTracker();

        pageLockTracker.onReadLock(1, 2, 3, 4);

        Thread asyncLockUnlock = new Thread(() -> {
            pageLockTracker.onReadLock(4, 32, 1, 64);

            pageLockTracker.onReadUnlock(4, 32, 1, 64);
        }, "async-lock-unlock");
        asyncLockUnlock.start();
        asyncLockUnlock.join();

        SharedPageLockTrackerDump pageLockDump = pageLockTracker.dump();

        assertNotNull(pageLockDump);

        String dumpStr = ToStringDumpHelper.toStringDump(pageLockDump);

        System.out.println("Dump saved:" + dumpStr);

        assertFalse(dumpStr.contains("Thread=[name=async-lock-unlock"));

        assertTrue(dumpStr.contains("Locked pages = [2[0000000000000002](r=1|w=0)]"));
    }
}