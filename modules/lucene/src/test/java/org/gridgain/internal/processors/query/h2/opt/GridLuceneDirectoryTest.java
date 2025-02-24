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

package org.gridgain.internal.processors.query.h2.opt;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.testframework.junits.JUnitAssertAware;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.Before;
import org.junit.Test;

public class GridLuceneDirectoryTest extends GridCommonAbstractTest {
    private GridLuceneDirectory dir;

    @Before
    public void setUp() throws Exception {
        dir = new GridLuceneDirectory(new GridUnsafeMemory(0));
    }

    @Test
    public void testPendingDeletion() throws IOException {
        final String fileName = "foo";

        assertNoFilenames();
        IndexOutput indexOutput = dir.createOutput(fileName, IOContext.DEFAULT);
        JUnitAssertAware.assertTrue("Missing created file: " + fileName, dirFileNames().contains(fileName));

        dir.deleteFile(fileName);
        assertNoFilenames();
        JUnitAssertAware.assertTrue(dir.getPendingDeletions().contains(fileName));

        // releasing pending file ref by closing IndexOutput
        indexOutput.close();
        assertNoFilenames();
        assertNoPendingDeletions();
    }

    private List<String> dirFileNames() {
        return Arrays.asList(dir.listAll());
    }

    private void assertNoFilenames() {
        JUnitAssertAware.assertTrue("Lucene Directory is not empty", dirFileNames().isEmpty());
    }

    private void assertNoPendingDeletions() throws IOException {
        JUnitAssertAware.assertTrue("Pending deletions is not empty", dir.getPendingDeletions().isEmpty());
    }
}
