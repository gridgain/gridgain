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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.processors.cache.persistence.pagemem.TestCheckpointUtils.fullPageId;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/** For {@link CheckpointPageReplacement} testing. */
public class CheckpointPageReplacementTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testBlock() {
        CheckpointPageReplacement pageReplacement = new CheckpointPageReplacement();

        pageReplacement.block(fullPageId(0, 0));
        pageReplacement.block(fullPageId(0, 1));

        pageReplacement.stopBlocking();
    }

    /** */
    @Test
    public void testUnblock() {
        CheckpointPageReplacement pageReplacement = new CheckpointPageReplacement();

        pageReplacement.block(fullPageId(0, 0));
        pageReplacement.block(fullPageId(0, 1));
        pageReplacement.block(fullPageId(0, 2));
        pageReplacement.block(fullPageId(0, 3));
        pageReplacement.block(fullPageId(0, 4));
        pageReplacement.block(fullPageId(0, 5));

        pageReplacement.unblock(fullPageId(0, 0), null);
        pageReplacement.unblock(fullPageId(0, 1), new Throwable("from test 0"));
        pageReplacement.unblock(fullPageId(0, 2), new Throwable("from test 1"));

        pageReplacement.stopBlocking();

        pageReplacement.unblock(fullPageId(0, 3), null);
        pageReplacement.unblock(fullPageId(0, 4), new Throwable("from test 0"));
        pageReplacement.unblock(fullPageId(0, 5), new Throwable("from test 1"));
    }

    /** */
    @Test
    public void testStopBlocking() {
        CheckpointPageReplacement pageReplacement = new CheckpointPageReplacement();

        pageReplacement.block(fullPageId(0, 0));
        pageReplacement.block(fullPageId(0, 1));

        CompletableFuture<Void> stopBlockingFut = pageReplacement.stopBlocking();
        assertFalse(stopBlockingFut.isDone());

        pageReplacement.unblock(fullPageId(0, 0), null);
        assertFalse(stopBlockingFut.isDone());

        pageReplacement.unblock(fullPageId(0, 1), null);
        assertTrue(stopBlockingFut.isDone());

        assertTrue(pageReplacement.stopBlocking().isDone());
    }

    /** */
    @Test
    public void testStopBlockingError() {
        CheckpointPageReplacement pageReplacement = new CheckpointPageReplacement();

        pageReplacement.block(fullPageId(0, 0));
        pageReplacement.block(fullPageId(0, 1));

        CompletableFuture<Void> stopBlockingFut = pageReplacement.stopBlocking();
        assertFalse(stopBlockingFut.isDone());

        pageReplacement.unblock(fullPageId(0, 0), new RuntimeException("from test 1"));
        assertThrowsAnyCause(log, () -> stopBlockingFut.get(1, SECONDS), RuntimeException.class, "from test 1");

        pageReplacement.unblock(fullPageId(0, 1), new TimeoutException("from test 2"));
        assertThrowsAnyCause(log, () -> stopBlockingFut.get(1, SECONDS), RuntimeException.class, "from test 1");

        assertThrowsAnyCause(
            log,
            () -> pageReplacement.stopBlocking().get(1, SECONDS),
            RuntimeException.class,
            "from test 1"
        );
    }

    /** */
    @Test
    public void testStopBlockingNoPageReplacement() {
        assertTrue(new CheckpointPageReplacement().stopBlocking().isDone());
    }
}
