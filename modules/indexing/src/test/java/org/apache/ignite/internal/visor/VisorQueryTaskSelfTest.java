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

package org.apache.ignite.internal.visor;

import java.util.UUID;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.visor.query.VisorQueryFetchFirstPageTask;
import org.apache.ignite.internal.visor.query.VisorQueryNextPageTaskArg;
import org.apache.ignite.internal.visor.query.VisorQueryResult;
import org.apache.ignite.internal.visor.query.VisorQueryTask;
import org.apache.ignite.internal.visor.query.VisorQueryTaskArg;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for {@link org.apache.ignite.internal.visor.query.VisorQueryTask}.
 */
public class VisorQueryTaskSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(new TestLogger());

        return cfg;
    }

    /**
     * @param ignite Ignite.
     * @param nid Node ID.
     * @return Task result.
     */
    private VisorEither<VisorQueryResult> executeTask(IgniteEx ignite, UUID nid) {
        VisorQueryTaskArg args = new VisorQueryTaskArg(null, "create table test", false, false, false, false, 10);

        return ignite
            .compute()
            .execute(VisorQueryTask.class,
                new VisorTaskArgument<>(nid, args, false));
    }

    /**
     * This test executes query via VisorQueryTask with enabled debug mode on logger.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testLogDebug() throws Exception {
        IgniteEx ignite = startGrids(1);
        ignite.cluster().active(true);

        UUID nid = ignite.localNode().id();

        VisorEither<VisorQueryResult> taskRes = ignite
            .compute()
            .execute(VisorQueryTask.class, new VisorTaskArgument<>(
                nid,
                new VisorQueryTaskArg(null, "CREATE TABLE TEST(K INTEGER PRIMARY KEY, V INTEGER)", false, false, false, false, 10),
                false
            ));

        assertNotNull(taskRes);
        assertNotNull(taskRes.getResult());

        taskRes = ignite.compute().execute(VisorQueryFetchFirstPageTask.class, new VisorTaskArgument<>(
            nid,
            new VisorQueryNextPageTaskArg(taskRes.getResult().getQueryId(), 10),
            false
        ));

        assertNotNull(taskRes);
        assertNull(taskRes.getError());
    }

    /**
     * Test logger with enabled debug mode.
     */
    private static class TestLogger extends NullLogger {
        /** {@inheritDoc} */
        @Override public boolean isDebugEnabled() {
            return true;
        }
    }
}
