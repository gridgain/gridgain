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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.h2.twostep.GridMapQueryExecutor;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

public class GridMapQueryExecutorTest extends BaseSqlTest {
    private IgniteLogger log;
    private GridMapQueryExecutor executor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        log = mock(IgniteLogger.class);
        executor = new GridMapQueryExecutor();
        executor.setLogger(log);  // A setLogger method is defined in GridMapQueryExecutor
    }

    /**
     * Tests that error logging behaves as expected during an error condition.
     */
    @Test
    public void testQueryExecutionFailure() {
        String failingSql = "SELECT * FROM non_existent_table;";
        doThrow(new SQLException("Table not found")).when(executor).executeSql(failingSql);

        executor.executeQuery(failingSql);

        verify(log).error(contains("Failed to execute query: " + failingSql), any(SQLException.class));
}
}
