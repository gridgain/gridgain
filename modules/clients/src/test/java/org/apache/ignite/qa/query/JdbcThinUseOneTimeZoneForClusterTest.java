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

package org.apache.ignite.qa.query;

import java.util.List;
import org.apache.ignite.internal.processors.query.UseOneTimeZoneForClusterTest;
import org.apache.ignite.jdbc.JdbcTestUtils;

/**
 */
public class JdbcThinUseOneTimeZoneForClusterTest extends UseOneTimeZoneForClusterTest {
    /** Jdbc thin url. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** {@inheritDoc} */
    @Override protected List<List<?>> sql(String sql, List<Object> params) throws Exception {
        return JdbcTestUtils.sql(URL, sql, params);
    }
}
