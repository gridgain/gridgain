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

package org.apache.ignite.internal.processors.cache.encryption;

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.encryption.EncryptedCacheRestartTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/** */
public class EncryptedSqlTemplateTableTest extends EncryptedCacheRestartTest {
    /** {@inheritDoc} */
    @Override protected void createEncryptedCache(IgniteEx grid0, @Nullable IgniteEx grid1, String cacheName,
        String cacheGroup, boolean putData) {
        CacheConfiguration templateConfiguration = new CacheConfiguration()
                .setName("ENCRYPTED_TEMPLATE")
                .setEncryptionEnabled(true);

        grid0.addCacheConfiguration(templateConfiguration);

        executeSql(grid0, "CREATE TABLE encrypted(ID BIGINT, NAME VARCHAR(10), PRIMARY KEY (ID)) " +
                "WITH \"TEMPLATE=ENCRYPTED_TEMPLATE\"");
        executeSql(grid0, "CREATE INDEX enc0 ON encrypted(NAME)");

        if (putData) {
            for (int i = 0; i < 100; i++)
                executeSql(grid0, "INSERT INTO encrypted(ID, NAME) VALUES(?, ?)", i, "" + i);
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkData(IgniteEx grid0) {
        IgniteCache cache = grid0.cache(cacheName());
        CacheConfiguration cacheConfiguration = (CacheConfiguration) cache.getConfiguration(CacheConfiguration.class);
        assertTrue(cacheConfiguration.isEncryptionEnabled());

        for (int i = 0; i < 100; i++) {
            List<List<?>> res = executeSql(grid0, "SELECT NAME FROM encrypted WHERE ID = ?", i);

            assertEquals(1, res.size());
            assertEquals("" + i, res.get(0).get(0));
        }
    }

    /** */
    private List<List<?>> executeSql(IgniteEx grid, String qry, Object...args) {
        return grid.context().query().querySqlFields(
                new SqlFieldsQuery(qry).setSchema("PUBLIC").setArgs(args), true).getAll();
    }

    /** {@inheritDoc} */
    @Override protected String cacheName() {
        return "SQL_PUBLIC_ENCRYPTED";
    }

    /** {@inheritDoc} */
    @Override protected String keystorePath() {
        return IgniteUtils.resolveIgnitePath("modules/indexing/src/test/resources/tde.jks").getAbsolutePath();
    }
}
