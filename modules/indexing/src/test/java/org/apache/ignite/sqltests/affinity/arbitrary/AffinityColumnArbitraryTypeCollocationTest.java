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

package org.apache.ignite.sqltests.affinity.arbitrary;

import java.util.List;

import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Test;

/**
 * Just a bunch of test to check how affinity field value from binary meta affects data distribution across the cluster.
 * Answer: is doesn't affect it anyhow. GG simply ignores what affinity key for binary type that is written to the cache.
 * Only cache-level settings matters.
 */
public class AffinityColumnArbitraryTypeCollocationTest extends AbstractAffinityColumnArbitraryTypeTest {

    public AffinityColumnArbitraryTypeCollocationTest() {
        super();
        gridCnt = 2;
    }

    // below generators will produce groupId values in a such way that if groupId is affinity key
    // join on foo.groupId = bar.groupId should produce result set which size is exactly half of overall table size
    static final FieldValueGenerator FOO_GENERATOR = (fieldName, id) -> ID_FIELD.equalsIgnoreCase(fieldName) ? id : id % 25;
    static final FieldValueGenerator BAR_GENERATOR = (fieldName, id) -> ID_FIELD.equalsIgnoreCase(fieldName) ? (-1) * id : id % 50;

    protected void initTables() {
        fooTable = table(FOO_TABLE, FOO_CACHE, ID_FIELD, GROUP_ID_FIELD, FOO_GENERATOR)
            .create(backups);
    }

    private Table table(String tableName,
        String cacheName,
        String idField,
        String grouField,
        FieldValueGenerator fvGenerator
    ) {
        return new Table(ignite(0), tableName, cacheName, idField, grouField, getKeyType(), VAL_TYPE, fvGenerator)
            .create(backups);
    }

    static final int COUNT = 1000;

    /**
     * OK
     */
    @Test
    public void testBothTablesDefined() throws Exception {
        // Create cache via CREATE TABLE DDL
        barTable = table(BAR_TABLE, BAR_CACHE, ID_FIELD, GROUP_ID_FIELD, BAR_GENERATOR)
            .create(backups);

        for (int i = 0; i < COUNT; i++) {
            barTable.putBinary(i, ID_FIELD, "groupid");
        }

        for (int i = 0; i < COUNT; i++) {
            fooTable.insert(i);
        }

        String join = "SELECT * FROM " + fooTable +
            " INNER JOIN " + barTable + " ON foo.groupId = bar.groupId" +
            " WHERE foo.factor = bar.factor" +
            " ORDER BY foo.factor";

        List<List<?>> result = defaultCache().query(new SqlFieldsQuery(join)).getAll();
        assertEquals("Data is not collocated", 500, result.size());

        assertBinaryMeta();
    }

    /**
     * Fails, but it's fine because 'puts' executed against cache without explicit affinity key
     */
    @Test
    public void testPutOnRawCacheFirst() throws Exception {
        // Create cache without table definition
        ignite(0).getOrCreateCache(new CacheConfiguration<>(BAR_CACHE)
            .setBackups(backups)
            .setKeyConfiguration(new CacheKeyConfiguration()
                    .setTypeName(getKeyType())
                    .setAffinityKeyFieldName(GROUP_ID_FIELD))
            .setCacheMode(CacheMode.PARTITIONED));

        barTable = table(BAR_TABLE, BAR_CACHE, ID_FIELD, GROUP_ID_FIELD, BAR_GENERATOR);

        for (int i = 0; i < COUNT; i++) {
            barTable.putBinary(i, ID_FIELD, "groupid");
        }

        for (int i = 0; i < COUNT; i++) {
            fooTable.insert(i);
        }

        // Create BAR table after data was populated
        barTable.create(backups);

        String join = "SELECT * FROM " + fooTable +
            " INNER JOIN " + barTable + " ON foo.groupId = bar.groupId" +
            " WHERE foo.factor = bar.factor" +
            " ORDER BY foo.factor";

        List<List<?>> result = defaultCache().query(new SqlFieldsQuery(join)).getAll();
        assertEquals("Data is not collocated", 500, result.size());

        assertBinaryMeta();
    }

}
