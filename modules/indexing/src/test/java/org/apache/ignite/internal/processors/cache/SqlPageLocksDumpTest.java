/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class SqlPageLocksDumpTest extends GridCommonAbstractTest {
    /** */
    private static final String PERSON_CACHE = "person";

    /** */
    private ListeningTestLogger testLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        QueryEntity entity = new QueryEntity();
        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Person.class.getName());
        entity.addQueryField("id", Integer.class.getName(), null);
        entity.addQueryField("name", String.class.getName(), null);
        entity.setKeyFieldName("id");

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(PERSON_CACHE);
        ccfg.setQueryEntities(F.asList(entity));

        cfg.setCacheConfiguration(ccfg);

        testLog = new ListeningTestLogger(log);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_PAGE_LOCK_TRACKER_CHECK_INTERVAL, value = "1000")
    public void queryTouchingManyPagesShouldNotLogPageLocksDump() throws Exception {
        Ignite ignite = startGrid(0);

        try (IgniteDataStreamer<Integer, Person> streamer = ignite.dataStreamer(PERSON_CACHE)) {
            for (int i = 0; i < 100_000; i++)
                streamer.addData(i, new Person(i, "name" + i));
        }

        IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"));

        ClientCache<Integer, Person> personCache = client.cache(PERSON_CACHE);

        LogListener lsnr = LogListener.matches("Page locks dump:").build();

        testLog.registerListener(lsnr);

        personCache.query(new SqlFieldsQuery("SELECT COUNT(*) FROM \"person\".Person")).getAll();

        assertFalse("Page locks dump should not be logged", GridTestUtils.waitForCondition(lsnr::check, 5000));
    }

    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        int id;

        /** */
        String name;

        /**
         * @param id Person ID.
         * @param name Name.
         */
        public Person(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }
    }
}
