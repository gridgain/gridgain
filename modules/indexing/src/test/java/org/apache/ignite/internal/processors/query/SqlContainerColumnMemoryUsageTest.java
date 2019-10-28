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

package org.apache.ignite.internal.processors.query;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2JavaObject;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** */
public class SqlContainerColumnMemoryUsageTest extends AbstractIndexingCommonTest {
    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setClientMode(client);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        client = true;

        IgniteEx cli = startGrid(42);

        cli.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, EntryValue.class));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
    }

    /** */
    private IgniteEx clientNode() {
        return grid(42);
    }

    /** */
    @Test
    public void testArrayColumnMessageSize() throws Exception {
        Object[] arr = Stream.generate(this::testObject).limit(10).toArray();

        checkColumnMessageSize(arr, 10 * testObjectSize(), 11 * testObjectSize());
    }

    /** */
    @Test
    public void testCollectionColumnMessageSize() throws Exception {
        List<Holder> list = Stream.generate(this::testObject).limit(10).collect(Collectors.toList());

        checkColumnMessageSize(list, 10 * testObjectSize(), 11 * testObjectSize());
    }

    /** */
    @Test
    public void testMapColumnMessageSize1() throws Exception {
        Map<Integer, Holder> map = IntStream.range(0, 10)
            .boxed()
            .collect(Collectors.toMap(Function.identity(), i -> testObject()));

        checkColumnMessageSize(map, 10 * (testObjectSize() + 5), 11 * (testObjectSize() + 5));
    }

    /** */
    @Test
    public void testMapColumnMessageSize2() throws Exception {
        Map<Holder, Integer> map = IntStream.range(0, 10)
            .boxed()
            .collect(Collectors.toMap(i -> new Holder(new byte[100 + i]), Function.identity()));

        int mapEntriesSize = 0;

        for (Holder holder : map.keySet())
            mapEntriesSize += objectSize(holder) + 5;

        checkColumnMessageSize(map, mapEntriesSize, mapEntriesSize + objectSize(map.keySet().iterator().next()) + 5);
    }

    /** */
    @Test
    public void testMapColumnMessageSize3() throws Exception {
        Map<Holder, Holder> map = IntStream.range(0, 10)
            .boxed()
            .collect(Collectors.toMap(i -> new Holder(new byte[100 + i]), i -> testObject()));

        int mapEntriesSize = 0;

        for (Holder holder : map.keySet())
            mapEntriesSize += objectSize(holder) + testObjectSize();

        checkColumnMessageSize(map, mapEntriesSize, mapEntriesSize + objectSize(map.keySet().iterator().next()) + testObjectSize());
    }

    /** */
    private void checkColumnMessageSize(Object container, int sizeLower, int sizeUpper) throws Exception {
        IgniteCache<Object, Object> cache = clientNode().cache(DEFAULT_CACHE_NAME);

        cache.put(1, new EntryValue(container));

        TestRecordingCommunicationSpi comm = (TestRecordingCommunicationSpi)grid(0).configuration().getCommunicationSpi();
        comm.record(GridQueryNextPageResponse.class);

        cache.query(new SqlFieldsQuery("select payload from EntryValue where _key = 1")).getAll();

        List<Object> msgs = comm.recordedMessages(true);

        byte[] columnBytes = extractColumnBytes(msgs);

        // Kind of smoke check to ensure that each container element do not need memory equal to whole container size
        assertTrue(sizeLower < columnBytes.length && columnBytes.length < sizeUpper);
    }

    /** */
    private Holder testObject() {
        return new Holder(new byte[100]);
    }

    /** */
    private int testObjectSize() throws Exception {
        return objectSize(testObject());
    }

    /** */
    private int objectSize(Object obj) throws Exception {
        return grid(0).configuration().getMarshaller().marshal(obj).length;
    }

    /** */
    private byte[] extractColumnBytes(List<Object> msgs) throws Exception {
        assert msgs.size() == 1;

        GridQueryNextPageResponse msg = (GridQueryNextPageResponse)msgs.get(0);

        assert msg.values().size() == 1;

        GridH2JavaObject valMsg = (GridH2JavaObject)msg.values().iterator().next();

        return GridTestUtils.getFieldValue(valMsg, "b");
    }

    /** */
    private static class EntryValue {
        /** */
        @QuerySqlField
        Object payload;

        /** */
        public EntryValue(Object payload) {
            this.payload = payload;
        }
    }

    /** */
    private static class Holder {
        /** */
        Object x;

        /** */
        public Holder(Object x) {
            this.x = x;
        }
    }
}
