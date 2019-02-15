/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class BinaryMetadataRegistrationInsideEntryProcessorTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "test-cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder()
            .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));

        return new IgniteConfiguration()
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder))
            .setPeerClassLoadingEnabled(true);
    }

    /**
     * @throws Exception If failed;
     */
    @Test
    public void test() throws Exception {
        Ignite ignite = startGrids(2);

        IgniteCache<Integer, Map<Integer, CustomObj>> cache = ignite.createCache(CACHE_NAME);

        try {
            for (int i = 0; i < 10_000; i++)
                cache.invoke(i, new CustomProcessor());
        }
        catch (Exception e) {
            Map<Integer, CustomObj> val = cache.get(1);

            if ((val != null) && (val.get(1).anEnum == CustomEnum.ONE) && val.get(1).obj.data.equals("test"))
                System.out.println("Data was saved.");
            else
                System.out.println("Data wasn't saved.");

            throw e;
        }
    }

    /**
     *
     */
    private static class CustomProcessor implements EntryProcessor<Integer,
        Map<Integer, CustomObj>, Object> {
        /** {@inheritDoc} */
        @Override public Object process(
            MutableEntry<Integer, Map<Integer, CustomObj>> entry,
            Object... objects) throws EntryProcessorException {
            Map<Integer, CustomObj> map = new HashMap<>();

            map.put(1, new CustomObj(new CustomInnerObject("test"), CustomEnum.ONE));

            entry.setValue(map);

            return null;
        }
    }

    /**
     *
     */
    private static class CustomObj {
        /** Object. */
        private final CustomInnerObject obj;

        /** Enum. */
        private final CustomEnum anEnum;

        /**
         * @param obj Object.
         * @param anEnum Enum.
         */
        CustomObj(
            CustomInnerObject obj,
            CustomEnum anEnum) {
            this.obj = obj;
            this.anEnum = anEnum;
        }
    }

    /**
     *
     */
    private enum CustomEnum {
        /** */ONE(1),
        /** */TWO(2),
        /** */THREE(3);

        /** Value. */
        private final Object val;

        /**
         * @param val Value.
         */
        CustomEnum(Object val) {
            this.val = val;
        }
    }

    /**
     *
     */
    private static class CustomInnerObject {
        /** */
        private final String data;

        /**
         * @param data Data.
         */
        CustomInnerObject(String data) {
            this.data = data;
        }
    }
}
