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

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CacheLocalGetSerializationTest extends GridCommonAbstractTest {
    /**
     * Check correct Map.Entry serialization key on local get/put operation.
     * https://issues.apache.org/jira/browse/IGNITE-10290
     *
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        Ignite ig = startGrid();

        IgniteCache<Map.Entry<Integer, Integer>, Long> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME);

        // Try use Map.Entry as key in destributed map.
        Map.Entry<Integer, Integer> key = new Map.Entry<Integer, Integer>() {
            @Override public Integer getKey() {
                return 123;
            }

            @Override public Integer getValue() {
                return 123;
            }

            @Override public Integer setValue(Integer value) {
                throw new UnsupportedOperationException();
            }
        };

        cache.put(key, Long.MAX_VALUE);

        Long val = cache.get(key);

        Assert.assertNotNull(val);
        Assert.assertEquals(Long.MAX_VALUE, (long)val);
    }
}
