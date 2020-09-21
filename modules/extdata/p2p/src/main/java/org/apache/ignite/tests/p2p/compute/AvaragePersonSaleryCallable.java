/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.tests.p2p.compute;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.tests.p2p.cache.Person;

/**
 * This closure calculate average salary of person in the defined key range.
 */
public class AvaragePersonSaleryCallable implements IgniteCallable<Double> {
    /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Cache name. */
    private String cacheName;

    /** Left range border. */
    private int from;

    /** Right range border. */
    private int to;

    /**
     * @param cacheName Cache name.
     * @param from First enrty key.
     * @param to Up border of keys.
     */
    public AvaragePersonSaleryCallable(String cacheName, int from, int to) {
        this.cacheName = cacheName;
        this.from = from;
        this.to = to;
    }

    /** {@inheritDoc} */
    @Override public Double call() {
        log.info("Job was started with parameters: [caceh=" + cacheName +
            ", from=" + from +
            ", to=" + to + ']');

        IgniteCache<Integer, Person> cache = ignite.cache(cacheName);

        if (cache == null)
            return 0D;

        double amount = 0;

        Set<Integer> keys = IntStream.range(from, to).boxed().collect(Collectors.toSet());

        Map<Integer, Person> entries = cache.getAll(keys);

        for (Integer key: keys) {
//            BinaryObject bo = (BinaryObject)cache.withKeepBinary().get(i);
//            Person p = bo.deserialize(getClass().getClassLoader());

            Person p = cache.get(key);

            Person p1 = entries.get(key);

            assert p.equals(p1);

            amount += p.getSalary();
        }

        return amount / (to - from);
    }
}
