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
package org.apache.ignite.tests.p2p;

import java.io.Serializable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;

public class P2PTestPredicate extends GridCacheIdMessage implements GridCacheDeployable, IgniteBiPredicate, Serializable {
    @IgniteInstanceResource
    Ignite ignite;

    @Override public boolean addDeploymentInfo() {
        return true;
    }

    @Override public boolean apply(Object o, Object o2) {

        Thread thread = new Thread(() -> {
            try {
                //Thread.sleep(2000);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            X.println("!!! Executing scan query inside of predicate.");

            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache("cache");

            cache.query(new ScanQuery<IgniteBiPredicate, Integer>(new P2PTestPredicate2()));
        });
        thread.start();

        return false;
        //throw new RuntimeException("!!!test");
    }

    @Override public short directType() {
        return 0;
    }
}
