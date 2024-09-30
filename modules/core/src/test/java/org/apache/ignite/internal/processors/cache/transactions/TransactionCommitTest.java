/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class TransactionCommitTest extends GridCommonAbstractTest {
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setCacheConfiguration(defaultCacheConfiguration());
        return cfg;
    }

    @Test
    public void testCommit() throws Exception {
        IgniteEx server = startGrid(0);

        IgniteEx client = startClientGrid(1);

        IgniteInternalCache<Object, Object> cache = client.cachex(DEFAULT_CACHE_NAME);

        GridCacheSharedContext<Object, Object> cctx = cache.context().shared();

        GridNearTxLocal tx = txStart(cctx);

        cache.put(1, Integer.toString(1));
        client.close();

        GridTestUtils.assertThrows(log, () -> {
            tx.prepare(true);
            return null;
        }, IgniteException.class, "Locking manager is not available (probably, disconnected from a cluster)");
    }

    private GridNearTxLocal txStart(GridCacheSharedContext<Object, Object> cctx) {
        GridNearTxLocal tx = cctx.tm().userTx();
        if (tx == null) {
            TransactionConfiguration tCfg = cctx.kernalContext().config()
                    .getTransactionConfiguration();
            tx = cctx.tm().newTx(
                    /*implicit*/false,
                    /*implicit single*/false,
                    null,
                    tCfg.getDefaultTxConcurrency(),
                    tCfg.getDefaultTxIsolation(),
                    tCfg.getDefaultTxTimeout(),
                    /*store enabled*/true,
                    /*sql*/false,
                    /*tx size*/0,
                    null
            );
        }
        return tx;
    }
}
