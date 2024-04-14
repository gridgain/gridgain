package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.distributed.IgniteCacheMessageRecoveryAbstractTest.closeSessions;

public class IgniteRecoveryHeartbeatTest extends GridCommonAbstractTest {
    @Test
    public void testHeartbeatMessageOccurDuringConnectionRecovery() throws Exception {
        LogListener logListener = LogListener.matches("Heartbeat message sent").build();

        IgniteEx ign0 = startGridWithCfg(0, cfg -> {
            ListeningTestLogger logger = new ListeningTestLogger(log, logListener);

            cfg.setGridLogger(logger);

            return cfg;
        });

        IgniteEx ign1 = startGrid(1);

        IgniteCache<Object, Object> cache = ign1.getOrCreateCache(new CacheConfiguration<>("test").setBackups(1));

        for (int i = 0; i < 50; i++) {
            closeSessions(ign1);

            cache.put("foo", "bar");

            if (logListener.check())
                return;

            Thread.sleep(200);
        }

        fail("Heartbeat message wasn't sent");
    }
}
