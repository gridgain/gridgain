package org.apache.ignite;

import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.client.CommonSecurityCheckTest;
import org.junit.Test;

public class Sdsb12083Test extends CommonSecurityCheckTest {

    @Test
    public void test() throws Exception {
        startGrids(3);

        long start = System.currentTimeMillis();

        System.out.println("========================== start cluster ========================== ");

                try (final IgniteClient thinClient = Ignition.startClient(getClientConfiguration())) {
                    System.out.println("========================== start thin client ========================== ");
                    thinClient.cluster().state(ClusterState.ACTIVE);

                    final ClientCache<Object, Object> cache = thinClient.getOrCreateCache(DEFAULT_CACHE_NAME);
                    System.out.println("========================== create cache ========================== ");

                    for (int j = 0; j < 15; j++)
                        cache.put(j, j);

                    System.out.println("========================== end operations ========================== ");
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }

        System.out.println("========================== end test ========================== ");

        System.out.println("========================= duration=" + (System.currentTimeMillis() - start));
    }
}
