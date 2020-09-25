package org.apache.ignite.cache.affinity.rendezvous;

import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionBackupFilterAbstractSelfTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

public class SBBackupFilterTest extends AffinityFunctionBackupFilterAbstractSelfTest {

    private int backups = 1;

    @Override
    protected AffinityFunction affinityFunction() {
        RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false);

        aff.setBackupFilter(backupFilter);

        return aff;
    }

    @Override
    protected AffinityFunction affinityFunctionWithAffinityBackupFilter(String attributeName) {
        RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false);

        String[] stringArray = new String[1];

        stringArray[0] = attributeName;

        aff.setAffinityBackupFilter(new ClusterNodeAttributeColocatedBackupFilter(attributeName));

        return aff;
    }

    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(3);

        cacheCfg.setAffinity(affinityFunctionWithAffinityBackupFilter("AFF_CELL"));

        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        cacheCfg.setRebalanceMode(SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheCfg);
        cfg.setUserAttributes(F.asMap("AFF_CELL", "cell_0"));

        return cfg;
    }

}
