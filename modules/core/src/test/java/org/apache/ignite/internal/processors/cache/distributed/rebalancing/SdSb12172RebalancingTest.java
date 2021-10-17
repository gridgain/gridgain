package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteRebalanceIterator;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class SdSb12172RebalancingTest  extends GridCommonAbstractTest {

    private static final String[] caches = {
            "com.sbt.processing.ta.dpl.model.CardAuthProfile_DPL_unique_constraint_cache",
            "com.sbt.limits.data.entity.LimitTransactionV1Entity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.ServiceFeeByShopEntity_DPL_unique_constraint_cache",
            "com.sbt.processing.clr.model.dpl.FileLoaderLogBucketParticle_DPL_unique_constraint_cache",
            "com.sbt.processing.clr.model.dpl.TwRequestParticle_DPL_unique_constraint_cache",
            "com.sbt.processing.clr.model.dpl.UserSettingsUIParticle_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.TransactionDuplicateEntity_DPL_unique_constraint_cache",
            "com.sbt.processing.ta.dpl.model.IsoTransaction_DPL_unique_constraint_cache",
            "com.sbt.processing.clr.model.dpl.TransactionParticle_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.ScheduledTaskCatalogEntity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.TerminalTurnoverPerMonthEntity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.ShopReconciliationErrorEntity_DPL_unique_constraint_cache",
            "com.sbt.limits.data.entity.LimitInstanceV1Entity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.LoyaltyFeeEntity_DPL_unique_constraint_cache",
            "com.sbt.processing.replication.dpl.data.ReplicationApplyErrorV1Entity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.ClientInfoV1Entity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.FeeV1Entity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.Token7ZpanEntity_DPL_unique_constraint_cache",
            "is_profile_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.TerminalBatchEntity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.DEKEntity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.MiniStatementV1Entity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.Token7IdxEntity_DPL_unique_constraint_cache",
            "is_plastic_DPL_unique_constraint_cache",
            "com.sbt.card.data.entity.EventListV2Entity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.ProductAccountingEntity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.Token9ZpanEntity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.TransactionBundleV1Entity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.MiniStatementV2Entity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.FF3KeysEntity_DPL_unique_constraint_cache",
            "com.sbt.pcidss.servicepoints.model.ServicePointSubdivisionEntity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.Token6IdxEntity_DPL_unique_constraint_cache",
            "com.sbt.processing.replication.dpl.data.ReplicationApplyStateV1Entity_DPL_unique_constraint_cache",
            "com.sbt.processing.ta.dpl.model.EmvCommand_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.AcquiringContractEntity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.GrantLifeCardEntity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.DocSearchV1Entity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.CustomReportExportCatalogEntity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.Token3ZpanEntity_DPL_unique_constraint_cache",
            "com.sbt.card.data.entity.CardV2Entity_DPL_unique_constraint_cache",
            "com.sbt.pcidss.servicepoints.model.ServicePointAtmEntity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.SubscriptionPaymentToolV1Entity_DPL_unique_constraint_cache",
            "is_crypto_param_DPL_unique_constraint_cache",
            "com.sbt.processing.clr.model.dpl.FileLoaderLogParticle_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.PayOrderEntity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.Token4ZpanEntity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.SberIdRequestEntity_DPL_unique_constraint_cache",
            "com.sbt.card.data.entity.CryptoParamV2Entity_DPL_unique_constraint_cache",
            "com.sbt.processing.dm.model.dpl.JournalEntryParticle_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.AccessToolV1Entity_DPL_unique_constraint_cache",
            "com.sbt.limits.data.entity.LimitTransactionV2Entity_DPL_unique_constraint_cache",
            "com.sbt.card.supin.data.entity.SupinKeysV2Entity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.CommissionAggregationEntity_DPL_unique_constraint_cache",
            "com.sbt.limits.data.entity.LimitPersonalValueHistoryV1Entity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.EpkClientInfoV1Entity_DPL_unique_constraint_cache",
            "com.sbt.processing.dm.model.dpl.UIClrTransactionParticle_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.PaymentOrderEntity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.TransportMessageEntity_DPL_unique_constraint_cache",
            "com.sbt.processing.clr.model.dpl.NotificationParticle_DPL_unique_constraint_cache",
            "com.sbt.processing.dm.model.dpl.ClaimCaseTransactionParticle_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.ServiceFeeByTerminalEntity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.ShopEntity_DPL_unique_constraint_cache",
            "com.sbt.processing.ta.dpl.model.PaymentToken_DPL_unique_constraint_cache",
            "is_card_DPL_unique_constraint_cache",
            "com.sbt.limits.data.entity.LimitCustomizeV2Entity_DPL_unique_constraint_cache",
            "com.sbt.card.data.entity.PersoCardDataEntity_DPL_unique_constraint_cache",
            "com.sbt.card.supin.data.entity.SupinClaimV1Entity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.Token8ZpanEntity_DPL_unique_constraint_cache",
            "com.sbt.limits.data.entity.LimitInstanceV2Entity_DPL_unique_constraint_cache",
            "com.sbt.limits.data.entity.LimitSetV1Entity_DPL_unique_constraint_cache",
            "com.sbt.processing.clr.model.dpl.AuditJournalEntryParticle_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.Token4IdxEntity_DPL_unique_constraint_cache",
            "com.sbt.pcidss.servicepoints.model.AcquiringV1Entity_DPL_unique_constraint_cache",
            "com.sbt.limits.data.entity.RuleDimensionV1Entity_DPL_unique_constraint_cache",
            "com.sbt.limits.data.entity.LimitContainerV1Entity_DPL_unique_constraint_cache",
            "com.sbt.card.supin.data.entity.SupinKeysV1Entity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.TransactionV1Entity_DPL_unique_constraint_cache",
            "com.sbt.pcidss.servicepoints.model.ServicePointTerminalEntity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.Token9IdxEntity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.DelayedRefundEntity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.AccountDetailsV1Entity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.Token5IdxEntity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.AccountingRegisterV1Entity_DPL_unique_constraint_cache",
            "com.sbt.limits.data.entity.RuleConditionV1Entity_DPL_unique_constraint_cache",
            "com.sbt.card.data.entity.ParamInstV2Entity_DPL_unique_constraint_cache",
            "com.sbt.limits.data.entity.LimitContainerV2Entity_DPL_unique_constraint_cache",
            "com.sbt.processing.ta.dpl.model.tds.TdsEnrollment_DPL_unique_constraint_cache",
            "com.sbt.processing.clr.model.dpl.BpcIataTransactionParticle_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.OrganizationEntity_DPL_unique_constraint_cache",
            "com.sbt.limits.data.entity.LimitTemplateV1Entity_DPL_unique_constraint_cache",
            "com.sbt.pcidss.servicepoints.model.ServicePointShopEntity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.HoldV1Entity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.ProcessingOrderEntity_DPL_unique_constraint_cache",
            "com.sbt.limits.data.entity.LimitCustomizeV1Entity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.AcqTransactionV1Entity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.ShopTurnoverCashOutPerMonthEntity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.PartnerRegistryEntity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.PaymentProductV1Entity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.Token6DZpanEntity_DPL_unique_constraint_cache",
            "bp_perso_paraminst_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.StatusDetailsV1Entity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.OrganizationTurnoverPerMonthEntity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.PartnerTransactionEntity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.Token6DIdxEntity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.TerminalEntity_DPL_unique_constraint_cache",
            "com.sbt.card.data.entity.MaskedDataEntity_DPL_unique_constraint_cache",
            "com.sbt.processing.clr.fees.rules.dpl.model.particle.PeriodicFeeRuleParticle_DPL_unique_constraint_cache",
            "bp_perso_reqinst_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.SubdivisionLocationEntity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.SberIdReqEntity_DPL_unique_constraint_cache",
            "com.sbt.processing.replication.dpl.data.ReplicationErrorStateV1Entity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.Token5ZpanEntity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.integration.tokenization.data.entity.CardTokenEntity_DPL_unique_constraint_cache",
            "com.sbt.processing.clr.model.dpl.BinTableFileLogParticle_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.ClientToPartnerV1Entity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.Token6ZpanEntity_DPL_unique_constraint_cache",
            "com.sbt.processing.clr.model.dpl.BinTableParticle_DPL_unique_constraint_cache",
            "com.sbt.pcidss.servicepoints.model.ServicePointAtmV2Entity_DPL_unique_constraint_cache",
            "com.sbt.limits.data.entity.LimitTransactionCheckV1Entity_DPL_unique_constraint_cache",
            "com.sbt.card.data.entity.PlasticV2Entity_DPL_unique_constraint_cache",
            "com.sbt.card.data.entity.SecureProfileV2Entity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.ServiceFeeByContractEntity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.TerminalTurnoverPerDateEntity_DPL_unique_constraint_cache",
            "com.sbt.processing.dm.model.dpl.ClaimCaseParticle_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.ScheduleLogV1Entity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.ClientSubscriptionForPartnerV1Entity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.Token8IdxEntity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.SubscrTransactionV1Entity_DPL_unique_constraint_cache",
            "com.sbt.card.data.entity.RequestInstV2Entity_DPL_unique_constraint_cache",
            "com.sbt.processing.replication.dpl.data.ReplicationErrorStateV2Entity_DPL_unique_constraint_cache",
            "com.sbt.processing.clr.fees.rules.dpl.model.particle.FeeRuleParticle_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.RqUidToClientV1Entity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.ProductToClientV1Entity_DPL_unique_constraint_cache",
            "com.sbt.processing.data.CardToClientEntity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.Token3IdxEntity_DPL_unique_constraint_cache",
            "com.sbt.limits.data.entity.LimitTransactionCheckV2Entity_DPL_unique_constraint_cache",
            "com.sbt.pcidss.servicepoints.model.ServicePointAtmV3Entity_DPL_unique_constraint_cache",
            "com.sbt.card.data.entity.PlasticCardProfileV2Entity_DPL_unique_constraint_cache",
            "com.sbt.tokenization.data.entity.KEKEntity_DPL_unique_constraint_cache",
            "com.sbt.acquiring.processing.entities.particles.AdditionalAgreementEntity_DPL_unique_constraint_cache",
            "com.sbt.pcidss.servicepoints.model.AcquiringProvidersV1Entity_DPL_unique_constraint_cache",
            "com.sbt.tw.data.entity.CardRelationV1Entity_DPL_unique_constraint_cache",
            "com.sbt.processing.ta.dpl.model.tds.TdsMessageToClient_DPL_unique_constraint_cache"
    };

    private static final String CACHE_GROUP = "CACHEGROUP_UNIQUE_PARTICLE_union-module";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);
        final DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
        storageCfg.getDefaultDataRegionConfiguration().setInitialSize(1024L * 1024L * 1024L);
        storageCfg.getDefaultDataRegionConfiguration().setMaxSize(1024L * 1024L * 1024L);
        cfg.setDataStorageConfiguration(storageCfg);

        List<CacheConfiguration> configurationsList = Stream.of(caches)
                .map(t -> getCacheConfiguration(t, CACHE_GROUP))
                .collect(Collectors.toList());

        CacheConfiguration[] configurations = new CacheConfiguration[configurationsList.size()];
        configurationsList.toArray(configurations);

        cfg.setCacheConfiguration(configurations);

        return cfg;
    }

    private CacheConfiguration getCacheConfiguration(String cacheName, String cacheGroupName) {
        return new CacheConfiguration(cacheName)
                .setSqlSchema("PUBLIC")
                .setCacheMode(CacheMode.PARTITIONED)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(3)
                .setGroupName(cacheGroupName)
                .setAffinity(new RendezvousAffinityFunction(false, 16384));
    }

    @Override
    protected void beforeTestsStarted() throws Exception {
        //   cleanPersistenceDir();
    }

    /**
     * Prepare LFS before rebalance problem partition.
     * Preparation: copy problem partition to IGNITE_WORK_DIR\testPart\part-16241.bin
     *
     * Scenario:
     * 1. Start 2 nodes
     * 2. Activate
     * 3. Fill some caches
     * 4. Wait checkpoit
     * 5. Stop all nodes.
     * 6. Clean LFS on demander node.
     * 7. Remove index.bin and copy prepared partition to supplier LFS.
     *
     * @throws Exception Exception.
     */
    @Test
    public void prepareLFS() throws Exception {
        IgniteEx igniteEx = startGrid("supplier");

        startGrid("demander");

        igniteEx.cluster().state(ClusterState.ACTIVE);

        final IgniteCache<Object, Object> cache = igniteEx.cache("com.sbt.tw.data.entity.FeeV1Entity_DPL_unique_constraint_cache");

        for (int i = 0; i < 10_000; i++) {
            cache.put(i, i);
        }

        final IgniteCache<Object, Object> cache2 = igniteEx.cache("com.sbt.limits.data.entity.LimitInstanceV2Entity_DPL_unique_constraint_cache");

        for (int i = 10_000; i < 20_000; i++) {
            cache2.put(i, i);
        }

        awaitPartitionMapExchange();

        forceCheckpoint();

        stopAllGrids();

        cleanPersistenceDir("demander");

        U.delete(new File(U.workDirectory(null, null), "db\\supplier\\cacheGroup-CACHEGROUP_UNIQUE_PARTICLE_union-module\\index.bin"));

        U.copy(
                new File(U.workDirectory(null, null), "testPart\\part-16241.bin"),
                new File(U.workDirectory(null, null), "db\\supplier\\cacheGroup-CACHEGROUP_UNIQUE_PARTICLE_union-module\\part-16241.bin"),
                true);
    }

    /**
     *  Preparation: run prepareLFS()
     *
     *  Scenario:
     *  1. Start supplier node.
     *  2. Start demander node(cleaned).
     *  3. Activate.
     *  4. Wait rebalance.
     *  5. Wait checkpoint.
     *  6. Count part rows on demander and supplier by rebalance iterator.
     *
     * @throws Exception Exception.
     */
    @Test
    public void testRebalanceWithProblemPartition() throws Exception {

        IgniteEx supplier = startGrid("supplier");

        IgniteEx demander = startGrid("demander");

        supplier.cluster().state(ClusterState.ACTIVE);

        GridDhtPartitionDemander.RebalanceFuture fut = (GridDhtPartitionDemander.RebalanceFuture)grid("demander").context().
                cache().internalCache("com.sbt.limits.data.entity.LimitInstanceV2Entity_DPL_unique_constraint_cache").preloader().rebalanceFuture();

        fut.get();

        forceCheckpoint();

        log.info("Count rows for 16241 on demander, total: " + countPartitionRowsByRebalanceIterator(demander));

        log.info("Count rows for 16241 on supplier, total: " + countPartitionRowsByRebalanceIterator(supplier));
    }

    /**
     * Count rows by {@link IgniteRebalanceIterator} for partition 16241.
     *
     * @param igniteEx Node.
     * @return Rows count.
     * @throws IgniteCheckedException Exception
     */
    private int countPartitionRowsByRebalanceIterator(IgniteEx igniteEx) throws IgniteCheckedException {
        CacheGroupContext grp = igniteEx.context().cache().cacheGroup(
                CU.cacheGroupId("com.sbt.limits.data.entity.LimitInstanceV2Entity_DPL_unique_constraint_cache", CACHE_GROUP)
        );

        AffinityTopologyVersion topVer = grp.affinity().lastVersion();

        IgniteCacheOffheapManager offh = grp.offheap();

        IgniteDhtDemandedPartitionsMap map = new IgniteDhtDemandedPartitionsMap();

        map.addFull(16241);

        IgniteRebalanceIterator it = offh.rebalanceIterator(map, topVer);

        int cnt = 0;

        while (it.hasNext()) {
            cnt++;

            it.next();
        }

        return cnt;
    }
}
