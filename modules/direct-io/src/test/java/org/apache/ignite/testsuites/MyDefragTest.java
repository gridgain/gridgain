//package org.apache.ignite.testsuites;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Random;
//import java.util.logging.Formatter;
//import java.util.logging.LogRecord;
//import java.util.logging.Logger;
//import java.util.logging.StreamHandler;
//import org.apache.ignite.Ignite;
//import org.apache.ignite.IgniteCache;
//import org.apache.ignite.IgniteException;
//import org.apache.ignite.Ignition;
//import org.apache.ignite.configuration.CacheConfiguration;
//import org.apache.ignite.configuration.DataPageEvictionMode;
//import org.apache.ignite.configuration.DataRegionConfiguration;
//import org.apache.ignite.configuration.DataStorageConfiguration;
//import org.apache.ignite.configuration.IgniteConfiguration;
//import org.apache.ignite.configuration.WALMode;
//import org.apache.ignite.internal.IgniteEx;
//import org.apache.ignite.internal.pagemem.PageMemory;
//import org.apache.ignite.internal.processors.cache.persistence.defragmentation.maintenance.DefragmentationParameters;
//import org.apache.ignite.internal.util.typedef.internal.CU;
//import org.apache.ignite.maintenance.MaintenanceTask;
//import org.apache.ignite.testframework.ListeningTestLogger;
//import org.apache.ignite.testframework.LogListener;
//import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
//import org.junit.Test;
//import org.apache.ignite.internal.commandline.CommandHandler;
//
//import static org.apache.ignite.cluster.ClusterState.ACTIVE;
//import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
//import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
//
//public class MyDefragTest extends GridCommonAbstractTest {
//
//    private final String cacheName = "myCache";
//
//    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
//        final IgniteConfiguration cfg = super.getConfiguration(gridName);
//
//
//        cfg.setDataStorageConfiguration(createDbConfig());
//
//        cfg.setCacheConfiguration(new CacheConfiguration<>(cacheName));
//
//        return cfg;
//    }
//
//    /**
//     * @return DB config.
//     */
//    private DataStorageConfiguration createDbConfig() {
//        final DataStorageConfiguration memCfg = new DataStorageConfiguration();
//
//        DataRegionConfiguration memPlcCfg = new DataRegionConfiguration();
//        memPlcCfg.setName("dfltDataRegion");
//        memPlcCfg.setPersistenceEnabled(true);
//
//        return memCfg;
//    }
//
//    /** {@inheritDoc} */
//    @Override protected void beforeTestsStarted() throws Exception {
//        super.beforeTestsStarted();
//
//        cleanPersistenceDir();
//    }
//
//    /** {@inheritDoc} */
//    @Override protected void afterTestsStopped() throws Exception {
//        stopAllGrids();
//
//        cleanPersistenceDir();
//    }
//
//
//    @Test
//    public void testDefragmentationSchedule() throws Exception {
//        Ignite ignite = startGrids(2);
//
//        ignite.cluster().state(ACTIVE);
//
//        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--defragmentation", "schedule"));
//
//        String grid0ConsId = grid(0).configuration().getConsistentId().toString();
//        String grid1ConsId = grid(1).configuration().getConsistentId().toString();
//
//        ListeningTestLogger testLog = new ListeningTestLogger();
//
//        CommandHandler cmd = createCommandHandler(testLog);
//
//        LogListener logLsnr = LogListener.matches("Scheduling completed successfully.").build();
//
//        testLog.registerListener(logLsnr);
//
//        assertEquals(EXIT_CODE_OK, execute(
//            cmd,
//            "--defragmentation",
//            "schedule",
//            "--nodes",
//            grid0ConsId
//        ));
//
//        assertTrue(logLsnr.check());
//
//        MaintenanceTask mntcTask = DefragmentationParameters.toStore(Collections.emptyList());
//
//        assertNotNull(grid(0).context().maintenanceRegistry().registerMaintenanceTask(mntcTask));
//        assertNull(grid(1).context().maintenanceRegistry().registerMaintenanceTask(mntcTask));
//
//        stopGrid(0);
//        startGrid(0);
//
//        logLsnr = LogListener.matches("Node is already in Maintenance Mode").build();
//
//        testLog.clearListeners();
//
//        testLog.registerListener(logLsnr);
//
//        assertEquals(EXIT_CODE_OK, execute(
//            cmd,
//            "--defragmentation",
//            "schedule",
//            "--nodes",
//            grid0ConsId
//        ));
//
//        assertTrue(logLsnr.check());
//
//        stopGrid(0);
//        startGrid(0);
//
//        stopGrid(1);
//        startGrid(1);
//
//        stopAllGrids();
//
//        startGrids(2);
//
//        logLsnr = LogListener.matches("Scheduling completed successfully.").times(2).build();
//
//        testLog.clearListeners();
//
//        testLog.registerListener(logLsnr);
//
//        assertEquals(EXIT_CODE_OK, execute(
//            cmd,
//            "--defragmentation",
//            "schedule",
//            "--nodes",
//            String.join(",", grid0ConsId, grid1ConsId)
//        ));
//
//        assertTrue(logLsnr.check());
//    }
//
//
//    @Test
//    public void testDefragmentation() throws Exception {
//
//        final IgniteEx ig = startGrid(0);
//
//        ig.active(true);
//
//
//        IgniteCache<Object, Object> cache = ig.getOrCreateCache(cacheName);
//        pupulateCache(cache);
//
//
//        ArrayList<String> cmdArgs = new ArrayList<>();
//        cmdArgs.add("--defragmentation");
//        cmdArgs.add("schedule");
//        cmdArgs.add("--nodes");
//        cmdArgs.add(ig.localNode().id().toString());
//
//
//        ListeningTestLogger testLog = new ListeningTestLogger();
//
//        CommandHandler cmd = createCommandHandler(testLog);
//
//        cmd.execute(cmdArgs);
//
//        System.out.println("Done");
//    }
//
//
//    private CommandHandler createCommandHandler(ListeningTestLogger testLog) {
//        Logger log = CommandHandler.initLogger(null);
//
//        log.addHandler(new StreamHandler(System.out, new Formatter() {
//            /** {@inheritDoc} */
//            @Override public String format(LogRecord record) {
//                String msg = record.getMessage();
//
//                testLog.info(msg);
//
//                return msg + "\n";
//            }
//        }));
//
//        return new CommandHandler(log);
//    }
//
//
//    private static void pupulateCache(IgniteCache cache){
//        Random rnd = new Random();
//        for(int i = 0; i < 10000; i++) {
//
//            int len = rnd.nextInt(10000);
//            int[] data = new int[len];
//
//            for(int j=0; j < len; j++){
//                data[j] = rnd.nextInt(10000);
//            }
//
//            cache.put(i, data);
//        }
//    }
//}
