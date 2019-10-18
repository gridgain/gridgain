/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.glowroot.converter;

import org.apache.ignite.glowroot.converter.model.CacheConfigMeta;
import org.apache.ignite.glowroot.converter.model.TraceItem;
import org.apache.ignite.glowroot.converter.service.GlowrootDataProvider;
import org.apache.ignite.glowroot.converter.service.IgniteDataConsumer;

import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main class of GlowRoot to Ignite data converter app.
 */
public class DataConverter {

    /** **/
    private static final Logger logger = Logger.getLogger(DataConverter.class.getName());

    // TODO: 07.10.19 Write better javadoc: General and param four.

    /**
     * All staff goes here.
     *
     * @param args At least two (and at most four) arguments are expected:
     * <ol>
     * <li>Ignite jdbc thin connection string—e.g., 'jdbc:ignite:thin://127.0.0.1'</li>
     * <li>Path to glowroot data folder—e.g., '/home/username/glowroot/data/'</li>
     * <li>Boolean param that enables dropping all data within corresponding converter-related tables in Ignite
     * before adding new one from glowroot. Default is false.</li>
     * <li>Boolean param that enables overwriting entries if they are already exist in converter-related tables in
     * Ignite. Default false.</li>
     * </ol>
     */
    public static void main(String[] args) {
        validateApplicationArguments(args);

        String igniteJdbcConnStr = args[0];

        String glowrootDataFolder = args[1];

        boolean cleanupAllData = args.length > 2 && Boolean.parseBoolean(args[2]);

        boolean overwriteEntries = args.length > 3 && Boolean.parseBoolean(args[3]);

        try (IgniteDataConsumer igniteDataConsumer = new IgniteDataConsumer(igniteJdbcConnStr, cleanupAllData,
            overwriteEntries)) {
            try (GlowrootDataProvider glowrootDataProvider = new GlowrootDataProvider(glowrootDataFolder)) {
                logger.info("Total transactions cnt: " + glowrootDataProvider.getTotalTxCnt());

                processCacheConfigurations(glowrootDataFolder, igniteDataConsumer, glowrootDataProvider);

                processTraces(igniteDataConsumer, glowrootDataProvider);
            }
            catch (SQLException glowrootInitException) {
                logger.log(Level.SEVERE, "Unable to init glowroot data provider, " +
                    "dataFolderPath=[" + glowrootDataFolder + ']', glowrootInitException);
                System.exit(-1);
            }
            catch (Exception glowrootCleanupException) {
                logger.log(Level.SEVERE, "Unable to clean up glowroot data provider.", glowrootCleanupException);
                System.exit(-1);
            }
        }
        catch (SQLException igniteInitException) {
            logger.log(Level.SEVERE, "Unable to init ignite data consumer, " +
                "url=[" + igniteJdbcConnStr + ']', igniteInitException);
            System.exit(-1);
        }
        catch (Exception igniteCleanupException) {
            logger.log(Level.SEVERE, "Unable to clean up ignite data consumer.", igniteCleanupException);
            System.exit(-1);
        }

        System.exit(0);
    }

    /**
     * Process traces.
     *
     * @param igniteDataConsumer Ignite data consumer.
     * @param glowrootDataProvider Glowroot data provider.
     */
    private static void processTraces(IgniteDataConsumer igniteDataConsumer, GlowrootDataProvider glowrootDataProvider){
        long tracesCnt = 0;

        long processedTxCnt = 0;

        try {
            while (glowrootDataProvider.next()) {
                List<TraceItem> traceItems = glowrootDataProvider.readTraceData();

                igniteDataConsumer.persist(traceItems);

                tracesCnt += traceItems.size();

                processedTxCnt++;

                if (processedTxCnt % 1000 == 0)
                    logger.info("Transactions processed: " + processedTxCnt + " Traces processed: " + tracesCnt);
            }
            logger.info("Transactions processed: " + processedTxCnt + " Traces processed: " + tracesCnt);
        }
        catch (SQLException e) {
            logger.log(Level.SEVERE, "Unable to read glowroot data.", e);

            System.exit(-1);
        }
    }

    /**
     * Process cache configurations.
     *
     * @param glowrootDataFolder Path to glowroot data folder.
     * @param igniteDataConsumer Ignite data consumer.
     * @param glowrootDataProvider Glowroot data provider.
     */
    private static void processCacheConfigurations(String glowrootDataFolder, IgniteDataConsumer igniteDataConsumer,
        GlowrootDataProvider glowrootDataProvider) {
        logger.info("Processing cache configurations...");

        List<CacheConfigMeta> cacheCfgItems = null;

        try {
            cacheCfgItems = glowrootDataProvider.readCacheConfigurations();
        }
        catch (SQLException cacheCfgReadingException) {
            logger.log(Level.SEVERE, "Unable to read cache configurations, " +
                "dataFolderPath=[" + glowrootDataFolder + ']', cacheCfgReadingException);
        }

        long cacheCfgCnt = 0;

        if (cacheCfgItems != null) {
            igniteDataConsumer.persistCacheConfigMeta(cacheCfgItems);

            cacheCfgCnt++;
        }

        logger.info("Cache configurations processed: " + cacheCfgCnt);
    }

    /**
     * Validate application arguments.
     * @param args Arguments to validate.
     */
    private static void validateApplicationArguments(String[] args) {
        if (args.length < 2)
            throw new IllegalArgumentException("Missing mandatory arguments: both ignite jdbc connection string " +
                "and glowroot data directory path should be present.");
        else if (args.length == 3) {
            try {
                Boolean.parseBoolean(args[2]);
            }
            catch (Exception e) {
                throw new IllegalArgumentException("Unexpected argument type: boolean is expected. Third param is " +
                    "for forcing data cleanup before adding new data.");
            }
        }
        else if (args.length == 4) {
            try {
                Boolean.parseBoolean(args[3]);
            }
            catch (Exception e) {
                throw new IllegalArgumentException("Unexpected argument type: boolean is expected. Fourth param is " +
                    "for forcing overwrite mode.");
            }
        }
        else if (args.length > 4)
            throw new IllegalArgumentException("There's some crap in the arguments: at most four arguments " +
                "are expected.");
    }
}
