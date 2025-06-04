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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Tests for SQL MERGE.
 */
public class RbcJoinDuplicatesTest extends GridCommonAbstractTest {

    private static final String QUERY_CACHE = "_qry";
    private static final String RISK_CACHE = "risk";
    private static final String MARKET_CACHE = "market";

    private Ignite client;
    private IgniteCache<RiskKey, Risk> riskCache;
    private IgniteCache<MarketKey, Market> marketCache;

    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSqlConfiguration(new SqlConfiguration().setLongQueryWarningTimeout(1000));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                        .setInitialSize(256 * 1024 * 1024)
                        .setMaxSize(256 * 1024 * 1024)
                        .setPersistenceEnabled(false)
                ));

        cfg.setCacheConfiguration(
                new CacheConfiguration<>(QUERY_CACHE),
                new CacheConfiguration<RiskKey, Risk>(RISK_CACHE)
                        .setBackups(1)
                        .setCacheMode(CacheMode.PARTITIONED)
                        .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                        .setIndexedTypes(RiskKey.class, Risk.class),
                new CacheConfiguration<MarketKey, Market>(MARKET_CACHE)
                        .setBackups(1)
                        .setCacheMode(CacheMode.REPLICATED)
                        .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                        .setIndexedTypes(MarketKey.class, Market.class)
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override
    protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override
    protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    @Test
    public void runScenario() throws Exception {
        startGrids(4);

        client = startClientGrid();

        riskCache = client.cache(RISK_CACHE);
        marketCache = client.cache(MARKET_CACHE);

        for (int i = 0; i < RISK_VOLUME; i++) {
            RiskKey k = RiskKey.from(i);
            Risk v = Risk.from(k);
            riskCache.put(k, v);
            if (i % 1000 == 0) log("Insert risk #" + i);
        }

        for (int i = 0; i < MARKET_VOLUME; i++) {
            MarketKey k = MarketKey.from(i);
            Market v = Market.from(k);
            marketCache.put(k, v);
            if (i % 1000 == 0) log("Insert market #" + i);
        }

        log("\n\n>>> Finish initial load");
        CompletableFuture.runAsync(this::streamUpdates);

        repeat(100, i -> {
            log(">>> Query #" + i);
            client.cache(QUERY_CACHE).query(new SqlFieldsQuery(QUERY)).forEach(r -> {
                int fldIdx = 0;
                String market = (String) r.get(fldIdx++);
                Long count = (Long) r.get(fldIdx++);
                Long countDistinct = (Long) r.get(fldIdx++);

                if (countDistinct > 1) {
                    log(String.valueOf(r));
                }
            });
        });
    }

    protected void streamUpdates() {
        //loadViaKv();
        loadViaDs();
    }

    protected void loadViaKv() {
        repeat(10, i -> {
            MarketKey k = MarketKey.onLoad(i);
            Market v = Market.from(k);
            marketCache.put(k, v);
        });
    }

    protected void loadViaDs() {
        IgniteDataStreamer<MarketKey, Market> dataStreamer = client.dataStreamer(MARKET_CACHE);
        dataStreamer.autoFlushFrequency(100);

        repeat(10, i -> {
            MarketKey k = MarketKey.onLoad(i);
            Market v = Market.from(k);
            dataStreamer.addData(k, v);
        });
    }

    private static final String QUERY = "SELECT r.market, count(quote), count(distinct quote)\n" +
            "FROM \"risk\".risk r\n" +
            "LEFT OUTER JOIN \"market\".market md  ON r.market = md.market\n" +
            "GROUP BY r.market\n" +
            "ORDER BY r.market" +
            "";

    /*
    We create and fill relatively big amount of records,
    but update stream will hit only narrow range of MARKET records [MARKET_RANGE_START, MARKET_RANGE_END],
    this help us to ensure that it will take some time for each join fragment (MAP stage) to collect result set,
    but at the same time there always be some ongoing update that hadn't benn replicated to all fragment nodes yet
     */

    private static final int BOOK_VARIETY = 50;
    private static final int MARKET_VARIETY = 1000;

    private static final int RISK_VOLUME = BOOK_VARIETY * MARKET_VARIETY * 2;
    private static final int MARKET_VOLUME = MARKET_VARIETY;

    private static final int MARKET_RANGE_START = 0;
    private static final int MARKET_RANGE_END = 10;

    /* Two methods below help us to overload particular range of market records */
    private static int fromMarketTargetRange() {
        return randomInt(MARKET_RANGE_START, MARKET_RANGE_END);
    }

    private static int probeMarketTargetRangeId() {
        return chance(10)
                ? randomInt(MARKET_VARIETY)
                : fromMarketTargetRange();
    }

    private static class RiskKey {

        @AffinityKeyMapped
        @QuerySqlField(index = true)
        String book;

        @QuerySqlField(index = true)
        String market;

        static RiskKey from(long id) {
            RiskKey instance = new RiskKey();
            instance.book = "book-" + randomInt(BOOK_VARIETY);
            instance.market = "market-" + probeMarketTargetRangeId();
            return instance;
        }
    }

    private static class Risk {

        @QuerySqlField
        private double val;

        static Risk from(RiskKey riskKey) {
            Risk instance = new Risk();
            instance.val = randomLong(1000);
            return instance;
        }
    }

    private static class MarketKey {

        @QuerySqlField(index = true)
        private String market;

        static MarketKey from(long id) {
            MarketKey instance = new MarketKey();
            instance.market = "market-" + randomInt(MARKET_VARIETY);
            return instance;
        }

        static MarketKey onLoad(long id) {
            MarketKey instance = new MarketKey();
            instance.market = "market-" + fromMarketTargetRange();
            return instance;
        }
    }

    private static final AtomicLong QUOTE_SEQ = new AtomicLong(0);

    private static class Market {

        @QuerySqlField
        private double quote;

        static Market from(MarketKey riskKey) {
            Market instance = new Market();
            instance.quote = QUOTE_SEQ.getAndIncrement();
            return instance;
        }
    }

    // Random

    public static int randomInt(int ceiling) {
        return (int) (Math.random() * ceiling);
    }

    public static int randomInt(int floor, int ceiling) {
        return floor + (int) (Math.random() * (ceiling - floor));
    }

    public static int randomIntTopHalf(int ceiling) {
        return randomInt(ceiling / 2, ceiling);
    }

    public static int randomDispersion(int target, int dispersion) {
        return randomInt(target - dispersion, target + dispersion);
    }

    public static long randomLong(long ceiling) {
        return (long) (Math.random() * ceiling);
    }

    public static long randomLong(long floor, long ceiling) {
        return floor + randomLong(ceiling - floor);
    }

    public static long randomLongTopHalf(long ceiling) {
        return randomLong(ceiling / 2, ceiling);
    }

    public static long randomDispersion(long target, long dispersion) {
        return randomLong(target - dispersion, target + dispersion);
    }

    public static long randomSign(long challenge) {
        long r = randomLong(challenge);
        return r % 2 == 0 ? -1 : 1;
    }

    public static boolean chance(int percentage) {
        return percentage != 0 && (percentage == 100 || randomInt(100) <= percentage);
    }

    public static <T> T randomOf(T... values) {
        int size = values.length;
        return values[randomInt(size)];
    }

    public static <T> T randomOf(List<T> values) {
        return values.get(randomInt(values.size()));
    }

    // Stuff

    private static void log(String msg) {
        System.out.println(msg);
    }

    public static void repeat(int pauseMillis, long max, Consumer<Long> action) {
        long iteration = 0;
        while (!Thread.currentThread().isInterrupted() && (max < 0 || iteration != max)) {
            action.accept(iteration++);
            pause(pauseMillis);
        }
    }

    public static void repeat(int pauseMillis, Consumer<Long> action) {
        repeat(pauseMillis, -1, action);
    }

    public static void pause(long millis) {
        if (millis > 0) {
            try {
                TimeUnit.MILLISECONDS.sleep(millis);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
