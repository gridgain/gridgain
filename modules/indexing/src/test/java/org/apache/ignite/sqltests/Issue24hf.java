package org.apache.ignite.sqltests;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.LoadAllWarmUpConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.sqltests.models.Account;
import org.apache.ignite.sqltests.models.Subscription;
import org.apache.ignite.testframework.junits.GridAbstractTest;

/**
 * <ol>
 *     <li>Start Server1, Server2, Server3</li>
 *     <li>Start Filler and wait until it finishes</li>
 *     <li>Start Writer (constantly doing updates) and QueryClient (constantly running SELECTS with joins)</li>
 *     <li>Restart any of Server1 || Server2 || Server3 and watch the logs and selected indexes (it will chose __SCAN indexes)</li>
 * </ol>
 */
public class Issue24hf extends GridAbstractTest {

    /**
     * We will use this cache for running SqlFieldsQueries
     */
    private static final String QUERY_CACHE = "query-cache";
    public static final KeyPolicy KEY_POLICY = KeyPolicy.AFFINITY;

    protected Ignite ignite;

    public Issue24hf(Ignite ignite) throws IgniteCheckedException {
        super();
        this.ignite = ignite;
    }

    static IgniteConfiguration nodeConfig(String instance) {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setWorkDirectory("/tmp/ignite/work");
        cfg.setConsistentId(instance);
        cfg.setIgniteInstanceName(instance);
        cfg.setMetricsLogFrequency(90_000);
        cfg.setGridLogger(log);
        cfg.setSqlConfiguration(new SqlConfiguration().setLongQueryWarningTimeout(200));
        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setMetricsEnabled(true)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setWarmUpConfiguration(new LoadAllWarmUpConfiguration())
                .setMetricsEnabled(true)
                .setPersistenceEnabled(true)
            ));

        cfg.setCacheConfiguration(
            new CacheConfiguration(QUERY_CACHE).setAtomicityMode(CacheAtomicityMode.ATOMIC).setCacheMode(CacheMode.REPLICATED),
            cacheConfig(Account.CACHE).setQueryEntities(Collections.singletonList(Account.queryEntity())),
            cacheConfig(Subscription.CACHE).setQueryEntities(Collections.singletonList(Subscription.queryEntity()))
        );

        return cfg;
    }

    static CacheConfiguration cacheConfig(String name) {
        return new CacheConfiguration(name)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
    }

    IgniteCache<Integer, Integer> queryCache() {
        return ignite.cache(QUERY_CACHE);
    }

    IgniteCache accountCache() {
        return ignite.cache(Account.CACHE);
    }

    IgniteCache subscriptionCache() {
        return ignite.cache(Subscription.CACHE);
    }

    static Ignite run(String instance, boolean client) {
        System.setProperty("IGNITE_QUIET", "false");
        System.setProperty("IGNITE_INDEX_COST_FUNCTION", "COMPATIBLE_8_7_6");
        System.setProperty("IGNITE_SHOW_ORDER_OF_INDEXES", "true");
        System.setProperty("IGNITE_SHOW_INDEX_CANDIDATES", "true");
        System.setProperty("IGNITE_SHOW_INDEX_PRETTY_PRINT", "true");

        Ignite ignite = Ignition.start(nodeConfig(instance).setClientMode(client));
        return ignite;
    }

    static class Server1 {

        public static void main(String[] args) throws IgniteException {
            run("server-1", false);
        }

    }

    static class Server2 {

        public static void main(String[] args) throws IgniteException {
            run("server-2", false);
        }

    }

    static class Server3 {

        public static void main(String[] args) throws IgniteException {
            run("server-3", false);
        }

    }

    static final String ACCOUNT_TABLE = "\"account\".Account";
    static final String SUBSCRIPTION_TABLE = "\"subscription\".Subscription";

    static final String QUERY_SUBSCRIPTIONS_BY_ACCOUNT_NAME = "SELECT " +
        "s.id, s.accountId, s.version, s.name, s.subscriptionStatus, s.externalStatus " +
        "FROM " + ACCOUNT_TABLE + " a " +
        "INNER JOIN " + SUBSCRIPTION_TABLE + " s " +
        "ON s.accountId = a.id " +
        "WHERE s.name = ? " +
        "ORDER BY s.version DESC " +
        "FETCH FIRST ROW ONLY";

    static final String QUERY_SUBSCRIPTIONS_BY_ACCOUNT_NUMBER = "SELECT " +
        "a.id, a.number, s.id, s.accountId, s.version, s.name, s.subscriptionStatus, s.externalStatus " +
        "FROM " + ACCOUNT_TABLE + " a " +
        "INNER JOIN " + SUBSCRIPTION_TABLE + " s ON s.accountId = a.id " +
        "INNER JOIN table (subscriptionStatus INTEGER=?) as ss " +
        "INNER JOIN table (externalStatus INTEGER=?) as es " +
        "WHERE a.number = ? AND s.id IN (" +
        "    SELECT i.id FROM " + SUBSCRIPTION_TABLE + " as i " +
        "    WHERE i.name = ? " +
        "    ORDER BY i.version DESC " +
        "    FETCH FIRST ROW ONLY )" +
        "ORDER BY s.version DESC " +
        "FETCH FIRST ROW ONLY";

    public static String qry(String qry, boolean explain) {
        return (explain ? "EXPLAIN ANALYZE " : "") + qry;
    }

    static void pause(long millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static final long QRY_PAUSE = 100;

    static class QueryClient extends Issue24hf {

        public QueryClient(Ignite ignite) throws IgniteCheckedException {
            super(ignite);
        }

        public static void main(String[] args) throws IgniteException, IgniteCheckedException {
            Ignite ignite = run("filler", true);
            QueryClient client = new QueryClient(ignite);
            client.run();
        }

        public void run() {
            while (true) {
                querySubscriptionsByAccount(false);
                pause(QRY_PAUSE);
                querySubscriptionsByAccount(true);
                pause(QRY_PAUSE);
                querySubscriptionsByAccountNumber(false);
                pause(QRY_PAUSE);
                querySubscriptionsByAccountNumber(true);
            }
        }

        private void querySubscriptionsByAccount(boolean explain) {
            String subName = "sub-" + random(ACCOUNTS_COUNT) + '.' + random(ACCOUNT_SUBSCRIPTIONS_COUNT / 3);

            SqlFieldsQuery qry = new SqlFieldsQuery(qry(QUERY_SUBSCRIPTIONS_BY_ACCOUNT_NAME, explain)).setArgs(subName);
            runQuery(queryCache(), qry);
        }

        private void querySubscriptionsByAccountNumber(boolean explain) {
            long subscriptionStatus = random(Subscription.SUBSCRIPTION_STATUS_CARDINALITY);
            long externalStatus = random(Subscription.EXTERNAL_STATUS_CARDINALITY);
            long accountNumber = random(ACCOUNTS_COUNT);
            String subName = "sub-" + accountNumber + '.' + random(ACCOUNT_SUBSCRIPTIONS_COUNT / 3);

            SqlFieldsQuery qry = new SqlFieldsQuery(qry(QUERY_SUBSCRIPTIONS_BY_ACCOUNT_NUMBER, explain)).setArgs(subscriptionStatus, externalStatus, accountNumber, subName);
            runQuery(queryCache(), qry);
        }

        static void runQuery(IgniteCache<Integer, Integer> cache, SqlFieldsQuery qry) {
            List<List<?>> items = cache.query(qry).getAll();
            int size = items.size();
            if (size > 0) {
                String plan = items.get(0).toString();
                if (plan != null && plan.contains("__SCAN")) {
                    System.out.println("Result [" + size + "]: \n" + plan);
                }
            }
        }

    }

    static final int EXPECTED_NODES = 3;
    static final int ACCOUNTS_PER_NODE = 5000;
    static final int ACCOUNTS_COUNT = ACCOUNTS_PER_NODE * EXPECTED_NODES;
    static final int ACCOUNT_SUBSCRIPTIONS_COUNT = 15;

    static class Filler extends Issue24hf {

        public Filler(Ignite ignite) throws IgniteCheckedException {
            super(ignite);
        }

        public static void main(String[] args) throws IgniteException, IgniteCheckedException {
            Ignite ignite = run("filler", true);
            Filler filler = new Filler(ignite);
            filler.fill();
        }

        public void fill() {
            System.out.println("Start filling");
            ignite.cluster().state(ClusterState.ACTIVE);
            AtomicLong idSequence = new AtomicLong(accountCache().size(CachePeekMode.PRIMARY));

            for (int a = 0; a < ACCOUNTS_COUNT; a++) {
                fillAccount(idSequence);
            }

            System.out.println("Finish filling");
            ignite.close();
        }

        public void write() {
            AtomicLong idSequence = new AtomicLong(accountCache().size(CachePeekMode.PRIMARY));

            System.out.println("Start writing");
            while (true) {
                fillAccount(idSequence);
                pause(25);
            }
        }

        public void fillAccount(AtomicLong idSequence) {
            String accountId = UUID.randomUUID().toString();
            long sequence = idSequence.getAndIncrement();
            fillAccount(accountId, sequence);
        }

        public void fillAccount(String accountId, long sequenceId) {
            Account.AccountKey accountKey = new Account.AccountKey();
            accountKey.setId(accountId);

            Account account = new Account();
            account.setName("acc-" + sequenceId);
            account.setNumber(sequenceId);
            accountKey.putToCache(accountCache(), account);

            // Fill subscriptions
            long subscriptionCount = ACCOUNT_SUBSCRIPTIONS_COUNT - 3 + random(6);
            for (int s = 0; s < subscriptionCount; s++) {
                String subId = UUID.randomUUID().toString();
                Subscription.SubscriptionKey subscriptionKey = new Subscription.SubscriptionKey();
                subscriptionKey.setId(subId);
                subscriptionKey.setAccountId(accountId);

                Subscription subscription = new Subscription();
                subscription.setVersion(random(Subscription.VERSION_CARDINALITY));
                subscription.setName("sub-" + sequenceId + '.' + random(subscriptionCount / 3));
                subscription.setSubscriptionStatus(random(Subscription.SUBSCRIPTION_STATUS_CARDINALITY));
                subscription.setExternalStatus(random(Subscription.EXTERNAL_STATUS_CARDINALITY));
                subscriptionKey.putToCache(subscriptionCache(), subscription);
            }
        }

    }

    static class Writer {

        public static void main(String[] args) throws IgniteException, IgniteCheckedException {
            Ignite ignite = run("writer", true);
            Filler filler = new Filler(ignite);
            filler.write();
        }

    }

    public static long random(long ceiling) {
        return random(0, ceiling);
    }

    public static long random(long floor, long ceiling) {
        return floor + (long)(Math.random() * (ceiling - floor));
    }

    public enum KeyPolicy {
        AFFINITY,
        WRAPPED
    }

}
