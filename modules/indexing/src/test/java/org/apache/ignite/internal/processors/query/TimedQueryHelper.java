package org.apache.ignite.internal.processors.query;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * This class helps to prepare a query which will run for a specific amount of time
 * and able to be cancelled by timeout.
 * Some tricks is needed because internally (H2) a query is checked for timeout after retrieving every N rows.
 */
public class TimedQueryHelper {
    private static final int ROW_COUNT = 250;

    private final long executionTime;
    private final String cacheName;

    public TimedQueryHelper(long executionTime, String cacheName) {
        assert executionTime >= ROW_COUNT;

        this.executionTime = executionTime;
        this.cacheName = cacheName;
    }

    public void createCache(Ignite ign) {
        IgniteCache<Object, Object> cache = ign.createCache(new CacheConfiguration<>(cacheName)
            .setIndexedTypes(Integer.class, Integer.class)
            .setSqlFunctionClasses(TimedQueryHelper.class));

        Map<Integer, Integer> entries = IntStream.range(0, ROW_COUNT).boxed()
            .collect(Collectors.toMap(Function.identity(), Function.identity()));

        cache.putAll(entries);
    }

    public List<List<?>> executeQuery(Ignite ign) {
        long rowTimeout = executionTime / ROW_COUNT;

        SqlFieldsQuery qry = new SqlFieldsQuery("select longProcess(_val, " + rowTimeout + ") from Integer");

        return ign.cache(cacheName).query(qry).getAll();
    }

    public List<List<?>> executeQuery(Ignite ign, long timeout) {
        long rowTimeout = executionTime / ROW_COUNT;

        SqlFieldsQuery qry = new SqlFieldsQuery("select longProcess(_val, " + rowTimeout + ") from Integer")
            .setTimeout((int)timeout, TimeUnit.MILLISECONDS);

        return ign.cache(cacheName).query(qry).getAll();
    }

    @QuerySqlFunction
    public static int longProcess(int i, long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return i;
    }
}
