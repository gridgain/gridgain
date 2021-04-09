package org.apache.ignite.examples.cq;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;

import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class Client {
    public static void main(String[] args) throws Exception {
        try (Ignite client = Ignition.start(Cnf.getCfg("C1", true))) {
            IgniteCache<Integer, String> cache = client.getOrCreateCache("C");

            for (int i = 0; i < 100; i++) {
                cache.put(i, i + "_value");
            }

            ContinuousQuery<Integer, String> cq1 = new ContinuousQuery<>();
            cq1.setLocalListener(new LL());
            cq1.setRemoteFilterFactory(new RemoteFactory());
            cq1.setInitialQuery(new ScanQuery<>());
            cache.query(cq1).getAll();

            ContinuousQueryWithTransformer<Integer, String, String> cq2 = new ContinuousQueryWithTransformer<>();
            cq2.setLocalListener(new LLTrans());
            cq2.setRemoteFilterFactory(new RemoteFactory());
            cq2.setRemoteTransformerFactory(FactoryBuilder.factoryOf(new TransformClosureTrans()));
            cq2.setInitialQuery(new ScanQuery<>());
            cache.query(cq2).getAll();

            Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
            String url = "jdbc:ignite:thin://127.0.0.1/SYS";

            String q = "select CACHE_NAME, LOCAL_LISTENER, REMOTE_FILTER, REMOTE_TRANSFORMER, LOCAL_TRANSFORMED_LISTENER from CONTINUOUS_QUERIES where CACHE_NAME = 'C'";
            try (Connection conn = DriverManager.getConnection(url)) {
                ResultSet rs = conn.createStatement().executeQuery(q);

                ResultSetMetaData md = rs.getMetaData();
                for (int col = 0; col < md.getColumnCount(); col++) {
                    System.out.println(md.getColumnName(col + 1));
                }

                while (rs.next()) {
                    System.out.println(
                            rs.getString(1)
                                    + " | " + rs.getString(2)
                                    + " | " + rs.getString(3)
                                    + " | " + rs.getString(4)
                                    + " | " + rs.getString(5)
                    );
                }
            }
        }
    }

    public static class TransformClosureTrans implements IgniteClosure<CacheEntryEvent<? extends Integer, ? extends String>, String> {
        @Override public String apply(CacheEntryEvent<? extends Integer, ? extends String> event) {
            return event.getValue().length() + "";
        }
    }

    public static class LLTrans implements ContinuousQueryWithTransformer.EventListener<String> {
        @Override public void onUpdated(Iterable<? extends String> events) {
            System.out.println("LLT");
        }
    }

    public static class LL implements CacheEntryUpdatedListener<Integer, String> {
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> cacheEntryEvents) throws CacheEntryListenerException {
            System.out.println("LL");
        }
    }

    public static class RemoteFactory implements Factory<CacheEntryEventFilter<Integer, String>> {
        @Override public CacheEntryEventFilter<Integer, String> create() {
            return new EventFilter();
        }
    }

    public static class EventFilter implements CacheEntryEventFilter<Integer, String> {
        @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends String> event) throws CacheEntryListenerException {
            System.out.println("RFF");
            return true;
        }
    }

    public static class InitialPredicate implements IgniteBiPredicate<Integer, String> {
        @Override public boolean apply(Integer integer, String s) {
            return integer < 10;
        }
    }
}
