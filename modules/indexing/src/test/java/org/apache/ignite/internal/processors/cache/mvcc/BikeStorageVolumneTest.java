package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

public class BikeStorageVolumneTest extends GridCommonAbstractTest {
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    @After
    public void stopCluster() {
        stopAllGrids();
    }

    @Test
    public void populate() throws Exception {
        IgniteEx ign = startGrid(0);

        q("create table Person(id int primary key, name varchar, age int)");

        q("insert into Person values(0, 'ivan', 30)");

        String cacheName = "SQL_PUBLIC_PERSON";

        String typeName = ign.binary().types().stream()
            .filter(t -> t.typeName().contains(cacheName))
            .findAny().get().typeName();

        try (IgniteDataStreamer<Object, Object> ds = ign.dataStreamer(cacheName)) {
            for (int i = 1; i < 10_000_000; i++) {
                ds.addData(i, ign.binary().builder(typeName).build());
            }
        }

        q("select * from Person limit 10").forEach(System.err::println);
        System.err.println(q("select count(1) from Person"));

        TimeUnit.MINUTES.sleep(10);
    }

    private List<List<?>> q(String sql, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql).setArgs(args), false).getAll();
    }
}
