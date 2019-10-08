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

package org.apache.ignite.jdbc.thin;

import java.io.File;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import javax.cache.Cache.Entry;
import org.apache.commons.lang.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Result set test.
 */
public class JdbcTimeZoneMultiJvmTest extends JdbcThinAbstractSelfTest {
    /** */
    private static final String THICK_DRIVER_URL = "jdbc:ignite:cfg://cache=default@src/test/config/jdbc-config.xml";

    /** */
    private static final String THIN_DRIVER_URL = "jdbc:ignite:thin://localhost;schema=\"default\"";

    /** */
    private static final String THIN_DRIVER_URL_1 = "jdbc:ignite:thin://localhost:10801;schema=\"default\"";

    /** */
    private static final String THIN_DRIVER_URL_2 = "jdbc:ignite:thin://localhost:10802;schema=\"default\"";

    /** Timeszone for remote JVM. */
    private String remoteTz;

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /**
     * @return Additional JVM args for remote instances.
     */
    @Override protected List<String> additionalRemoteJvmArgs() {
        return remoteTz == null ? Collections.emptyList() : Collections.singletonList("-Duser.timezone=" + remoteTz);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setIndexedTypes(TemporalKey.class, TemporalObject.class);

        cfg.setCacheConfiguration(cache);

        return cfg;
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-23732")
    @Test
    public void testTimeZoneStabilityPojo() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(
                "CREATE TABLE temporal_ddl (node VARCHAR, clnt VARCHAR, tzname VARCHAR, types VARCHAR, " +
                    // "timetzval TIME WITH TIMEZONE, tstzval TIMESTAMP WITH TIMEZONE, "
                    "dateval DATE, timeval TIME, tsval TIMESTAMP, PRIMARY KEY (node, clnt, types, tzname));"));

            insertPojo("local", ignite);

            insertPojoNative("local", ignite);

            insertPojoJdbc("local", false, THIN_DRIVER_URL, new Properties());

            insertPojoJdbc("thick local", true, THICK_DRIVER_URL, new Properties());

            remoteTz = "EST5EDT";

            startGrid(1);

            insertPojoJdbc("local->remote", false, THIN_DRIVER_URL_1, new Properties());

            remoteTz = "IST";

            startGrid(2);

            ignite.compute(ignite.cluster().forRemotes()).broadcast(() -> insertPojo("remote", Ignition.localIgnite()));

            ignite.compute(ignite.cluster().forRemotes()).broadcast(() -> insertPojoNative("remote", Ignition.localIgnite()));

            ignite.compute(ignite.cluster().forRemotes()).broadcast(() -> insertPojoJdbc("remote", false, THIN_DRIVER_URL, new Properties()));

            ignite.compute(ignite.cluster().forRemotes()).broadcast(() -> insertPojoJdbc("thick remote", true, THICK_DRIVER_URL, new Properties()));

            List<T2<Boolean, String>> allResults = new ArrayList<>();

            for (List<T2<Boolean, String>> results : ignite.compute(ignite.cluster().forRemotes()).broadcast(
                () -> selectPojo("remote", Ignition.localIgnite())))
                allResults.addAll(results);

            for (List<T2<Boolean, String>> nativeResults : ignite.compute(ignite.cluster().forRemotes()).broadcast(
                () -> selectPojoNative("remote", Ignition.localIgnite())))
                allResults.addAll(nativeResults);

            for (List<T2<Boolean, String>> thinResults : ignite.compute(ignite.cluster().forRemotes()).broadcast(
                () -> selectPojoJdbc("remote", false, THIN_DRIVER_URL, new Properties())))
                allResults.addAll(thinResults);

            for (List<T2<Boolean, String>> thickResults : ignite.compute(ignite.cluster().forRemotes()).broadcast(
                () -> selectPojoJdbc("thick remote", true, THICK_DRIVER_URL, new Properties())))
                allResults.addAll(thickResults);

            allResults.addAll(selectPojo("local", ignite));

            allResults.addAll(selectPojoJdbc("local<-remote", false, THIN_DRIVER_URL_2, new Properties()));

            allResults.addAll(selectPojoNative("local", ignite));

            allResults.addAll(selectPojoJdbc("local", false, THIN_DRIVER_URL, new Properties()));

            allResults.addAll(selectPojoJdbc("thick local", true, THICK_DRIVER_URL, new Properties()));

            StringBuilder sb = new StringBuilder();

            boolean success = true;

            for (T2<Boolean, String> row : allResults) {
                success &= row.get1();

                sb.append(row.get2()).append("\n");
            }

            U.writeStringToFile(new File("ignite-pojo.csv"), sb.toString());

            if (!success) {
                System.err.println(sb.toString());

                fail("Not all cases passed, please see error output.");
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-23732")
    @Test
    public void testTimeZoneStabilityDml() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(
                "CREATE TABLE temporal_ddl (node VARCHAR, clnt VARCHAR, tzname VARCHAR, types VARCHAR, " +
                    // "timetzval TIME WITH TIMEZONE, tstzval TIMESTAMP WITH TIMEZONE, "
                    "dateval DATE, timeval TIME, tsval TIMESTAMP, PRIMARY KEY (node, clnt, types, tzname))" +
                    " WITH \"key_type=TemporalSqlKey, value_type=TemporalSqlValue\";"));

            insertDmlBinary("local", ignite);

            insertDmlNative("local", ignite);

            insertDmlJdbc("local", THIN_DRIVER_URL, new Properties());

            insertDmlJdbc("thick local", THICK_DRIVER_URL, new Properties());

            remoteTz = "EST5EDT";

            startGrid(1);

            remoteTz = "IST";

            startGrid(2);

            insertDmlJdbc("local->remote", THIN_DRIVER_URL_2, new Properties());

            ignite.compute(ignite.cluster().forRemotes()).broadcast(() -> insertDmlBinary("remote", Ignition.localIgnite()));

            ignite.compute(ignite.cluster().forRemotes()).broadcast(() -> insertDmlNative("remote", Ignition.localIgnite()));

            ignite.compute(ignite.cluster().forRemotes()).broadcast(() -> insertDmlJdbc("remote", THIN_DRIVER_URL, new Properties()));

            ignite.compute(ignite.cluster().forRemotes()).broadcast(() -> insertDmlJdbc("thick remote", THICK_DRIVER_URL, new Properties()));

            List<T2<Boolean, String>> allResults = new ArrayList<>();

            for (List<T2<Boolean, String>> nativeResults : ignite.compute(ignite.cluster().forRemotes()).broadcast(
                () -> selectDmlNative("remote", Ignition.localIgnite())))
                allResults.addAll(nativeResults);

            for (List<T2<Boolean, String>> thinResults : ignite.compute(ignite.cluster().forRemotes()).broadcast(
                () -> selectDmlJdbc("remote", THIN_DRIVER_URL, new Properties())))
                allResults.addAll(thinResults);

            for (List<T2<Boolean, String>> thickResults : ignite.compute(ignite.cluster().forRemotes()).broadcast(
                () -> selectDmlJdbc("thick remote", THICK_DRIVER_URL, new Properties())))
                allResults.addAll(thickResults);

            for (List<T2<Boolean, String>> binResults : ignite.compute(ignite.cluster().forRemotes()).broadcast(
                () -> selectDmlBinary("remote", Ignition.localIgnite())))
                allResults.addAll(binResults);

            allResults.addAll(selectDmlNative("local", ignite));

            allResults.addAll(selectDmlJdbc("local", THIN_DRIVER_URL, new Properties()));

            allResults.addAll(selectDmlJdbc("local<-remote", THIN_DRIVER_URL_1, new Properties()));

            allResults.addAll(selectDmlJdbc("thick local", THICK_DRIVER_URL, new Properties()));

            allResults.addAll(selectDmlBinary("local", ignite));

            StringBuilder sb = new StringBuilder();

            boolean success = true;

            for (T2<Boolean, String> row : allResults) {
                success &= row.get1();

                sb.append(row.get2()).append("\n");
            }

            U.writeStringToFile(new File("ignite-dml.csv"), sb.toString());

            if (!success) {
                System.err.println(sb.toString());

                fail("Not all cases passed, please see error output.");
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    @Ignore
    @Test
    public void testTimeZoneStabilitySqlite() throws Exception {
        timeZoneStabilityForeignDb("jdbc:sqlite:sample.db", new Properties());
    }

    /** */
    @Ignore
    @Test
    public void testTimeZoneStabilityMysql() throws Exception {
        timeZoneStabilityForeignDb("jdbc:mysql://localhost:3306/tz", new Properties() {{
            setProperty("user", "gridgain");
            setProperty("password", "Secret.ly");
        }});
    }

    /** */
    @Ignore
    @Test
    public void testTimeZoneStabilityMsSqlServer() throws Exception {
        // Needs minor code changes to run
        timeZoneStabilityForeignDb("jdbc:sqlserver://localhost:1433;user=SA;password=Secret.ly;",
            new Properties());
    }

    /** */
    @Ignore
    @Test
    public void testTimeZoneStabilityThickClient() throws Exception {
        timeZoneStabilityForeignDb("jdbc:ignite:cfg://src/test/config/jdbc-config.xml", new Properties());
    }

    /** */
    @Ignore
    @Test
    public void testTimeZoneStabilityOracle() throws Exception {
        // Needs several code changes to run
        timeZoneStabilityForeignDb("jdbc:oracle:thin:ADMIN/gridgain@lab32:1521:TEST", new Properties());
    }

    /** */
    @Ignore
    @Test
    public void testTimeZoneStabilityPostgreSql() throws Exception {
        timeZoneStabilityForeignDb("jdbc:postgresql://127.0.0.1:5432/tz", new Properties() {{
            setProperty("user", "tz");
            setProperty("password", "Secret.ly");
        }});
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-24020")
    @Test
    public void testThinDriverJsr310Types() throws Exception {
        try {
            startGrid(0);

            createJdbc(THIN_DRIVER_URL, new Properties());

            try (Connection conn = DriverManager.getConnection(THIN_DRIVER_URL, new Properties())) {
                try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO temporal_ddl (node, clnt, types, tzname, dateval, timeval, tsval) " +
                        "VALUES ('local', 'jdbc', 'jsr310', 'UTC', ?, ?, ?);")) {
                    ps.setObject(1, jsr310Date());
                    ps.setObject(2, jsr310Time());
                    ps.setObject(3, jsr310DateTime());

                    ps.execute();
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-24135")
    @Test
    public void testThickDriverWrongType() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            try (Connection conn = DriverManager.getConnection(THICK_DRIVER_URL, new Properties())) {
                try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO TemporalObject (node, clnt, types, tzname, dateVal, timeVal, tsVal, locDateVal, " +
                        "locTimeVal, locDateTimeVal) VALUES ('local', 'jdbc', 'good', 'UTC', ?, ?, ?, ?, ?, ?);")) {
                    ps.setDate(1, modernDate());
                    ps.setTime(2, modernTime());
                    ps.setTimestamp(3, modernTs());
                    ps.setDate(4, modernDate());
                    ps.setTime(5, modernTime());
                    ps.setTimestamp(6, modernTs());

                    ps.execute();
                }

                try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO TemporalObject (node, clnt, types, tzname, dateVal, timeVal, tsVal, locDateVal, " +
                        "locTimeVal, locDateTimeVal) VALUES ('local', 'jdbc', 'modern', 'UTC', ?, ?, ?, ?, ?, ?);")) {
                    ps.setObject(1, modernDate());
                    ps.setObject(2, modernTime());
                    ps.setObject(3, modernTs());
                    ps.setObject(4, modernDate());
                    ps.setObject(5, modernTime());
                    ps.setObject(6, modernTs());

                    ps.execute();
                }

                try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO TemporalObject (node, clnt, types, tzname, dateVal, timeVal, tsVal, locDateVal, " +
                        "locTimeVal, locDateTimeVal) VALUES ('local', 'jdbc', 'jsr310', 'UTC', ?, ?, ?, ?, ?, ?);")) {
                    ps.setObject(1, jsr310Date());
                    ps.setObject(2, jsr310Time());
                    ps.setObject(3, jsr310DateTime());
                    ps.setObject(4, jsr310Date());
                    ps.setObject(5, jsr310Time());
                    ps.setObject(6, jsr310DateTime());

                    ps.execute();
                }
            }

            for (Entry e : ignite.cache(DEFAULT_CACHE_NAME));
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-24135")
    @Test
    public void testNativeWrongType() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(
                "INSERT INTO TemporalObject (node, clnt, types, tzname, dateval, timeval, tsval, locDateVal, " +
                    "locTimeVal, locDateTimeVal) VALUES ('local', 'native', 'legacy', 'UTC', ?, ?, ?, ?, ?, ?);")
                .setArgs(legacyDate(), legacyTime(), legacyTs(), legacyDate(), legacyTime(), legacyTs()));

            ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(
                "INSERT INTO TemporalObject (node, clnt, types, tzname, dateval, timeval, tsval, locDateVal, " +
                    "locTimeVal, locDateTimeVal) VALUES ('local', 'native', 'modern', 'UTC', ?, ?, ?, ?, ?, ?);")
                .setArgs(modernDate(), modernTime(), modernTs(), jsr310Date(), jsr310Time(), jsr310DateTime()));

            ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(
                "INSERT INTO TemporalObject (node, clnt, types, tzname, dateval, timeval, tsval, locDateVal, " +
                    "locTimeVal, locDateTimeVal) VALUES ('local', 'native', 'bad-jsr310', 'UTC', ?, ?, ?, ?, ?, ?);")
                .setArgs(jsr310Date(), jsr310Time(), jsr310DateTime(), jsr310Date(), jsr310Time(), jsr310DateTime()));

            for (Entry e : ignite.cache(DEFAULT_CACHE_NAME));
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-24134")
    @Test
    public void testThickDriverLiteralJsr310() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            try (Connection conn = DriverManager.getConnection(THICK_DRIVER_URL, new Properties())) {
                try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO TemporalObject (node, clnt, types, tzname, dateVal, timeVal, tsVal, locDateVal, " +
                        "locTimeVal, locDateTimeVal) VALUES ('local', 'jdbc', 'good', 'UTC', " +
                        "'2019-09-09', '09:09:09', '2019-09-09 09:09:09.090900000', " +
                        "'2019-09-09', '09:09:09', '2019-09-09 09:09:09.090900000');")) {

                    ps.execute();
                }
            }

            for (Entry e : ignite.cache(DEFAULT_CACHE_NAME));
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-24134")
    @Test
    public void testNativeLiteralJsr310() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(
                "INSERT INTO TemporalObject (node, clnt, types, tzname, dateval, timeval, tsval, locDateVal, " +
                    "locTimeVal, locDateTimeVal) VALUES ('local', 'native', 'legacy', 'UTC', " +
                    "'2019-09-09', '09:09:09', '2019-09-09 09:09:09.090900000', " +
                    "'2019-09-09', '09:09:09', '2019-09-09 09:09:09.090900000');"));

            for (Entry e : ignite.cache(DEFAULT_CACHE_NAME));
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private void timeZoneStabilityForeignDb(String connStr, Properties props) throws Exception {
        Ignite ignite = startGrid(0);

        try {
            createJdbc(connStr, props);

            insertDmlJdbc("local", connStr, props);

            remoteTz = "EST5EDT";

            startGrid(1);

            remoteTz = "Asia/Kolkata";

            startGrid(2);

            ignite.compute(ignite.cluster().forRemotes()).broadcast(() -> insertDmlJdbc("remote", connStr, props));

            Collection<List<T2<Boolean, String>>> results = ignite.compute(ignite.cluster().forRemotes()).broadcast(
                () -> selectDmlJdbc("remote", connStr, props));

            List<T2<Boolean, String>> localResult = selectDmlJdbc("local", connStr, props);

            StringBuilder sb = new StringBuilder();

            boolean success = true;

            for (T2<Boolean, String> row : localResult) {
                success &= row.get1();

                sb.append(row.get2()).append("\n");
            }

            for (List<T2<Boolean, String>> result : results) {
                for (T2<Boolean, String> row : result) {
                    success &= row.get1();

                    sb.append(row.get2()).append("\n");
                }
            }

            U.writeStringToFile(new File(connStr.replaceAll("[^a-zA-Z0-9]+", "_") + ".csv"),
                sb.toString());

            if (!success) {
                System.err.println(sb.toString());

                fail("Not all cases passed, please see error output.");
            }
        }
        finally {
            try {
                dropJdbc(connStr, props);
            }
            catch (Exception e) {
                log.warning("Cleanup failed", e);
            }

            stopAllGrids();
        }
    }

    /** */
    private void createJdbc(String connStr, Properties props) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connStr, props);
             Statement statement = conn.createStatement()) {
            statement.executeUpdate(
                "CREATE TABLE temporal_ddl (node VARCHAR(40), clnt VARCHAR(40), tzname VARCHAR(40), types VARCHAR(40), " +
                    // "timetzval TIME WITH TIMEZONE, tstzval TIMESTAMP WITH TIMEZONE, "
                    // datetime for MSSQL
                    "dateval DATE, timeval TIME, tsval TIMESTAMP, PRIMARY KEY (node, clnt, types, tzname))");
        }
    }

    /** */
    private void dropJdbc(String connStr, Properties props) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connStr, props);
             Statement statement = conn.createStatement()) {
            statement.executeUpdate("DROP TABLE temporal_ddl");
        }
    }

    /** */
    private List<T2<Boolean, String>> selectDmlJdbc(String node, String connStr, Properties props) {
        try (Connection conn = DriverManager.getConnection(connStr, props);
             PreparedStatement ps = conn.prepareStatement("SELECT '" + node + "', 'jdbc', '" +
                 tzname() + "', td.* FROM temporal_ddl td ORDER BY types, tzname, node, clnt");
             ResultSet rs = ps.executeQuery()) {
            List<T2<Boolean, String>> result = new ArrayList<>();

            while (rs.next()) {
                List l = new ArrayList();

                for (int i = 1; i <= 7; i++)
                    l.add(rs.getObject(i));

                l.add(rs.getDate(8));
                l.add(rs.getObject(8));
//              For Sqlite's broken driver, instead:
//              l.add("");
                l.add(rs.getTime(9));
                l.add(rs.getObject(9));
//              l.add("");
                l.add(rs.getTimestamp(10));
                l.add(rs.getObject(10));
//              l.add("");

                result.add(checkDmlRow(l));
            }

            return result;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private List<T2<Boolean, String>> selectDmlNative(String node, Ignite ignite) {
        List<T2<Boolean, String>> result = new ArrayList<>();

        for (List l : ignite.cache(DEFAULT_CACHE_NAME).query(
            new SqlFieldsQuery("SELECT '" + node + "', 'native', '" + tzname() + "', " +
                "node, clnt, types, tzname, dateval, '', timeval, '', tsval, '' " +
                "FROM temporal_ddl ORDER BY types, tzname, node, clnt")).getAll())
            result.add(checkDmlRow(l));

        return result;
    }

    /** */
    private List<T2<Boolean, String>> selectDmlBinary(String node, Ignite ignite) {
        IgniteCache cache = null;

        for (String cacheName : ignite.cacheNames()) {
            cache = ignite.cache(cacheName);

            CacheConfiguration<Object, Object> cfg = (CacheConfiguration)cache.getConfiguration(CacheConfiguration.class);

            for (QueryEntity e : cfg.getQueryEntities()) {
                if ("temporal_ddl".equalsIgnoreCase(e.getTableName()))
                    break;
            }
        }

        IgniteCache<BinaryObject, BinaryObject> binaryCache = cache.withKeepBinary();

        List<T2<Boolean, String>> result = new ArrayList<>();

        for (Entry<BinaryObject, BinaryObject> entry : binaryCache) {
            List l = new ArrayList();

            l.add(node);
            l.add("binary");
            l.add(tzname());
            l.add(entry.getKey().field("node"));
            l.add(entry.getKey().field("clnt"));
            l.add(entry.getKey().field("types"));
            l.add(entry.getKey().field("tzname"));
            l.add(entry.getValue().field("dateval"));
            l.add("");
            l.add(entry.getValue().field("timeval"));
            l.add("");
            l.add(entry.getValue().field("tsval"));
            l.add("");

            result.add(checkDmlRow(l));
        }

        return result;
    }

    /** */
    private void insertDmlBinary(String node, Ignite ignite) {
        IgniteCache cache = null;

        for (String cacheName : ignite.cacheNames()) {
            cache = ignite.cache(cacheName);

            CacheConfiguration<Object, Object> cfg = (CacheConfiguration)cache.getConfiguration(CacheConfiguration.class);

            for (QueryEntity e : cfg.getQueryEntities()) {
                if ("temporal_ddl".equalsIgnoreCase(e.getTableName()))
                    break;
            }
        }

        IgniteCache<BinaryObject, BinaryObject> binaryCache = cache.withKeepBinary();

        binaryCache.put(
            ignite.binary().builder("TemporalSqlKey")
                .setField("node", node)
                .setField("clnt", "binary")
                .setField("types", "legacy")
                .setField("tzname", tzname()).build(),
            ignite.binary().builder("TemporalSqlValue")
                .setField("dateval", legacyDate())
                .setField("timeval", legacyTime())
                .setField("tsval", legacyTs()).build());

        binaryCache.put(
            ignite.binary().builder("TemporalSqlKey")
                .setField("node", node)
                .setField("clnt", "binary")
                .setField("types", "modern")
                .setField("tzname", tzname()).build(),
            ignite.binary().builder("TemporalSqlValue")
                .setField("dateval", modernDate())
                .setField("timeval", modernTime())
                .setField("tsval", modernTs()).build());
    }

    /** */
    private void insertDmlJdbc(String node, String connStr, Properties props) {
        try (Connection conn = DriverManager.getConnection(connStr, props)) {
            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO temporal_ddl (node, clnt, types, tzname, dateval, timeval, tsval) " +
                    "VALUES (?, 'jdbc', 'literal', ?, '2019-09-09', '09:09:09', '2019-09-09 09:09:09.090');")) {
                ps.setString(1, node);
                ps.setString(2, tzname());

                ps.execute();
            }

            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO temporal_ddl (node, clnt, types, tzname, dateval, timeval, tsval) " +
                    "VALUES (?, 'jdbc', 'legacy', ?, ?, ?, ?)")) {
                ps.setString(1, node);
                ps.setString(2, tzname());

                ps.setDate(3, legacyDate());
                ps.setTime(4, legacyTime());
                ps.setTimestamp(5, legacyTs());

                ps.execute();
            }

            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO temporal_ddl (node, clnt, types, tzname, dateval, timeval, tsval) " +
                    "VALUES (?, 'jdbc', 'calendarized', ?, ?, ?, ?)")) {
                ps.setString(1, node);
                ps.setString(2, tzname());

                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("Europe/Moscow"));

                ps.setDate(3, legacyDate(), calendar);
                ps.setTime(4, legacyTime(), calendar);
                ps.setTimestamp(5, legacyTs(), calendar);

                ps.execute();
            }

            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO temporal_ddl (node, clnt, types, tzname, dateval, timeval, tsval) " +
                    "VALUES (?, 'jdbc', 'modern', ?, ?, ?, ?)")) {
                ps.setString(1, node);
                ps.setString(2, tzname());

                ps.setDate(3, modernDate());
                ps.setTime(4, modernTime());
                ps.setTimestamp(5, modernTs());

                ps.execute();
            }

            if (!connStr.startsWith("jdbc:ignite:thin:")) {
                try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO temporal_ddl (node, clnt, types, tzname, dateval, timeval, tsval) " +
                        "VALUES (?, 'jdbc', 'JSR310', ?, ?, ?, ?)")) {
                    ps.setString(1, node);
                    ps.setString(2, tzname());

                    ps.setObject(3, jsr310Date());
                    ps.setObject(4, jsr310Time());
                    ps.setObject(5, jsr310DateTime());

                    ps.execute();
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private void insertDmlNative(String node, Ignite ignite) {
        ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(
            "INSERT INTO temporal_ddl (node, clnt, types, tzname, dateval, timeval, tsval) " +
                "VALUES ('" + node + "', 'native', 'literal', '" + tzname() + "', " +
                "'2019-09-09', '09:09:09', '2019-09-09 09:09:09.090900000');"));

        ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(
            "INSERT INTO temporal_ddl (node, clnt, types, tzname, dateval, timeval, tsval) " +
                "VALUES ('" + node + "', 'native', 'legacy', '" + tzname() + "', ?, ?, ?);")
            .setArgs(legacyDate(), legacyTime(), legacyTs()));

        ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(
            "INSERT INTO temporal_ddl (node, clnt, types, tzname, dateval, timeval, tsval) " +
                "VALUES ('" + node + "', 'native', 'modern', '" + tzname() + "', ?, ?, ?);")
            .setArgs(modernDate(), modernTime(), modernTs()));

        ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(
            "INSERT INTO temporal_ddl (node, clnt, types, tzname, dateval, timeval, tsval) " +
                "VALUES ('" + node + "', 'native', 'JSR310', '" + tzname() + "', ?, ?, ?);")
            .setArgs(jsr310Date(), jsr310Time(), jsr310DateTime()));
    }

    /** */
    private void insertPojo(String node, Ignite ignite) {
        IgniteCache c = ignite.cache(DEFAULT_CACHE_NAME);

        {
            TemporalKey tk = new TemporalKey();
            TemporalObject to = new TemporalObject();

            tk.node = node;
            tk.clnt = "cache";
            tk.tzname = tzname();
            tk.types = "legacy";

            to.dateVal = legacyDate();
            to.timeVal = legacyTime();
            to.tsVal = legacyTs();
            to.instantVal = Instant.ofEpochMilli(to.tsVal.getTime());
            to.locDateVal = jsr310Date();
            to.locTimeVal = jsr310Time();
            to.locDateTimeVal = jsr310DateTime();

            c.put(tk, to);
        }

        {
            TemporalKey tk = new TemporalKey();
            TemporalObject to = new TemporalObject();

            tk.node = node;
            tk.clnt = "cache";
            tk.tzname = tzname();
            tk.types = "modern";

            to.dateVal = modernDate();
            to.timeVal = modernTime();
            to.tsVal = modernTs();
            to.instantVal = Instant.ofEpochMilli(to.tsVal.getTime());
            to.locDateVal = jsr310Date();
            to.locTimeVal = jsr310Time();
            to.locDateTimeVal = jsr310DateTime();

            c.put(tk, to);
        }
    }

    /** */
    private void insertPojoJdbc(String node, boolean jsr310, String connStr, Properties props) {
        try (Connection conn = DriverManager.getConnection(connStr, props)) {
            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO TemporalObject (node, clnt, types, tzname, dateVal, timeVal, tsVal) VALUES (" +
                    "?, 'jdbc', 'literal', ?, '2019-09-09', '09:09:09', '2019-09-09 09:09:09.090900000');")) {
                ps.setString(1, node);
                ps.setString(2, tzname());

                ps.execute();
            }

            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO TemporalObject (node, clnt, types, tzname, dateVal, timeVal, tsVal, instantVal, " +
                    "locDateVal, locTimeVal, locDateTimeVal) VALUES (?, 'jdbc', 'legacy', ?, ?, ?, ?, ?, ?, ?, ?)")) {
                ps.setString(1, node);
                ps.setString(2, tzname());

                ps.setDate(3, legacyDate());
                ps.setTime(4, legacyTime());
                ps.setTimestamp(5, legacyTs());

                if (jsr310) {
                    ps.setObject(6, Instant.ofEpochMilli(legacyTs().getTime()));
                    ps.setDate(7, legacyDate());
                    ps.setTime(8, legacyTime());
                    ps.setTimestamp(9, legacyTs());
                }
                else {
                    ps.setObject(6, null);
                    ps.setObject(7, null);
                    ps.setObject(8, null);
                    ps.setObject(9, null);
                }

                ps.execute();
            }

            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO TemporalObject (node, clnt, types, tzname, dateVal, timeVal, tsVal, instantVal, " +
                    "locDateVal, locTimeVal, locDateTimeVal) VALUES (?, 'jdbc', 'calendarized', ?, ?, ?, ?, ?, ?, ?, ?)")) {
                ps.setString(1, node);
                ps.setString(2, tzname());

                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("Europe/Moscow"));

                ps.setDate(3, legacyDate(), calendar);
                ps.setTime(4, legacyTime(), calendar);
                ps.setTimestamp(5, legacyTs(), calendar);

                if (jsr310) {
                    ps.setObject(6, Instant.ofEpochMilli(legacyTs().getTime()));
                    ps.setDate(7, legacyDate(), calendar);
                    ps.setTime(8, legacyTime(), calendar);
                    ps.setTimestamp(9, legacyTs(), calendar);
                }
                else {
                    ps.setObject(6, null);
                    ps.setObject(7, null);
                    ps.setObject(8, null);
                    ps.setObject(9, null);
                }

                ps.execute();
            }

            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO TemporalObject (node, clnt, types, tzname, dateVal, timeVal, tsVal, instantVal, " +
                    "locDateVal, locTimeVal, locDateTimeVal) VALUES (?, 'jdbc', 'modern', ?, ?, ?, ?, ?, ?, ?, ?)")) {
                ps.setString(1, node);
                ps.setString(2, tzname());

                ps.setDate(3, modernDate());
                ps.setTime(4, modernTime());
                ps.setTimestamp(5, modernTs());

                if (jsr310) {
                    ps.setObject(6, Instant.ofEpochMilli(legacyTs().getTime()));
                    ps.setDate(7, modernDate());
                    ps.setTime(8, modernTime());
                    ps.setTimestamp(9, modernTs());
                }
                else {
                    ps.setObject(6, null);
                    ps.setObject(7, null);
                    ps.setObject(8, null);
                    ps.setObject(9, null);
                }

                ps.execute();
            }

            if (jsr310) {
                try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO TemporalObject (node, clnt, types, tzname, dateVal, timeVal, tsVal, instantVal, " +
                        "locDateVal, locTimeVal, locDateTimeVal) VALUES (?, 'jdbc', 'jsr310', ?, ?, ?, ?, ?, ?, ?, ?)")) {
                    ps.setString(1, node);
                    ps.setString(2, tzname());

                    ps.setDate(3, modernDate());
                    ps.setTime(4, modernTime());
                    ps.setTimestamp(5, modernTs());
                    ps.setObject(6, Instant.ofEpochMilli(legacyTs().getTime()));
                    ps.setObject(7, jsr310Date());
                    ps.setObject(8, jsr310Time());
                    ps.setObject(9, jsr310DateTime());

                    ps.execute();
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private void insertPojoNative(String node, Ignite ignite) {
        ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(
            "INSERT INTO TemporalObject (node, clnt, types, tzname, dateval, timeval, tsval) VALUES " +
                "('" + node + "', 'native', 'literal', '" + tzname() + "', " +
                "'2019-09-09', '09:09:09', '2019-09-09 09:09:09.090900000');"));

        ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(
            "INSERT INTO TemporalObject (node, clnt, types, tzname, dateval, timeval, tsval, instantVal, locDateVal, " +
                "locTimeVal, locDateTimeVal) VALUES ('" + node + "', 'native', 'legacy', '" + tzname() + "', ?, ?, ?, ?, ?, ?, ?);")
            .setArgs(legacyDate(), legacyTime(), legacyTs(), Instant.ofEpochMilli(legacyTs().getTime()),
                legacyDate(), legacyTime(), legacyTs()));

        ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(
            "INSERT INTO TemporalObject (node, clnt, types, tzname, dateval, timeval, tsval, instantVal, locDateVal, " +
                "locTimeVal, locDateTimeVal) VALUES ('" + node + "', 'native', 'modern', '" + tzname() + "', ?, ?, ?, ?, ?, ?, ?);")
            .setArgs(modernDate(), modernTime(), modernTs(), Instant.ofEpochMilli(legacyTs().getTime()),
                modernDate(), modernTime(), modernTs()));

        ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(
            "INSERT INTO TemporalObject (node, clnt, types, tzname, dateval, timeval, tsval, instantVal, locDateVal, " +
                "locTimeVal, locDateTimeVal) VALUES ('" + node + "', 'native', 'jsr310', '" + tzname() + "', ?, ?, ?, ?, ?, ?, ?);")
            .setArgs(modernDate(), modernTime(), modernTs(), Instant.ofEpochMilli(legacyTs().getTime()),
                jsr310Date(), jsr310Time(), jsr310DateTime()));
    }

    /** */
    private List<T2<Boolean, String>> selectPojoJdbc(String node, boolean instant, String connStr, Properties props) {
        try (Connection conn = DriverManager.getConnection(connStr, props);
             PreparedStatement ps = conn.prepareStatement("SELECT '" + node + "', 'jdbc', '" +
                 tzname() + "', node, clnt, tzname, types, dateVal, timeVal, tsVal, locDateVal, locTimeVal, locDateTimeVal, " +
                 (instant ? "instantVal" : "''") + " FROM TemporalObject ORDER BY types, tzname, node, clnt");
             ResultSet rs = ps.executeQuery()) {
            List<T2<Boolean, String>> result = new ArrayList<>();

            while (rs.next()) {
                List l = new ArrayList();

                for (int i = 1; i <= 7; i++)
                    l.add(rs.getObject(i));

                l.add(rs.getDate(8));
                l.add(rs.getObject(8));
                l.add(rs.getTime(9));
                l.add(rs.getObject(9));
                l.add(rs.getTimestamp(10));
                l.add(rs.getObject(10));
                l.add(rs.getDate(11));
                l.add(rs.getObject(11));
                l.add(rs.getTime(12));
                l.add(rs.getObject(12));
                l.add(rs.getTimestamp(13));
                l.add(rs.getObject(13));
                l.add(rs.getObject(14));

                result.add(checkPojoRow(l));
            }

            return result;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private List<T2<Boolean, String>> selectPojoNative(String node, Ignite ignite) {
        List<T2<Boolean, String>> result = new ArrayList<>();

        for (List l : ignite.cache(DEFAULT_CACHE_NAME).query(
            new SqlFieldsQuery("SELECT '" + node + "', 'native', '" + tzname() + "', " +
                "node, clnt, types, tzname, dateVal, '', timeVal, '', tsVal, '' " +
                "locDateVal, '', locTimeVal, '', locDateTimeVal, '', instantVal " +
                "FROM TemporalObject ORDER BY types, tzname, node, clnt")).getAll())
            result.add(checkPojoRow(l));

        return result;
    }

    /** */
    private List<T2<Boolean, String>> selectPojo(String node, Ignite ignite) {
        IgniteCache<TemporalKey, TemporalObject> cache = ignite.cache(DEFAULT_CACHE_NAME);

        List<T2<Boolean, String>> result = new ArrayList<>();

        for (Entry<TemporalKey, TemporalObject> entry : cache) {
            List l = new ArrayList();

            l.add(node);
            l.add("pojo");
            l.add(tzname());
            l.add(entry.getKey().node);
            l.add(entry.getKey().clnt);
            l.add(entry.getKey().types);
            l.add(entry.getKey().tzname);
            l.add(entry.getValue().dateVal);
            l.add("");
            l.add(entry.getValue().timeVal);
            l.add("");
            l.add(entry.getValue().tsVal);
            l.add("");
            l.add(entry.getValue().locDateVal);
            l.add("");
            l.add(entry.getValue().locTimeVal);
            l.add("");
            l.add(entry.getValue().locDateTimeVal);
            l.add("");
            l.add(entry.getValue().instantVal);

            result.add(checkPojoRow(l));
        }

        return result;
    }

    /** */
    private T2<Boolean, String> checkDmlRow(List<Object> row) {
        boolean result;

        boolean calendarized = row.get(6).equals("calendarized");
        String remoteTz = row.get(5).toString();

        if (!calendarized || remoteTz.equals("Europe/Moscow"))
            result = checkDmlValues(row, "2019-09-09", "09:09:09", "2019-09-09", "09:09:09");
        else if (remoteTz.equals("EST5EDT"))
            result = checkDmlValues(row, "2019-09-09", "17:09:09", "2019-09-09", "16:09:09");
        else
            result = checkDmlValues(row, "2019-09-08", "06:39:09", "2019-09-09", "06:39:09");

        return new T2(result, (result ? "correct" : "INVALID") + "\t" + StringUtils.join(row, "\t"));
    }

    /** */
    private T2<Boolean, String> checkPojoRow(List<Object> row) {
        boolean result;

        boolean calendarized = row.get(6).equals("calendarized");
        String remoteTz = row.get(5).toString();

        if (!calendarized || remoteTz.equals("Europe/Moscow"))
            result = checkPojoValues(row, "2019-09-09", "09:09:09", "2019-09-09", "09:09:09");
        else if (remoteTz.equals("EST5EDT"))
            result = checkPojoValues(row, "2019-09-09", "17:09:09", "2019-09-09", "16:09:09");
        else
            result = checkPojoValues(row, "2019-09-08", "06:39:09", "2019-09-09", "06:39:09");

        return new T2(result, (result ? "correct" : "INVALID") + "\t" + StringUtils.join(row, "\t"));
    }

    /** */
    private boolean checkDmlValues(List<Object> row, String expectDate, String expectTime,
        String expectTsDate, String expectTsTime) {
        try {
            assertEquals(expectDate, row.get(7).toString());

            //@Ignore("https://ggsystems.atlassian.net/browse/GG-23665")
            //if (isNotBlank(row.get(8)))
            //    assertEquals(expectDate, row.get(8).toString());

            assertEquals(expectTime, row.get(9).toString());

            if (isNotBlank(row.get(10)))
                assertEquals(expectTime, row.get(10).toString());

            assertEquals(expectTsDate, row.get(11).toString().split("[ .]")[0]);
            assertEquals(expectTsTime, row.get(11).toString().split("[ .]")[1]);
            assertTrue(row.get(11).toString().split("[ .]")[2].matches("^(9.|09)[0-9]*"));

            if (isNotBlank(row.get(12))) {
                assertEquals(expectTsDate, row.get(12).toString().split("[ .]")[0]);
                assertEquals(expectTsTime, row.get(12).toString().split("[ .]")[1]);
                assertTrue(row.get(12).toString().split("[ .]")[2].matches("^(9.|09)[0-9]*"));
            }

            return true;
        } catch (AssertionError ae) {
            ae.printStackTrace();

            return false;
        }
    }

    /** */
    private boolean checkPojoValues(List<Object> row, String expectDate, String expectTime,
        String expectTsDate, String expectTsTime) {
        try {
            assertEquals(expectDate, row.get(7).toString());

            //@Ignore("https://ggsystems.atlassian.net/browse/GG-23665")
            //if (isNotBlank(row.get(8)))
            //    assertEquals(expectDate, row.get(8).toString());

            assertEquals(expectTime, row.get(9).toString());

            if (isNotBlank(row.get(10)))
                assertEquals(expectTime, row.get(10).toString());

            assertEquals(expectTsDate, row.get(11).toString().split("[ .]")[0]);
            assertEquals(expectTsTime, row.get(11).toString().split("[ .]")[1]);
            assertTrue(row.get(11).toString().split("[ .]")[2].matches("^(9.|09)[0-9]*"));

            if (isNotBlank(row.get(12))) {
                assertEquals(expectTsDate, row.get(12).toString().split("[ .]")[0]);
                assertEquals(expectTsTime, row.get(12).toString().split("[ .]")[1]);
                assertTrue(row.get(12).toString().split("[ .]")[2].matches("^(9.|09)[0-9]*"));
            }

            if (isNotBlank(row.get(13)))
                assertEquals(expectDate, row.get(13).toString());

            //@Ignore("https://ggsystems.atlassian.net/browse/GG-23665")
            //if (isNotBlank(row.get(14).toString()))
            //    assertEquals(expectDate, row.get(14).toString());

            if (isNotBlank(row.get(15)))
                assertEquals(expectTime, row.get(15).toString());

            if (isNotBlank(row.get(16)))
                assertEquals(expectTime, row.get(16).toString());

            if (isNotBlank(row.get(17))) {
                assertEquals(expectTsDate, row.get(17).toString().split("[ .]")[0]);
                assertEquals(expectTsTime, row.get(17).toString().split("[ .]")[1]);
                assertTrue(row.get(17).toString().split("[ .]")[2].matches("^(9.|09)[0-9]*"));
            }

            if (isNotBlank(row.get(18))) {
                assertEquals(expectTsDate, row.get(18).toString().split("[ .]")[0]);
                assertEquals(expectTsTime, row.get(18).toString().split("[ .]")[1]);
                assertTrue(row.get(18).toString().split("[ .]")[2].matches("^(9.|09)[0-9]*"));
            }

            // XXX no idea how to handle Instant
            //if (isNotBlank(row.get(19).toString())) {
            //    assertEquals(expectTsDate, row.get(19).toString().split("[ .]")[0]);
            //    assertEquals(expectTsTime, row.get(19).toString().split("[ .]")[1]);
            //    assertTrue(row.get(19).toString().split("[ .]")[2].matches("^(9.|09)[0-9]*"));
            //}

            return true;
        } catch (AssertionError ae) {
            ae.printStackTrace();

            return false;
        }
    }

    /** */
    private LocalDate jsr310Date() {
        return LocalDate.of(2019, Month.SEPTEMBER, 9);
    }

    /** */
    private LocalTime jsr310Time() {
        return LocalTime.of(9, 9, 9);
    }

    /** */
    private LocalDateTime jsr310DateTime() {
        return LocalDateTime.of(2019, 9, 9, 9, 9, 9, 90900000);
    }

    /** */
    private Date modernDate() {
        Calendar datecal = Calendar.getInstance();
        datecal.set(2019, 8, 9);

        return new Date(datecal.getTimeInMillis());
    }

    /** */
    private Time modernTime() {
        Calendar timecal = Calendar.getInstance();

        timecal.set(Calendar.HOUR_OF_DAY, 9);
        timecal.set(Calendar.MINUTE, 9);
        timecal.set(Calendar.SECOND, 9);

        return new Time(timecal.getTimeInMillis());
    }

    /** */
    private Timestamp modernTs() {
        Calendar tscal = Calendar.getInstance();

        tscal.set(2019, 8, 9, 9, 9, 9);
        tscal.set(Calendar.MILLISECOND, 909);

        return new Timestamp(tscal.getTimeInMillis());
    }

    /** */
    private Date legacyDate() {
        return new Date(119, 8, 9);
    }

    /** */
    private Time legacyTime() {
        return new Time(9, 9, 9);
    }

    /** */
    private Timestamp legacyTs() {
        return new Timestamp(119, 8, 9, 9, 9, 9,
            90900000);
    }

    /** */
    private String tzname() {
        return Calendar.getInstance().getTimeZone().getID();
    }

    /** */
    private boolean isNotBlank(Object o) {
        return o != null && !o.toString().isEmpty();
    }

    /**
     * Test object.
     */
    private static class TemporalKey implements Serializable {
        /** */
        @QuerySqlField
        private String node;

        /** */
        @QuerySqlField
        private String clnt;

        /** */
        @QuerySqlField
        private String tzname;

        /** */
        @QuerySqlField
        private String types;

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TemporalKey.class, this);
        }
    }

    /**
     * Test object.
     */
    private static class TemporalObject implements Serializable {
        /** */
        @QuerySqlField
        private Date dateVal;

        /** */
        @QuerySqlField
        private Time timeVal;

        /** */
        @QuerySqlField
        private Timestamp tsVal;

        /** */
        @QuerySqlField
        private Instant instantVal;

        /** */
        @QuerySqlField
        private LocalDate locDateVal;

        /** */
        @QuerySqlField
        private LocalTime locTimeVal;

        /** */
        @QuerySqlField
        private LocalDateTime locDateTimeVal;

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TemporalObject.class, this);
        }
    }
}
