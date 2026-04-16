package org.apache.ignite.jdbc.thin;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

public abstract class AbstractJdbcBinaryConfigurationConsistencyTest extends JdbcThinAbstractSelfTest {

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true))
            )
            .setCacheConfiguration(new CacheConfiguration<>()
                .setName(DEFAULT_CACHE_NAME)
                .setIndexedTypes(Integer.class, Outer.class)
            );
    }

    IgniteEx setupCluster(IgniteConfiguration ignCfg) throws Exception {
        IgniteEx ign = (IgniteEx)startGrid("srv", ignCfg.setIgniteInstanceName("srv"), null);

        ign.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Outer> cache = ign.cache(DEFAULT_CACHE_NAME);

        cache.put(0, Outer.of(0));
        verifyMarshallerFolder(ign);

        return ign;
    }

    void prepareJdbcParamsAndRunQuery(IgniteEx ignite,
        String factory,
        Boolean lowerCase,
        Boolean simpleName
    ) throws SQLException {
        String params = prepareJdbcParams(factory, lowerCase, simpleName);

        try (Connection c = connect(ignite, params)) {
            execute(c, String.format("SELECT * from \"%s\".%s", DEFAULT_CACHE_NAME, Outer.class.getSimpleName()));
        }
    }

    static String prepareJdbcParams(String factory, Boolean lowerCase, Boolean simpleName) {
        StringBuilder params = new StringBuilder();

        if (factory != null)
            params.append("binaryConfigurationFactory=").append(factory).append("&");

        if (lowerCase != null)
            params.append("useLowerCaseForBinaryTypes=").append(lowerCase).append("&");

        if (simpleName != null)
            params.append("useSimpleNamesForBinaryTypes=").append(simpleName);

        // Delete trailing '&' if present
        if (params.length() > 1 && params.charAt(params.length() - 1) == '&')
            params.deleteCharAt(params.length() - 1);

        return params.toString();
    }

    static void verifyMarshallerFolder(IgniteEx ign) throws IOException {
        Path p = Paths.get(ign.configuration().getWorkDirectory(), DataStorageConfiguration.DFLT_MARSHALLER_PATH);
        File[] files = p.toFile().listFiles();

        assertNotNull(p.toFile().getAbsolutePath() + " directory should contain at least one file", files);

        StringBuilder assertionHelp = new StringBuilder("\nFound entries:");

        Map<String, Long> typeNames = new HashMap<>();
        for (File f : files) {
            String fileName = f.getName();

            long typeId = Long.parseLong(fileName.substring(0, fileName.indexOf(".classname")));
            String typeName = new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);

            assertionHelp.append("\n  ").append(typeId).append("\t -> ").append(typeName);

            Long previousTypeId = typeNames.get(typeName);
            if (previousTypeId != null)
                throw new AssertionError("Multiple entries with same type name " +
                    "<" + typeName + "> " +
                    "[" + typeId + ", " + previousTypeId + "]" +
                    assertionHelp
                );

            typeNames.put(typeName, typeId);
        }
    }

    protected static void assertThrowsSqlException(String expectedErrMsg, JdbcOperation operation) {
        assertThrows(
            null,
            () -> {
                operation.execute();

                // return <null>, so this lambda becomes Callable that doesn't wrap thrown exception unlike RunnableX.run()
                return null;
            },
            SQLException.class,
            expectedErrMsg
        );
    }

    /** Any sort of operation with JDBC client */
    protected interface JdbcOperation {
        void execute() throws SQLException;
    }

    /** Row level POJO */
    private static class Outer {

        @QuerySqlField(index = true)
        private long id;

        @QuerySqlField
        private Inner inner;

        private static Outer of(long id) {
            Outer o = new Outer();
            o.id = id;
            o.inner = Inner.of(id);
            return o;
        }

        @Override public String toString() {
            return "O[" +
                "id=" + id + ", " +
                "inner=" + inner +
                ']';
        }
    }

    /** Inner field level POJO */
    private static class Inner {
        public String name;

        public static Inner of(long id) {
            Inner i = new Inner();
            i.name = "name-" + id;
            return i;
        }

        @Override public String toString() {
            return "I[name='" + name + "']";
        }
    }
}
