package org.apache.ignite.jdbc.thin;

import java.util.Arrays;
import javax.cache.configuration.Factory;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class JdbcFlagParamsBinaryConfigurationConsistencyTest extends AbstractJdbcBinaryConfigurationConsistencyTest {

    @Parameterized.Parameters(name = "lowerCase={0},simpleName={1}")
    public static Iterable<Boolean[]> params() {
        return Arrays.asList(
            new Boolean[] {true, true},
            new Boolean[] {true, false},
            new Boolean[] {true, null},

            new Boolean[] {false, true},
            new Boolean[] {false, false},
            new Boolean[] {false, null},

            new Boolean[] {null, true},
            new Boolean[] {null, false},
            new Boolean[] {null, null}
        );
    }

    /** Use lowercase in ID mapper */
    @Parameterized.Parameter(0)
    public Boolean lowerCase;

    /** Use simple class name in name mapper */
    @Parameterized.Parameter(1)
    public Boolean simpleName;

    private static BinaryConfiguration getServerBinaryConfig(Boolean lowerCase, Boolean simpleName) {
        BinaryConfiguration cfg = new BinaryConfiguration();

        if (lowerCase != null)
            cfg.setIdMapper(new BinaryBasicIdMapper(lowerCase));

        if (simpleName != null)
            cfg.setNameMapper(new BinaryBasicNameMapper(simpleName));

        return cfg;
    }

    /**
     * Run server and jdbc client with different variations of idMapper and classNameMapper settings and
     * verify that both server and client come up with same binary type related IDs resolution results
     */
    @Test
    public void equalBinarySettingsShouldPreserveBinaryIdsResolutionConsistency() throws Exception {
        IgniteEx ign = setupCluster(getConfiguration("srv")
            .setBinaryConfiguration(getServerBinaryConfig(lowerCase, simpleName)));

        prepareJdbcParamsAndRunQuery(ign, null, lowerCase, simpleName);

        verifyMarshallerFolder(ign);
    }

    /**
     * Set up default binary settings on server-side and jdbc-side via config factory
     * and verify that adjusted jdbc-side flags do not impact anything
     */
    @Test
    public void customFactoryShouldHaveHigherPriorityOverFlags() throws Exception {
        if (lowerCase == null && simpleName == null) {
            // this case can't produce any parameters conflict
            return;
        }

        IgniteConfiguration ignCfg = getConfiguration().setBinaryConfiguration(new BinaryConfiguration());

        IgniteEx ign = setupCluster(ignCfg);

        assertThrowsSqlException(
            "Parameter 'binaryConfigurationFactory' is mutually exclusive with " +
                "'useLowerCaseForBinaryTypes' or 'useSimpleNamesForBinaryTypes'",
            () -> prepareJdbcParamsAndRunQuery(ign, DefaultBinaryConfigFactory.class.getName(), lowerCase, simpleName)
        );

        verifyMarshallerFolder(ign);
    }

    /** Factory that simply produces default binary configuration */
    public static class DefaultBinaryConfigFactory implements Factory<BinaryConfiguration> {
        @Override public BinaryConfiguration create() {
            return new BinaryConfiguration();
        }
    }
}
