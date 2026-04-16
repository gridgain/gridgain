package org.apache.ignite.jdbc.thin;

import javax.cache.configuration.Factory;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

public class JdbcFactoryParamBinaryConfigurationConsistencyTest extends AbstractJdbcBinaryConfigurationConsistencyTest {

    @Test
    public void assertCustomBinaryConfigFactoryProducesConsistentResults() throws Exception {
        IgniteConfiguration ignCfg = getConfiguration("srv")
            .setBinaryConfiguration(new BinaryConfiguration()
                .setIdMapper(new ShiftingIdMapper())
            );

        IgniteEx ign = setupCluster(ignCfg);

        prepareJdbcParamsAndRunQuery(ign, ShiftingBinaryConfigFactory.class.getName(), null, null);

        verifyMarshallerFolder(ign);
    }

    @Test
    public void assertThatIncorrectFactoryNameThrows() throws Exception {
        IgniteConfiguration ignCfg = getConfiguration("srv");

        IgniteEx ign = setupCluster(ignCfg);

        String factoryName = "FooBarFactory";

        assertThrowsSqlException(
            "Could not instantiate binary configuration factory class: " + factoryName,
            () -> prepareJdbcParamsAndRunQuery(ign, factoryName, null, null)
        );

        verifyMarshallerFolder(ign);
    }

    /** Custom factory that returns binary configuration with */
    public static class ShiftingBinaryConfigFactory implements Factory<BinaryConfiguration> {
        @Override public BinaryConfiguration create() {
            return new BinaryConfiguration()
                .setIdMapper(new ShiftingIdMapper());
        }
    }

    /** Custom mapper that works like default {@link BinaryBasicIdMapper}, but adds constant to each id */
    private static class ShiftingIdMapper extends BinaryBasicIdMapper {
        @Override public int typeId(String typeName) {
            return super.typeId(typeName) + 100500;
        }

        @Override public int fieldId(int typeId, String fieldName) {
            return super.fieldId(typeId, fieldName) + 100500;
        }
    }
}
