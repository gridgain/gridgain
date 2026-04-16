package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SqlSystemViewIndexedTypesAffinityColumnsTest extends SqlSystemViewAbstractAffinityColumnSelfTest {

    @Parameterized.Parameters(name = "cls={0}, overrideKey={1}, field={2}, col={3}")
    public static Iterable<Object[]> params() {
        return Arrays.asList(
            new Object[] {KeyType.UNANNOTATED, false, null, _KEY},
            new Object[] {KeyType.AFF_AND_SQL, false, userId, userId.toUpperCase()},
            new Object[] {KeyType.AFF_AND_SQL_WITH_ALIAS, false, userId, annotation_alias.toUpperCase()}

            // When affinity key is overridden via custom CacheKeyConfiguration SQL doesn't see affinity field,
            // but at the same time _KEY should not be marked as affinity=true
            // because only groupId is used for affinity calculations
            // TODO:
            //new Object[] {KeyType.UNANNOTATED, true, groupId, null},
            //new Object[] {KeyType.AFF_ONLY, false, userId, null},
            //new Object[] {KeyType.AFF_ONLY, true, groupId, null},
            //new Object[] {KeyType.AFF_AND_SQL, true, groupId, null},
            //new Object[] {KeyType.AFF_AND_SQL_WITH_ALIAS, true, groupId, null}
        );
    }

    /** Key class use-case */
    @Parameterized.Parameter(0)
    public KeyType keyType;

    /** Whether to override affinity key with {@link #groupId} in custom CacheKeyConfiguration */
    @Parameterized.Parameter(1)
    public Boolean overrideAffinity;

    /** What to expect in <B>BINARY_TYPE_AFFINITY_FIELD</B> sys view column */
    @Parameterized.Parameter(2)
    public String expectedField;

    /** What to expect in <B>AFFINITY_KEY_COLUMN</B> sys view column */
    @Parameterized.Parameter(3)
    public String expectedColumn;

    /** Real affinity field is exposed to SQL via QE that must be seen as affinity column. */
    @Test
    public void assertQeFromClass() throws Exception {
        setupCache(cfg -> {
            cfg.setIndexedTypes(keyType.keyCls(), Val.class);

            if (overrideAffinity) {
                cfg.setKeyConfiguration(new CacheKeyConfiguration()
                    .setTypeName(keyType.keyName())
                    .setAffinityKeyFieldName(groupId)
                );
            }
        });

        assertTableViewHas(expectedField, expectedColumn);
        assertTableColumnsViewHasOnlyThatColumnAsAffinity(expectedColumn);
    }
}
