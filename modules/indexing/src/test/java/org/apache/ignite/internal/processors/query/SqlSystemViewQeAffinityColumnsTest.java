package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class SqlSystemViewQeAffinityColumnsTest extends SqlSystemViewAbstractAffinityColumnSelfTest {

    /** Key class use-case */
    @Parameterized.Parameter(0)
    public KeyType keyType;

    /** Defines what to do about <code>userId</code> field after QE initialization */
    @Parameterized.Parameter(1)
    public ColDef columnTreatment;

    /** If not <code>null</code> it will be used as affinity key in custom CacheKeyConfiguration */
    @Parameterized.Parameter(2)
    public String overrideAffinity;

    /** What to expect in <B>BINARY_TYPE_AFFINITY_FIELD</B> sys view column */
    @Parameterized.Parameter(3)
    public String expectedField;

    /** What to expect in <B>AFFINITY_KEY_COLUMN</B> sys view column */
    @Parameterized.Parameter(4)
    public String expectedColumn;

    /** Different variations with {@link QueryEntity} produced from key-value class tokens. */
    public static class ClassesTest extends SqlSystemViewQeAffinityColumnsTest {

        @Parameterized.Parameters(name = "cls={0}, userIdColumn: {1}, override={2}, field={3}, col={4}")
        public static Iterable<Object[]> params() {
            return Arrays.asList(
                new Object[] {KeyType.UNANNOTATED, ColDef.LEAVE_AS_IS, null, null, _KEY},
                new Object[] {KeyType.UNANNOTATED, ColDef.DEFINE, null, null, _KEY},
                new Object[] {KeyType.UNANNOTATED, ColDef.DEFINE_WITH_ALIAS, null, null, _KEY},

                new Object[] {KeyType.SQL_ONLY, ColDef.LEAVE_AS_IS, null, null, _KEY},
                new Object[] {KeyType.SQL_ONLY, ColDef.DEFINE, null, null, _KEY},
                new Object[] {KeyType.SQL_ONLY, ColDef.DEFINE_WITH_ALIAS, null, null, _KEY},

                new Object[] {KeyType.AFF_ONLY, ColDef.DEFINE, null, userId, userId.toUpperCase()},
                new Object[] {KeyType.AFF_ONLY, ColDef.DEFINE_WITH_ALIAS, null, userId, qe_alias.toUpperCase()},

                new Object[] {KeyType.AFF_AND_SQL, ColDef.LEAVE_AS_IS, null, userId, userId.toUpperCase()},
                new Object[] {KeyType.AFF_AND_SQL, ColDef.DEFINE, null, userId, userId.toUpperCase()},
                new Object[] {KeyType.AFF_AND_SQL, ColDef.DEFINE_WITH_ALIAS, null, userId, qe_alias.toUpperCase()},

                new Object[] {KeyType.AFF_AND_SQL_WITH_ALIAS, ColDef.LEAVE_AS_IS, null, userId, annotation_alias.toUpperCase()},
                new Object[] {KeyType.AFF_AND_SQL_WITH_ALIAS, ColDef.DEFINE, null, userId, annotation_alias.toUpperCase()},
                new Object[] {KeyType.AFF_AND_SQL_WITH_ALIAS, ColDef.DEFINE_WITH_ALIAS, null, userId, qe_alias.toUpperCase()}

                // TODO:
                //new Object[] {KeyType.UNANNOTATED, ColDef.LEAVE_AS_IS, groupId, groupId, null},
                //new Object[] {KeyType.SQL_ONLY, ColDef.LEAVE_AS_IS, groupId, groupId, null},
                //new Object[] {KeyType.AFF_ONLY, ColDef.LEAVE_AS_IS, null, userId, null},
                //new Object[] {KeyType.AFF_ONLY, ColDef.LEAVE_AS_IS, groupId, groupId, null},
                //new Object[] {KeyType.AFF_AND_SQL, ColDef.LEAVE_AS_IS, groupId, groupId, null}
            );
        }

        @Test
        public void assertQeFromClass() throws Exception {
            // It's enough to pass class tokens for Ignite to infer proper CacheKeyConfiguration under the hood
            QueryEntity qeFromClasses = new QueryEntity(keyType.keyCls(), Val.class)
                .addQueryField("val", Integer.class.getName(), null);

            assertQe(qeFromClasses, expectedField, expectedColumn);
        }
    }

    /** Different variations with {@link QueryEntity} produced from key-value class names. */
    public static class ClassNamesTest extends SqlSystemViewQeAffinityColumnsTest {

        @Parameterized.Parameters(name = "cls={0}, useAlias={1}, override={2}, field={3}, col={4}")
        public static Iterable<Object[]> params() {
            return Arrays.asList(
                new Object[] {KeyType.UNANNOTATED, ColDef.LEAVE_AS_IS, null, null, _KEY},
                new Object[] {KeyType.UNANNOTATED, ColDef.DEFINE, null, null, _KEY},
                new Object[] {KeyType.UNANNOTATED, ColDef.DEFINE_WITH_ALIAS, null, null, _KEY},

                new Object[] {KeyType.SQL_ONLY, ColDef.LEAVE_AS_IS, null, null, _KEY},
                new Object[] {KeyType.SQL_ONLY, ColDef.DEFINE, null, null, _KEY},
                new Object[] {KeyType.SQL_ONLY, ColDef.DEFINE_WITH_ALIAS, null, null, _KEY},

                new Object[] {KeyType.AFF_ONLY, ColDef.DEFINE, null, userId, userId.toUpperCase()},
                new Object[] {KeyType.AFF_ONLY, ColDef.DEFINE_WITH_ALIAS, null, userId, qe_alias.toUpperCase()},

                new Object[] {KeyType.AFF_AND_SQL, ColDef.DEFINE, null, userId, userId.toUpperCase()},
                new Object[] {KeyType.AFF_AND_SQL, ColDef.DEFINE_WITH_ALIAS, null, userId, qe_alias.toUpperCase()},

                new Object[] {KeyType.AFF_AND_SQL_WITH_ALIAS, ColDef.DEFINE_WITH_ALIAS, null, userId, qe_alias.toUpperCase()},

                // When QueryEntity is initialized from class names it doesn't process @QuerySqlField annotations
                // and doesn't recognize alias from annotation, so it sees null (or userId if it was defined) as an affinity
                // column name instead.
                new Object[] {KeyType.AFF_AND_SQL_WITH_ALIAS, ColDef.DEFINE, null, userId, userId.toUpperCase()}

                // TODO:
                //new Object[] {KeyType.UNANNOTATED, ColDef.LEAVE_AS_IS, groupId, groupId, null},
                //new Object[] {KeyType.AFF_ONLY, ColDef.LEAVE_AS_IS, null, userId, null},
                //new Object[] {KeyType.AFF_ONLY, ColDef.LEAVE_AS_IS, groupId, groupId, null},
                //new Object[] {KeyType.SQL_ONLY, ColDef.LEAVE_AS_IS, groupId, groupId, null},
                //new Object[] {KeyType.AFF_AND_SQL, ColDef.LEAVE_AS_IS, groupId, groupId, null}
                //new Object[] {KeyType.AFF_AND_SQL_WITH_ALIAS, ColDef.LEAVE_AS_IS, null, userId, null},
            );
        }

        @Test
        public void assertQeFromClassName() throws Exception {
            // It's enough to pass class tokens for Ignite to infer proper CacheKeyConfiguration under the hood
            QueryEntity qeFromClasses = new QueryEntity(keyType.keyName(), Val.class.getName())
                .addQueryField("val", Integer.class.getName(), null);

            // When QueryEntity is initialized from class names it doesn't process @QuerySqlField annotations
            // and will not see alias assigned via this annotation, so it should see affinityField as an affinity
            // column name instead
            assertQe(qeFromClasses, expectedField, expectedColumn);
        }
    }

    protected void assertQe(QueryEntity qe, String expectedField, String expectedColumn) throws Exception {
        if (columnTreatment != ColDef.LEAVE_AS_IS) {
            String a = columnTreatment == ColDef.DEFINE_WITH_ALIAS ? qe_alias : null;

            qe
                .addQueryField(userId, Integer.class.getName(), a)
                .setKeyFields(F.asSet(userId));
        }

        setupCache(cfg -> {
            cfg.setQueryEntities(Collections.singleton(qe));

            if (overrideAffinity != null) {
                cfg.setKeyConfiguration(new CacheKeyConfiguration()
                    .setTypeName(keyType.keyName())
                    .setAffinityKeyFieldName(overrideAffinity)
                );
            }
        });

        assertTableViewHas(expectedField, expectedColumn);
        assertTableColumnsViewHasOnlyThatColumnAsAffinity(expectedColumn);
    }

    /** Defines what to do about <code>userId</code> field after QE initialization */
    protected enum ColDef {
        LEAVE_AS_IS,
        DEFINE,
        DEFINE_WITH_ALIAS
    }

}
