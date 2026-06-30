/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

            // TODO: https://ggsystems.atlassian.net/browse/GG-48162
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

    /** What to expect in <B>BINARY_AFFINITY_FIELD</B> sys view column */
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
