/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.sqltests.affinity.arbitrary;

import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BINARY_SORT_OBJECT_FIELDS;

/**
 * All tests below fail because of binary schema merging conflicts,
 * binary metadata initialized by 'puts' doesn't have affinityKey specified (despite actual affinity distribution will use GROUP_ID field),
 * meanwhile metadata created during SQL inserts HAS affinityKey=GROUP_ID.
 *
 * Merging it triggered by difference between 'puts' and SQL binary schema ids
 * (see {@link org.apache.ignite.internal.binary.builder.BinaryObjectBuilderImpl:302})
 *
 * 'puts' schema has field order [USER_ID, GROUP_ID], while SQL schemas [GROUP_ID, USER_ID].
 * This happens even despite we have <code>IGNITE_BINARY_SORT_OBJECT_FIELDS=true</code> and default idMapper
 * {@link org.apache.ignite.binary.BinaryBasicIdMapper} <code>lower=true</code>
 */
@WithSystemProperty(key = IGNITE_BINARY_SORT_OBJECT_FIELDS, value = "true")
public class AffinityColumnArbitraryTypeAndFieldsNameMismatchWithSortingTest extends AffinityColumnArbitraryTypeAndFieldsNameMismatchTest {

    @Test
    public void testPutFirstUCapGLower() throws Exception {
        // When sorting is enabled "UserId" still will be first for 'put',
        // because field names inserted to TreeSet !BEFORE! applying default field name mapper (lower=true)
        // (see BinaryObjectBuilderImpl:302)
        testPutFirst("UserId", "groupid");
    }

    @Test
    public void testPutFirstGLowerUCap() throws Exception {
        // Same as previous
        testPutFirst("groupid", "UserId");
    }

    @Test
    public void testPutFirstGCapUCap() throws Exception {
        // Same as previous
        testPutFirst("Groupid", "UserId");
    }

}
