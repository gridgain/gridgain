/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

import org.apache.ignite.sqltests.affinity.AbstractAffinityColumnTest;

public abstract class AbstractAffinityColumnArbitraryTypeTest extends AbstractAffinityColumnTest<AbstractAffinityColumnTest.Table> {

    static final String KEY_TYPE = "ACME_KEY_TYPE";
    static final String VAL_TYPE = "ACME_VAL_TYPE";

    @Override protected String getKeyType() {
        return KEY_TYPE;
    }

    protected void initTables() {
        fooTable = new Table(ignite(0), FOO_TABLE, FOO_CACHE, ID_FIELD, GROUP_ID_FIELD, getKeyType(), VAL_TYPE, FieldValueGenerator.DEFAULT)
            .create(backups);

        barTable = new Table(ignite(0), BAR_TABLE, BAR_CACHE, ID_FIELD, GROUP_ID_FIELD, getKeyType(), VAL_TYPE, FieldValueGenerator.DEFAULT)
            .create(backups);
    }

}
