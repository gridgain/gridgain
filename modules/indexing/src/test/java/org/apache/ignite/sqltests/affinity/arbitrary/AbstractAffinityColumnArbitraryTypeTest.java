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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.internal.binary.BinaryFieldMetadata;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
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

    // Metadata

    protected Collection<BinaryMetadata> getMetadata() {
        return ((CacheObjectBinaryProcessorImpl)ignite(0).context().cacheObjects()).binaryMetadata();
    }

    protected BinaryMetadata getKeyMetadata() {
        return getMetadata().stream()
            .filter(m -> getKeyType().equalsIgnoreCase(m.typeName()))
            .findAny()
            .orElseThrow(() -> new RuntimeException("Cant find " + getKeyType() + " metadata"));
    }

    protected BinaryMetadata dumpBinaryMeta() {
        return printResult(getKeyMetadata());
    }

    protected static BinaryMetadata printResult(BinaryMetadata m) {
        log("\ntypeId=" + printInt(m.typeId()));
        log("typeName=" + m.typeName());
        log("affinityKey=" + m.affinityKeyFieldName());
        log("Fields:");

        final Map<Integer, String> fldMap = new HashMap<>();
        m.fieldsMap().forEach((name, fldMeta) -> {
            log("    " +
                "name=" + name +
                ", type=" + BinaryUtils.fieldTypeName(fldMeta.typeId()) +
                ", fieldId=" + printInt(fldMeta.fieldId())
            );

            fldMap.put(fldMeta.fieldId(), name);
        });

        log("Schemas:");
        m.schemas().forEach(s ->
            log("    " +
                "schemaId=" + printInt(s.schemaId()) +
                ", fields=" + Arrays.stream(s.fieldIds())
                .mapToObj(fldMap::get)
                .collect(Collectors.toList())));
        log("");

        return m;
    }

    protected static String printInt(int val) {
        return "0x" + Integer.toHexString(val).toUpperCase() + " (" + val + ')';
    }

    protected void assertBinaryMeta() {
        BinaryMetadata binaryMetadata = dumpBinaryMeta();
        Map<String, BinaryFieldMetadata> fieldsMap = binaryMetadata.fieldsMap();

        Map<String, BinaryFieldMetadata> groupMap = new HashMap<>();
        fieldsMap.forEach((fn, fm) -> {
            if (GROUP_ID_FIELD.equalsIgnoreCase(fn)) {
                groupMap.put(fn, fm);
            }
        });

        assertEquals("Multiple fields for " + GROUP_ID_FIELD + " were created " + groupMap.keySet(), 1, groupMap.size());

        String affinityKey = binaryMetadata.affinityKeyFieldName();
        assertNotNull("Affinity key is null", affinityKey);
        assertEquals("Unexpected affinity key", GROUP_ID_FIELD.toLowerCase(), affinityKey.toLowerCase());
    }
}
