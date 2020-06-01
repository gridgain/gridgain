/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.util;

import java.util.Date;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.BinarySchema;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Checks command line metadata commands.
 */
public class GridCommandHandlerMetadataTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** Types count. */
    private static final int TYPES_CNT = 10;

    /**
     * Check the command '--meta list'.
     * Steps:
     * - Creates binary types for a test (by BinaryObjectBuilder);
     * - execute the command '--meta list'.
     * - Check command output (must contains all created types).
     */
    @Test
    public void testMetadataList() {
        injectTestSystemOut();

        for (int typeNum = 0; typeNum < TYPES_CNT; ++typeNum) {
            BinaryObjectBuilder bob = crd.binary().builder("Type_" + typeNum);

            for (int fldNum = 0; fldNum <= typeNum; ++fldNum)
                bob.setField("fld_" + fldNum, 0);

            bob.build();
        }

        assertEquals(EXIT_CODE_OK, execute("--meta", "list"));

        String out = testOut.toString();

        for (int typeNum = 0; typeNum < TYPES_CNT; ++typeNum)
            assertContains(log, out, "typeName=Type_" + typeNum);
    }

    /**
     * Check the command '--meta details'.
     * Steps:
     * - Creates binary two types for a test (by BinaryObjectBuilder) with several fields and shemas;
     * - execute the command '--meta details' for the type Type0 by name
     * - check metadata print.
     * - execute the command '--meta details' for the type Type0 by type ID on different formats.
     * - check metadata print.
     */
    @Test
    public void testMetadataDetails() {
        injectTestSystemOut();

        BinaryObjectBuilder bob0 = crd.binary().builder("TypeName0");
        bob0.setField("fld0", 0);
        bob0.build();

        bob0 = crd.binary().builder("TypeName0");
        bob0.setField("fld1", "0");
        bob0.build();

        bob0 = crd.binary().builder("TypeName0");
        bob0.setField("fld0", 1);
        bob0.setField("fld2", UUID.randomUUID());
        BinaryObject bo0 = bob0.build();

        BinaryObjectBuilder bob1 = crd.binary().builder("TypeName1");
        bob1.setField("fld0", 0);
        bob1.build();

        bob1 = crd.binary().builder("TypeName1");
        bob1.setField("fld0", 0);
        bob1.setField("fld1", new Date());
        bob1.setField("fld2", 0.1);
        bob1.setField("fld3", new long[]{0, 1, 2, 3});

        BinaryObject bo1 = bob1.build();

        assertEquals(EXIT_CODE_OK, execute("--meta", "details", "--typeName", "TypeName0"));
        checkTypeDetails(log, testOut.toString(), crd.context().cacheObjects().metadata(bo0.type().typeId()));

        assertEquals(EXIT_CODE_OK, execute("--meta", "details", "--typeId",
            "0x" + Integer.toHexString(crd.context().cacheObjects().typeId("TypeName1"))));
        checkTypeDetails(log, testOut.toString(), crd.context().cacheObjects().metadata(bo1.type().typeId()));

        assertEquals(EXIT_CODE_OK, execute("--meta", "details", "--typeId",
            Integer.toString(crd.context().cacheObjects().typeId("TypeName1"))));
        checkTypeDetails(log, testOut.toString(), crd.context().cacheObjects().metadata(bo1.type().typeId()));
    }

    /**
     * @param t Binary type.
     */
    private void checkTypeDetails(@Nullable IgniteLogger log, String cmdOut, BinaryType t) {
        assertContains(log, cmdOut, "typeId=" + "0x" + Integer.toHexString(t.typeId()).toUpperCase());
        assertContains(log, cmdOut, "typeName=" + t.typeName());
        assertContains(log, cmdOut, "Fields:");

        for (String fldName : t.fieldNames())
            assertContains(log, cmdOut, "name=" + fldName + ", type=" + t.fieldTypeName(fldName));

        for (BinarySchema s : ((BinaryTypeImpl)t).metadata().schemas())
            assertContains(log, cmdOut, "schemaId=0x" + Integer.toHexString(s.schemaId()).toUpperCase());
    }
}
