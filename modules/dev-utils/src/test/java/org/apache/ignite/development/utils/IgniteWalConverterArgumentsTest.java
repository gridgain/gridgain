/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.development.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test for IgniteWalConverterArguments
 */
public class IgniteWalConverterArgumentsTest extends GridCommonAbstractTest {

    /**
     * Verify that your code throws a specific exception.
     */
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    /**
     *
     */
    public IgniteWalConverterArgumentsTest() {
        super(false);
    }

    /**
     * View help
     * <ul>
     *     <li>Read wal with out params</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testViewHelp() throws Exception {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        final IgniteWalConverterArguments parseArgs = IgniteWalConverterArguments.parse(new PrintStream(out), null);

        Assert.assertNull(parseArgs);

        final String help = out.toString();

        Assert.assertTrue(help.startsWith("Print WAL log data in human-readable form."));

        for (final Field field : IgniteWalConverterArguments.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())
                && Modifier.isStatic(field.getModifiers())
                && field.getType() == String.class) {
                field.setAccessible(true);

                final String arg = (String)field.get(null);

                Assert.assertTrue(help.contains("    " + arg + " "));
            }
        }
    }

    /**
     * Checking whether fields "walDir" or "walArchiveDir" are mandatory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRequiredWalDir() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("The paths to the WAL files are not specified.");

        IgniteWalConverterArguments.parse(System.out, new String[] {"pageSize=4096"});
    }

    /**
     * Checking whether field "walDir" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectWalDir() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Incorrect path to dir with wal files: non_existing_path");

        IgniteWalConverterArguments.parse(System.out, new String[] {"walDir=non_existing_path"});
    }

    /**
     * Checking whether field "walArchiveDir" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectWalArchiveDir() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Incorrect path to dir with archive wal files: non_existing_path");

        IgniteWalConverterArguments.parse(System.out, new String[] {"walArchiveDir=non_existing_path"});
    }

    /**
     * Checking whether field "pageSize" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectPageSize() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Incorrect page size. Error parse: not_integer");

        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "walDir=" + wal.getAbsolutePath(),
            "pageSize=not_integer"
        };

        IgniteWalConverterArguments.parse(System.out, args);
    }

    /**
     * Checking whether field "binaryMetadataFileStoreDir" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectBinaryMetadataFileStoreDir() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Incorrect path to dir with binary meta files: non_existing_path");

        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "walDir=" + wal.getAbsolutePath(),
            "binaryMetadataDir=non_existing_path"
        };

        IgniteWalConverterArguments.parse(System.out, args);
    }

    /**
     * Checking whether field "marshallerMappingFileStoreDir" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectMarshallerMappingDir() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Incorrect path to dir with marshaller files: non_existing_path");

        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "walDir=" + wal.getAbsolutePath(),
            "marshallerMappingDir=non_existing_path"
        };

        IgniteWalConverterArguments.parse(System.out, args);
    }

    /**
     * Checking whether field "keepBinary" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectKeepBinary() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Incorrect flag keepBinary, valid value: true or false. Error parse: not_boolean");

        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "walDir=" + wal.getAbsolutePath(),
            "keepBinary=not_boolean"
        };

        IgniteWalConverterArguments.parse(System.out, args);
    }

    /**
     * Checking whether field "recordTypes" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectRecordTypes() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Unknown record types: [not_exist].");

        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "walDir=" + wal.getAbsolutePath(),
            "recordTypes=not_exist"
        };

        IgniteWalConverterArguments.parse(System.out, args);
    }

    /**
     * Checking whether field "recordTypes" are incorrect several value.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectSeveralRecordTypes() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Unknown record types: [not_exist1, not_exist2].");

        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "walDir=" + wal.getAbsolutePath(),
            "recordTypes=not_exist1,not_exist2"
        };

        IgniteWalConverterArguments.parse(System.out, args);
    }

    /**
     * Checking whether field "walTimeFromMillis" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectWalTimeFromMillis() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Incorrect walTimeFromMillis. Error parse: not_long");

        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "walDir=" + wal.getAbsolutePath(),
            "walTimeFromMillis=not_long"
        };

        IgniteWalConverterArguments.parse(System.out, args);
    }

    /**
     * Checking whether field "walTimeToMillis" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectWalTimeToMillis() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Incorrect walTimeToMillis. Error parse: not_long");

        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "walDir=" + wal.getAbsolutePath(),
            "walTimeToMillis=not_long"
        };

        IgniteWalConverterArguments.parse(System.out, args);
    }

    /**
     * Checking whether field "processSensitiveData" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectProcessSensitiveData() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Unknown includeSensitive: unknown. Supported: ");

        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "walDir=" + wal.getAbsolutePath(),
            "includeSensitive=unknown"
        };

        IgniteWalConverterArguments.parse(System.out, args);
    }

    /**
     * Checking whether field "printStat" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectPrintStat() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Incorrect flag printStat, valid value: true or false. Error parse: not_boolean");

        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "walDir=" + wal.getAbsolutePath(),
            "printStat=not_boolean"
        };

        IgniteWalConverterArguments.parse(System.out, args);
    }

    /**
     * Checking whether field "skipCrc" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectSkipCrc() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Incorrect flag skipCrc, valid value: true or false. Error parse: not_boolean");

        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "walDir=" + wal.getAbsolutePath(),
            "skipCrc=not_boolean"
        };

        IgniteWalConverterArguments.parse(System.out, args);
    }

    /**
     * Checking default value.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDefault() throws IOException {
        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "walDir=" + wal.getAbsolutePath()
        };

        final IgniteWalConverterArguments parseArgs = IgniteWalConverterArguments.parse(System.out, args);

        Assert.assertEquals(4096, parseArgs.getPageSize());
        Assert.assertNull(parseArgs.getBinaryMetadataDir());
        Assert.assertNull(parseArgs.getMarshallerMappingDir());
        Assert.assertTrue(parseArgs.isKeepBinary());
        Assert.assertTrue(parseArgs.getRecordTypes().isEmpty());
        Assert.assertNull(parseArgs.getFromTime());
        Assert.assertNull(parseArgs.getToTime());
        Assert.assertNull(parseArgs.hasText());
        Assert.assertEquals(ProcessSensitiveData.MD5, parseArgs.includeSensitive());
        Assert.assertFalse(parseArgs.isPrintStat());
        Assert.assertFalse(parseArgs.isSkipCrc());
    }

    /**
     * Checking all value set.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testParse() throws IOException {
        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final File walArchive = File.createTempFile("wal_archive", "");
        walArchive.deleteOnExit();

        final File binaryMetadataDir = new File(System.getProperty("java.io.tmpdir"));

        final File marshallerDir = binaryMetadataDir;

        final String[] args = {
            "walDir=" + wal.getAbsolutePath(),
            "walArchiveDir=" + walArchive.getAbsolutePath(),
            "pageSize=2048",
            "binaryMetadataDir=" + binaryMetadataDir.getAbsolutePath(),
            "marshallerMappingDir=" + marshallerDir.getAbsolutePath(),
            "keepBinary=false",
            "recordTypes=DATA_RECORD,TX_RECORD",
            "walTimeFromMillis=1575158400000",
            "walTimeToMillis=1577836740999",
            "hasText=search string",
            "includeSensitive=MD5",
            "printStat=true",
            "skipCrc=true"};

        final IgniteWalConverterArguments parseArgs = IgniteWalConverterArguments.parse(System.out, args);
        Assert.assertEquals(wal, parseArgs.getWalDir());
        Assert.assertEquals(walArchive, parseArgs.getWalArchiveDir());
        Assert.assertEquals(2048, parseArgs.getPageSize());
        Assert.assertEquals(binaryMetadataDir, parseArgs.getBinaryMetadataDir());
        Assert.assertEquals(marshallerDir, parseArgs.getMarshallerMappingDir());
        Assert.assertFalse(parseArgs.isKeepBinary());
        Assert.assertTrue(parseArgs.getRecordTypes().contains(WALRecord.RecordType.DATA_RECORD));
        Assert.assertTrue(parseArgs.getRecordTypes().contains(WALRecord.RecordType.TX_RECORD));
        Assert.assertEquals(1575158400000L, (long)parseArgs.getFromTime());
        Assert.assertEquals(1577836740999L, (long)parseArgs.getToTime());
        Assert.assertEquals("search string", parseArgs.hasText());
        Assert.assertEquals(ProcessSensitiveData.MD5, parseArgs.includeSensitive());
        Assert.assertTrue(parseArgs.isPrintStat());
        Assert.assertTrue(parseArgs.isSkipCrc());
    }
}
