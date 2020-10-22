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

package org.apache.ignite.internal.commandline.walconverter;

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
        expectedEx.expectMessage("One of the arguments --wal-dir or --wal-archive-dir must be specified.");

        IgniteWalConverterArguments.parse(System.out, new String[] {"--page-size", "4096"});
    }

    /**
     * Checking whether field "walDir" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectWalDir() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("File/directory 'non_existing_path' does not exist.");

        IgniteWalConverterArguments.parse(System.out, new String[] {"--wal-dir", "non_existing_path"});
    }

    /**
     * Checking whether field "walArchiveDir" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectWalArchiveDir() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("File/directory 'non_existing_path' does not exist.");

        IgniteWalConverterArguments.parse(System.out, new String[] {"--wal-archive-dir", "non_existing_path"});
    }

    /**
     * Checking whether field "pageSize" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectPageSize() throws Exception {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Can't parse number 'not_integer', expected type: java.lang.Integer");

        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "--wal-dir",
            wal.getAbsolutePath(),
            "--page-size",
            "not_integer"
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
        expectedEx.expectMessage("File/directory 'non_existing_path' does not exist.");

        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "--wal-dir",
            wal.getAbsolutePath(),
            "--binary-metadata-dir",
            "non_existing_path"
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
        expectedEx.expectMessage("File/directory 'non_existing_path' does not exist.");

        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "--wal-dir", wal.getAbsolutePath(),
            "--marshaller-mapping-dir", "non_existing_path"
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
            "--wal-dir", wal.getAbsolutePath(),
            "--record-types", "not_exist"
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
            "--wal-dir",
            wal.getAbsolutePath(),
            "--record-types",
            "not_exist1,not_exist2"
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
        expectedEx.expectMessage("Can't parse number 'not_long', expected type: java.lang.Long");

        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "--wal-dir", wal.getAbsolutePath(),
            "--wal-time-from-millis", "not_long"
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
        expectedEx.expectMessage("Can't parse number 'not_long', expected type: java.lang.Long");

        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "--wal-dir", wal.getAbsolutePath(),
            "--wal-time-to-millis", "not_long"
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
        expectedEx.expectMessage("Unknown --include-sensitive: unknown. Supported:");

        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "--wal-dir", wal.getAbsolutePath(),
            "--include-sensitive", "unknown"
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
            "--wal-dir", wal.getAbsolutePath()
        };

        final IgniteWalConverterArguments parseArgs = IgniteWalConverterArguments.parse(System.out, args);

        Assert.assertEquals(4096, parseArgs.getPageSize());
        Assert.assertNull(parseArgs.getBinaryMetadataDir());
        Assert.assertNull(parseArgs.getMarshallerMappingDir());
        Assert.assertFalse(parseArgs.isUnwrapBinary());
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
            "--wal-dir", wal.getAbsolutePath(),
            "--wal-archive-dir", walArchive.getAbsolutePath(),
            "--page-size", "2048",
            "--binary-metadata-dir", binaryMetadataDir.getAbsolutePath(),
            "--marshaller-mapping-dir", marshallerDir.getAbsolutePath(),
            "--unwrap-binary",
            "--record-types", "DATA_RECORD,TX_RECORD",
            "--wal-time-from-millis", "1575158400000",
            "--wal-time-to-millis", "1577836740999",
            "--has-text", "search string",
            "--include-sensitive", "HASH",
            "--print-stat",
            "--skip-crc"};

        final IgniteWalConverterArguments parseArgs = IgniteWalConverterArguments.parse(System.out, args);
        Assert.assertEquals(wal, parseArgs.getWalDir());
        Assert.assertEquals(walArchive, parseArgs.getWalArchiveDir());
        Assert.assertEquals(2048, parseArgs.getPageSize());
        Assert.assertEquals(binaryMetadataDir, parseArgs.getBinaryMetadataDir());
        Assert.assertEquals(marshallerDir, parseArgs.getMarshallerMappingDir());
        Assert.assertTrue(parseArgs.isUnwrapBinary());
        Assert.assertTrue(parseArgs.getRecordTypes().contains(WALRecord.RecordType.DATA_RECORD));
        Assert.assertTrue(parseArgs.getRecordTypes().contains(WALRecord.RecordType.TX_RECORD));
        Assert.assertEquals(1575158400000L, (long)parseArgs.getFromTime());
        Assert.assertEquals(1577836740999L, (long)parseArgs.getToTime());
        Assert.assertEquals("search string", parseArgs.hasText());
        Assert.assertEquals(ProcessSensitiveData.HASH, parseArgs.includeSensitive());
        Assert.assertTrue(parseArgs.isPrintStat());
        Assert.assertTrue(parseArgs.isSkipCrc());
    }
}
