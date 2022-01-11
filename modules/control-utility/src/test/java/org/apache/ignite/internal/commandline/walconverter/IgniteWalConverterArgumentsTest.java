/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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
import java.util.stream.LongStream;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static java.nio.charset.Charset.defaultCharset;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterArguments.parse;
import static org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterArguments.parsePageId;
import static org.apache.ignite.internal.commandline.walconverter.IgniteWalConverterArguments.parsePageIds;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.corruptedPagesFile;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

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

        final IgniteWalConverterArguments parseArgs = parse(new PrintStream(out), null);

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

        parse(System.out, new String[] {"--page-size", "4096"});
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

        parse(System.out, new String[] {"--wal-dir", "non_existing_path"});
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

        parse(System.out, new String[] {"--wal-archive-dir", "non_existing_path"});
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

        parse(System.out, args);
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

        parse(System.out, args);
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

        parse(System.out, args);
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

        parse(System.out, args);
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

        parse(System.out, args);
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

        parse(System.out, args);
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

        parse(System.out, args);
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

        parse(System.out, args);
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

        final IgniteWalConverterArguments parseArgs = parse(System.out, args);

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
            "--record-types", "DATA_RECORD_V2,TX_RECORD",
            "--wal-time-from-millis", "1575158400000",
            "--wal-time-to-millis", "1577836740999",
            "--has-text", "search string",
            "--include-sensitive", "HASH",
            "--print-stat",
            "--skip-crc"};

        final IgniteWalConverterArguments parseArgs = parse(System.out, args);
        Assert.assertEquals(wal, parseArgs.getWalDir());
        Assert.assertEquals(walArchive, parseArgs.getWalArchiveDir());
        Assert.assertEquals(2048, parseArgs.getPageSize());
        Assert.assertEquals(binaryMetadataDir, parseArgs.getBinaryMetadataDir());
        Assert.assertEquals(marshallerDir, parseArgs.getMarshallerMappingDir());
        Assert.assertTrue(parseArgs.isUnwrapBinary());
        Assert.assertTrue(parseArgs.getRecordTypes().contains(WALRecord.RecordType.DATA_RECORD_V2));
        Assert.assertTrue(parseArgs.getRecordTypes().contains(WALRecord.RecordType.TX_RECORD));
        Assert.assertEquals(1575158400000L, (long)parseArgs.getFromTime());
        Assert.assertEquals(1577836740999L, (long)parseArgs.getToTime());
        Assert.assertEquals("search string", parseArgs.hasText());
        Assert.assertEquals(ProcessSensitiveData.HASH, parseArgs.includeSensitive());
        Assert.assertTrue(parseArgs.isPrintStat());
        Assert.assertTrue(parseArgs.isSkipCrc());
    }

    /**
     * Checking the correctness of the method {@link IgniteWalConverterArguments#parsePageId}.
     */
    @Test
    public void testParsePageId() {
        String[] invalidValues = {
            null,
            "",
            " ",
            "a",
            "a:",
            "a:b",
            "a:b",
            "a:1",
            "1:b",
            "1;1",
            "1a:1",
            "1:1b",
            "1:1:1",
        };

        for (String v : invalidValues)
            assertThrows(log, () -> parsePageId(v), IllegalArgumentException.class, null);

        assertEquals(new T2<>(1, 1L), parsePageId("1:1"));
    }

    /**
     * Checking the correctness of the method {@link IgniteWalConverterArguments#parsePageIds(File)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testParsePageIdsFile() throws Exception {
        File f = new File(System.getProperty("java.io.tmpdir"), "test");

        try {
            assertThrows(log, () -> parsePageIds(f), IllegalArgumentException.class, null);

            assertTrue(f.createNewFile());

            assertTrue(parsePageIds(f).isEmpty());

            U.writeStringToFile(f, "a:b", defaultCharset().toString(), false);
            assertThrows(log, () -> parsePageIds(f), IllegalArgumentException.class, null);

            U.writeStringToFile(f, "1:1,1:1", defaultCharset().toString(), false);
            assertThrows(log, () -> parsePageIds(f), IllegalArgumentException.class, null);

            U.writeStringToFile(f, "1:1", defaultCharset().toString(), false);
            assertEqualsCollections(F.asList(new T2<>(1, 1L)), parsePageIds(f));

            U.writeStringToFile(f, U.nl() + "2:2", defaultCharset().toString(), true);
            assertEqualsCollections(F.asList(new T2<>(1, 1L), new T2<>(2, 2L)), parsePageIds(f));
        }
        finally {
            assertTrue(U.delete(f));
        }
    }

    /**
     * Checking the correctness of the method {@link IgniteWalConverterArguments#parsePageIds(String...)}.
     */
    @Test
    public void testParsePageIdsStrings() {
        assertTrue(parsePageIds().isEmpty());

        assertThrows(log, () -> parsePageIds("a:b"), IllegalArgumentException.class, null);
        assertThrows(log, () -> parsePageIds("1:1", "a:b"), IllegalArgumentException.class, null);

        assertEqualsCollections(F.asList(new T2<>(1, 1L)), parsePageIds("1:1"));
        assertEqualsCollections(F.asList(new T2<>(1, 1L), new T2<>(2, 2L)), parsePageIds("1:1", "2:2"));
    }

    /**
     * Checking the correctness of parsing the argument "pages".
     *
     * @throws Exception If failed.
     */
    @Test
    public void testParsePagesArgument() throws IOException {
        File walDir = new File(System.getProperty("java.io.tmpdir"), "walDir");

        try {
            assertTrue(walDir.mkdir());

            String[] args = {"--wal-dir", walDir.getAbsolutePath(), "--pages", null};

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);

            args[3] = "1";
            assertThrows(log, () -> parse(ps, args), IllegalArgumentException.class, null);

            assertThrows(log, () -> parse(ps, args[0], args[1], args[2]), IgniteException.class, null);

            args[3] = "1:1";
            assertEqualsCollections(F.asList(new T2<>(1, 1L)), parse(ps, args).getPages());

            File f = new File(System.getProperty("java.io.tmpdir"), "test");

            try {
                args[3] = f.getAbsolutePath();
                assertThrows(log, () -> parse(ps, args), IllegalArgumentException.class, null);

                assertTrue(f.createNewFile());
                assertTrue(parse(ps, args).getPages().isEmpty());

                U.writeStringToFile(f, "1:1", defaultCharset().toString(), false);
                assertEqualsCollections(F.asList(new T2<>(1, 1L)), parse(ps, args).getPages());
            }
            finally {
                assertTrue(U.delete(f));
            }
        }
        finally {
            assertTrue(U.delete(walDir));
        }
    }

    /**
     * Checks that the file generated by the diagnostics is correct for the "pages" argument.
     *
     * @throws IOException If failed.
     */
    @Test
    public void testCorruptedPagesFile() throws IOException {
        File tmpDir = new File(System.getProperty("java.io.tmpdir"), getName());

        try {
            int grpId = 10;
            long[] pageIds = {20, 40};

            File f = corruptedPagesFile(tmpDir.toPath(), new RandomAccessFileIOFactory(), grpId, pageIds);

            assertTrue(f.exists());
            assertTrue(f.isFile());
            assertTrue(f.length() > 0);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);

            IgniteWalConverterArguments args =
                parse(ps, "--wal-dir", tmpDir.getAbsolutePath(), "--pages", f.getAbsolutePath());

            assertNotNull(args.getPages());

            assertEqualsCollections(
                LongStream.of(pageIds).mapToObj(pageId -> new T2<>(grpId, pageId)).collect(toList()),
                args.getPages()
            );
        }
        finally {
            if (tmpDir.exists())
                assertTrue(U.delete(tmpDir));
        }
    }
}
