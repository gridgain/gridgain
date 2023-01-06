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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.util.IgniteUtils.KB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class IgniteNativeIoSelfTest {
    /** Native io file factory. */
    private AlignedBuffersDirectFileIOFactory factory;

    /** Directory to store files in. */
    private Path directory;

    /**
     * Sets up test instance.
     *
     * @throws Exception If failed.
     */
    @Before
    public void setUp() throws Exception {
        directory = Files.createTempDirectory("native-io-test");
        File storePath = directory.toFile();

        AsyncFileIOFactory asyncFileIOFactory = new AsyncFileIOFactory();

        factory = new AlignedBuffersDirectFileIOFactory(
            new NullLogger(),
            storePath,
            (int)(16 * KB),
            asyncFileIOFactory
        );
    }

    /**
     * Tears down test instance.
     */
    @After
    public void tearDown() {
        U.delete(directory);
    }

    /**
     * Tests creation of a new file.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateNewFile() throws Exception {
        File file = directory.resolve("file").toFile();

        try (FileIO io = factory.create(file, CREATE, WRITE)) {
            assertThat(io, is(instanceOf(AlignedBuffersDirectFileIO.class)));
        }
    }

    /**
     * Tests creation of a new file failing if it already exists.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateNewFileFailIfExists() throws Exception {
        File file = directory.resolve("file-if-not-exists").toFile();

        try (FileIO io = factory.create(file, CREATE_NEW, WRITE)) {
            assertThat(io, is(instanceOf(AlignedBuffersDirectFileIO.class)));
        }
    }
}
