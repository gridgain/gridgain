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

package org.apache.ignite.ml.inference.storage.model;

import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Base tests for all implementation of {@link ModelStorage}.
 */
public abstract class AbstractModelStorageTest {
    /**
     * Returns model storage to be tested.
     *
     * @return Model storage to be tested.
     */
    abstract ModelStorage getModelStorage();

    /** */
    @Test
    public void testPutGetRemoveFile() {
        ModelStorage mdlStorage = getModelStorage();

        byte[] data = new byte[]{1, 2, 3, 4, 5};

        mdlStorage.mkdirs("/");
        mdlStorage.putFile("/test", data);

        assertTrue(mdlStorage.exists("/test"));
        assertArrayEquals(data, mdlStorage.getFile("/test"));

        Set<String> paths = mdlStorage.listFiles("/");
        assertTrue(paths.contains("/test"));
        mdlStorage.remove("/test");

        assertFalse(mdlStorage.exists("/test"));
        assertTrue(mdlStorage.listFiles("/").isEmpty());
    }

    /** */
    @Test
    public void testListDirectory() {
        ModelStorage mdlStorage = getModelStorage();

        mdlStorage.mkdirs("/a/b");
        mdlStorage.mkdirs("/a/c");
        mdlStorage.putFile("/a/test", new byte[0]);

        Set<String> aFiles = mdlStorage.listFiles("/a");
        Set<String> bFiles = mdlStorage.listFiles("/a/b");
        Set<String> cFiles = mdlStorage.listFiles("/a/c");

        assertEquals(3, aFiles.size());
        assertTrue(bFiles.isEmpty());
        assertTrue(cFiles.isEmpty());

        assertTrue(aFiles.contains("/a/b"));
        assertTrue(aFiles.contains("/a/c"));
        assertTrue(aFiles.contains("/a/test"));
    }

    /** */
    @Test
    public void testIsDirectory() {
        ModelStorage mdlStorage = getModelStorage();

        mdlStorage.mkdirs("/a");

        assertTrue(mdlStorage.exists("/a"));
        assertTrue(mdlStorage.isDirectory("/a"));
        assertFalse(mdlStorage.isFile("/a"));
    }

    /** */
    @Test
    public void testIsFile() {
        ModelStorage mdlStorage = getModelStorage();

        mdlStorage.mkdirs("/");
        mdlStorage.putFile("/test", new byte[0]);

        assertTrue(mdlStorage.exists("/test"));
        assertTrue(mdlStorage.isFile("/test"));
        assertFalse(mdlStorage.isDirectory("/test"));
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testRemoveNonEmptyDirectory() {
        ModelStorage mdlStorage = getModelStorage();

        mdlStorage.mkdirs("/a/b/c");
        mdlStorage.mkdirs("/a/b/d");
        mdlStorage.mkdirs("/a/c");
        mdlStorage.putFile("/a/b/c/test", new byte[0]);
        mdlStorage.putFile("/a/b/test", new byte[0]);

        mdlStorage.remove("/a/b");
    }

    @Test
    public void testRemoveEmptyDirectory() {
        ModelStorage mdlStorage = getModelStorage();

        mdlStorage.mkdirs("/a/b/c");
        mdlStorage.mkdirs("/a/b/d");
        mdlStorage.mkdirs("/a/c");
        mdlStorage.putFile("/a/b/c/test", new byte[0]);
        mdlStorage.putFile("/a/b/test", new byte[0]);

        mdlStorage.remove("/a/b/c/test");
        mdlStorage.remove("/a/b/c");
        mdlStorage.remove("/a/b/d");
        mdlStorage.remove("/a/b/test");
        mdlStorage.remove("/a/b");

        assertFalse(mdlStorage.exists("/a/b"));
        assertFalse(mdlStorage.exists("/a/b/c"));
        assertFalse(mdlStorage.exists("/a/b/d"));
        assertFalse(mdlStorage.exists("/a/b/c/test"));
        assertFalse(mdlStorage.exists("/a/b/test"));
        assertTrue(mdlStorage.exists("/a"));

    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testPutFileIntoNonExistingDirectory() {
        ModelStorage mdlStorage = getModelStorage();

        mdlStorage.putFile("/test", new byte[0]);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testMakeDirInNonExistingDirectory() {
        ModelStorage mdlStorage = getModelStorage();

        mdlStorage.mkdir("/test");
    }
}
