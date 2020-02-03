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

package org.apache.ignite.internal.processors.cache.ivan;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class BinaryToStringClassNotFoundProblem extends GridCommonAbstractTest {
    /** */
    @Test
    public void testToStringInaccessibleOptimizedMarshallerClass() throws Exception {
        URLClassLoader extClsLdr = extClassLoader("Problematic.java", "" +
            "import java.io.Externalizable;\n" +
            "import java.io.IOException;\n" +
            "import java.io.ObjectInput;\n" +
            "import java.io.ObjectOutput;\n" +
            "\n" +
            "public class Problematic implements Externalizable {\n" +
            "    int x;\n" +
            "\n" +
            "    private Problematic() {\n" +
            "    }\n" +
            "\n" +
            "    public Problematic(int x) {\n" +
            "        this.x = x;\n" +
            "    }\n" +
            "\n" +
            "    @Override public void writeExternal(ObjectOutput out) throws IOException {\n" +
            "        out.writeInt(x);\n" +
            "    }\n" +
            "\n" +
            "    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {\n" +
            "        x = in.readInt();\n" +
            "    }\n" +
            "}");

        Object ext = newExtInstance(extClsLdr);

        IgniteEx ign = startGrid(0);

        IgniteCache<Object, Object> cache = ign.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        cache.put(1, new TestContainer(ext));

        assertTrue(cache.withKeepBinary().get(1).toString()
            .matches("org.apache.ignite.internal.processors.cache.ivan.BinaryToStringClassNotFoundProblem\\$TestContainer " +
                "\\[idHash=-?\\d+, hash=-?\\d+, x=Problematic\\(Class not found\\)]"));
    }

    /** */
    private URLClassLoader extClassLoader(String srcFileName, String srcCode) throws IOException {
        // t0d0 cleanup tmp files carefully
        Path tmpDir = Files.createTempDirectory("binary_object_to_string_test");
        Path src = tmpDir.resolve(srcFileName);

        Files.write(src, srcCode.getBytes());

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

        int status = compiler.run(null, null, null, src.toString());

        assert status == 0;

        return new URLClassLoader(new URL[] {tmpDir.toUri().toURL()});
    }

    /** */
    private Object newExtInstance(URLClassLoader extClsLdr) throws Exception {
        Class<?> extCls = extClsLdr.loadClass("Problematic");

        Constructor<?> ctor = extCls.getConstructor(int.class);

        return ctor.newInstance(42);
    }

    /** */
    private static class TestContainer {
        /** */
        private final Object x;

        /** */
        private TestContainer(Object x) {
            this.x = x;
        }
    }
}
