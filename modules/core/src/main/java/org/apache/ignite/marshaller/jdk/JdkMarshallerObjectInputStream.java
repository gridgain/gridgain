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

package org.apache.ignite.marshaller.jdk;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.ObjectInputStreamWrapper;

/**
 * This class defines custom JDK object input stream.
 */
public class JdkMarshallerObjectInputStream extends ObjectInputStream implements ObjectInputStreamWrapper {
    /** */
    private final ClassLoader clsLdr;

    /** Class name filter. */
    private final IgnitePredicate<String> clsFilter;

    /**
     * @param in Parent input stream.
     * @param clsLdr Custom class loader.
     * @throws IOException If initialization failed.
     */
    public JdkMarshallerObjectInputStream(InputStream in, ClassLoader clsLdr, IgnitePredicate<String> clsFilter) throws IOException {
        super(in);

        assert clsLdr != null;

        this.clsLdr = clsLdr;
        this.clsFilter = clsFilter;

        enableResolveObject(true);
    }

    /** {@inheritDoc} */
    @Override protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
        // NOTE: DO NOT CHANGE TO 'clsLoader.loadClass()'
        // Must have 'Class.forName()' instead of clsLoader.loadClass()
        // due to weird ClassNotFoundExceptions for arrays of classes
        // in certain cases.
        return U.forName(desc.getName(), clsLdr, clsFilter);
    }

    /** {@inheritDoc} */
    @Override protected Object resolveObject(Object o) throws IOException {
        if (o != null && o.getClass().equals(JdkMarshallerDummySerializable.class))
            return new Object();

        return super.resolveObject(o);
    }

    /** {@inheritDoc} */
    @Override public ObjectInputStream wrap(InputStream inputStream) throws IOException {
        return new JdkMarshallerObjectInputStream(inputStream, clsLdr, clsFilter);
    }
}
