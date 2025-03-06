/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.marshaller;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

/**
 * Wrapper for imput stream.
 */
public interface ObjectInputStreamWrapper {

    /**
     * Wraps an input stream to use a context while deserialization.
     *
     * @param inputStream Input stream.
     * @return Object input stream.
     */
    ObjectInputStream wrap(InputStream inputStream) throws IOException;
}
