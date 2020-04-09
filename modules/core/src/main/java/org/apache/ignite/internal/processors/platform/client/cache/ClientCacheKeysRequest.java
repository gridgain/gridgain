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

package org.apache.ignite.internal.processors.platform.client.cache;

import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.platform.client.tx.ClientTxAwareRequest;

/**
 * Key set request.
 */
public class ClientCacheKeysRequest extends ClientCacheDataRequest implements ClientTxAwareRequest {
    /** Keys. */
    private final Set<Object> keys;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    ClientCacheKeysRequest(BinaryRawReaderEx reader) {
        super(reader);

        keys = readSet(reader);
    }

    /**
     * Gets the set of keys.
     *
     * @return Keys.
     */
    public Set<Object> keys() {
        return keys;
    }

    /**
     * Reads a set of objects.
     *
     * @param reader Reader.
     * @return Set of objects.
     */
    private static Set<Object> readSet(BinaryRawReaderEx reader) {
        int cnt = reader.readInt();

        Set<Object> keys = new LinkedHashSet<>(cnt);

        for (int i = 0; i < cnt; i++)
            keys.add(reader.readObjectDetached());

        return keys;
    }
}
