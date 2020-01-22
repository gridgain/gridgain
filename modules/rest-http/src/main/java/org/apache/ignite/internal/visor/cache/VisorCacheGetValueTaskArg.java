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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Argument for {@link VisorCacheGetValueTask}.
 */
public class VisorCacheGetValueTaskArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String cacheName;

    /** Type of key object. String presentation of {@link VisorObjectType} value. */
    private String type;

    /** String presentation of key value. */
    private String key;

    /**
     * Default constructor.
     */
    public VisorCacheGetValueTaskArg() {
        // No-op.
    }

    /**
     * @param cacheName Cache name.
     * @param key Specified key.
     */
    public VisorCacheGetValueTaskArg(String cacheName, String type, String key) {
        this.cacheName = cacheName;
        this.type = type;
        this.key = key;
    }

    /**
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * @return Key type.
     */
    public String getType() {
        return type;
    }

    /**
     * @return Key.
     */
    public String getKey() {
        return key;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, cacheName);
        U.writeString(out, type);
        U.writeString(out, key);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cacheName = U.readString(in);
        type = U.readString(in);
        key = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheGetValueTaskArg.class, this);
    }
}
