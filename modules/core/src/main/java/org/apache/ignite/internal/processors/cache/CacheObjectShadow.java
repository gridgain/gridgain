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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.IgniteUtils.EMPTY_BYTES;

/**
 * This class represents a place holder for a cache object and provides its type only.
 * The instances of this class should not be used in any other context except the reading from data tree when the type is only needed.
 */
public class CacheObjectShadow extends CacheObjectAdapter {
    /** */
    private static final long  serialVersionUID = 0L;

    /** Type of cache object. */
    private byte type;

    /**
     * Default constructor.
     */
    public CacheObjectShadow() {
        valBytes = EMPTY_BYTES;
    }

    /**
     * Creates a new instance of CacheObjectShadow with the given type.
     *
     * @param type Type of cache object.
     **/
    public CacheObjectShadow(byte type) {
        this.type = type;
        valBytes = EMPTY_BYTES;
    }

    /** {@inheritDoc} */
    @Override public byte cacheObjectType() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public <T> @Nullable T value(CacheObjectValueContext ctx, boolean cpy) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T> @Nullable T value(CacheObjectValueContext ctx, boolean cpy, ClassLoader ldr) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes(CacheObjectValueContext ctx) throws IgniteCheckedException {
        return EMPTY_BYTES;
    }

    /** {@inheritDoc} */
    @Override public boolean isPlatformType() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(CacheObjectContext ctx, boolean compress) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(CacheObjectValueContext ctx, ClassLoader ldr) throws IgniteCheckedException {
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(CacheObjectValueContext ctx) throws IgniteCheckedException {
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
    }
}
