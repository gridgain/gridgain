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

package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface CacheObject extends Message {
    /** */
    public static final byte TYPE_REGULAR = 1;

    /** */
    public static final byte TYPE_BYTE_ARR = 2;

    /** */
    public static final byte TYPE_BINARY = 100;

    /** */
    public static final byte TYPE_BINARY_ENUM = 101;

    /** */
    public static final byte TYPE_BINARY_COMPRESSED = -TYPE_BINARY;

    /** */
    public static final byte TOMBSTONE = -1;

    /**
     * @param ctx Context.
     * @param cpy If {@code true} need to copy value.
     * @return Value.
     */
    @Nullable public <T> T value(CacheObjectValueContext ctx, boolean cpy);

    /**
     * Deserializes a value from an internal representation.
     *
     * @param ctx Context.
     * @param cpy If {@code true} need to copy value.
     * @param ldr Class loader, if it is {@code null}, default class loader will be used.
     * @return Value.
     */
    @Nullable public <T> T value(CacheObjectValueContext ctx, boolean cpy, ClassLoader ldr);

    /**
     * @param ctx Context.
     * @return Value bytes.
     * @throws IgniteCheckedException If failed.
     */
    public byte[] valueBytes(CacheObjectValueContext ctx) throws IgniteCheckedException;

    /**
     * Returns the original length of the value in bytes.
     * In general, this method is equivalent to {@code valueBytes(ctx).length}, except shadow objects {@link CacheObjectShadow}.
     *
     * @param ctx Context.
     * @return Value bytes.
     * @throws IgniteCheckedException If failed.
     */
    public default int valueBytesOriginLength(CacheObjectValueContext ctx) throws IgniteCheckedException {
        return valueBytes(ctx).length;
    }

    /**
     * @param ctx Cache object context.
     * @return Size required to store this value object.
     * @throws IgniteCheckedException If failed.
     */
    public int valueBytesLength(CacheObjectContext ctx) throws IgniteCheckedException;

    /**
     * @param buf Buffer to write value to.
     * @return {@code True} if value was successfully written, {@code false} if there was not enough space in the
     *      buffer.
     * @throws IgniteCheckedException If failed.
     */
    public boolean putValue(ByteBuffer buf) throws IgniteCheckedException;

    /**
     * @param addr Address tp write value to.
     * @return Number of bytes written.
     * @throws IgniteCheckedException If failed.
     */
    public int putValue(long addr) throws IgniteCheckedException;

    /**
     * @param buf Buffer to write value to.
     * @param off Offset in source binary data.
     * @param len Length of the data to write.
     * @return {@code True} if value was successfully written, {@code false} if there was not enough space in the
     *      buffer.
     * @throws IgniteCheckedException If failed.
     */
    public boolean putValue(ByteBuffer buf, int off, int len) throws IgniteCheckedException;

    /**
     * @return Object type.
     */
    public byte cacheObjectType();

    /**
     * Gets flag indicating whether object value is a platform type. Platform types will be automatically
     * deserialized on public API cache operations regardless whether
     * {@link org.apache.ignite.IgniteCache#withKeepBinary()} is used or not.
     *
     * @return Platform type flag.
     */
    public boolean isPlatformType();

    /**
     * Prepares cache object for cache (e.g. copies user-provided object if needed).
     *
     * @param ctx Cache context.
     * @param compress Compression enabled for this cache object if flag is {@code true}.
     * @return Instance to store in cache.
     */
    public CacheObject prepareForCache(CacheObjectContext ctx, boolean compress) throws IgniteCheckedException;

    /**
     * @param ctx Context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    public void finishUnmarshal(CacheObjectValueContext ctx, ClassLoader ldr) throws IgniteCheckedException;

    /**
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareMarshal(CacheObjectValueContext ctx) throws IgniteCheckedException;
}
