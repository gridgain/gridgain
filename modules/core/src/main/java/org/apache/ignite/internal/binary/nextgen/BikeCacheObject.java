/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.binary.nextgen;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectAdapter;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

public class BikeCacheObject implements CacheObject {
    private static final long serialVersionUID = 0L;

    private final BikeTuple bike;
    private final int binTypeId;

    public BikeCacheObject(BikeTuple bike, int binTypeId) {
        this.bike = bike;
        this.binTypeId = binTypeId;
    }

    public BikeTuple tuple() {
        return bike;
    }

    @Override public <T> @Nullable T value(CacheObjectValueContext ctx, boolean cpy) {
        return (T)this;
    }

    @Override public byte[] valueBytes(CacheObjectValueContext ctx) throws IgniteCheckedException {
        return bike.data();
    }

    @Override public int valueBytesLength(CacheObjectContext ctx) throws IgniteCheckedException {
        return CacheObjectAdapter.objectPutSize(bike.data().length);
    }

    @Override public boolean putValue(ByteBuffer buf) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    @Override public int putValue(long addr) throws IgniteCheckedException {
        return CacheObjectAdapter.putValue(addr, CacheObject.TYPE_BIKE, bike.data(), 0);
    }

    @Override public boolean putValue(ByteBuffer buf, int off, int len) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    @Override public byte cacheObjectType() {
        return TYPE_BIKE;
    }

    @Override public boolean isPlatformType() {
        return false;
    }

    @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
        return this;
    }

    @Override public void finishUnmarshal(CacheObjectValueContext ctx, ClassLoader ldr) throws IgniteCheckedException {
    }

    @Override public void prepareMarshal(CacheObjectValueContext ctx) throws IgniteCheckedException {
    }

    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        throw new UnsupportedOperationException();
    }

    @Override public short directType() {
        throw new UnsupportedOperationException();
    }

    @Override public byte fieldsCount() {
        throw new UnsupportedOperationException();
    }

    @Override public void onAckReceived() {
    }

    public int binaryTypeId() {
        return binTypeId;
    }
}
