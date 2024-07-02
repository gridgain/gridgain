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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.gridgain.internal.h2.api.JavaObjectSerializer;
import org.jetbrains.annotations.NotNull;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.binary.BinaryObject;  
import java.util.Arrays; 


/**
 * Ignite java object serializer implementation for H2.
 */
class H2JavaObjectSerializer implements JavaObjectSerializer {
    /** Class loader. */
    private final ClassLoader clsLdr;

    /** Marshaller. */
    private final Marshaller marshaller;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */

    private final IgniteLogger log;

    H2JavaObjectSerializer(@NotNull GridKernalContext ctx) {
        marshaller = ctx.config().getMarshaller();
        clsLdr = U.resolveClassLoader(ctx.config());
        log = ctx.log(H2JavaObjectSerializer.class);
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(Object obj) throws Exception {
        try {
            return U.marshal(marshaller, obj);
        } catch (Exception e) {
            String errorMsg = obj instanceof BinaryObject ?
                "Failed to serialize BinaryObject with typeId: " + ((BinaryObject) obj).typeId() :
                "Failed to Serialize object: " + obj.getClass().getName();
            U.error(log, errorMsg, e);
            throw new Exception(errorMsg, e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object deserialize(byte[] bytes) throws Exception {
        try {
            return U.unmarshal(marshaller, bytes, clsLdr);
        } catch (Exception e) {
            String errorMsg = "Failed to deserialize data: " + Arrays.toString(bytes);
            U.error(log, errorMsg, e);
            throw new Exception(errorMsg, e);
        }
    }
}

