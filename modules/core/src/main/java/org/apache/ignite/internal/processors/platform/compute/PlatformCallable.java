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

package org.apache.ignite.internal.processors.platform.compute;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.memory.PlatformInputStream;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * TODO: What is this class? Very similar to PlatformClosureJob. Consolidate.
 */
@SuppressWarnings("rawtypes")
public class PlatformCallable implements IgniteCallable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Serialized platform func. */
    private final Object func;

    /** Ignite instance. */
    @IgniteInstanceResource
    protected transient Ignite ignite;

    /**
     * Constructor.
     *
     * @param func Platform func.
     */
    public PlatformCallable(Object func) {
        assert func != null;

        this.func = func;
    }

    /** <inheritdoc /> */
    @Override public Object call() throws Exception {
        assert ignite != null;

        PlatformContext ctx = PlatformUtils.platformContext(ignite);

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            BinaryRawWriterEx writer = ctx.writer(out);

            writer.writeObject(func);

            out.synchronize();

            ctx.gateway().computeOutFuncExecute(mem.pointer());

            PlatformInputStream in = mem.input();

            in.synchronize();

            BinaryRawReaderEx reader = ctx.reader(in);

            return PlatformUtils.readInvocationResult(ctx, reader);
        }
    }
}
