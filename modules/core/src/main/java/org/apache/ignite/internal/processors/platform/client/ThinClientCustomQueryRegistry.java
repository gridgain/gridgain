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

package org.apache.ignite.internal.processors.platform.client;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryRawReader;

/**
 * Registry for custom queries processors in thin client.
 */
public class ThinClientCustomQueryRegistry {
    /** Processors map. */
    private static ConcurrentMap<String, CustomQueryProcessor> processors = new ConcurrentHashMap<>();

    /**
     * Finds processor by its name and call its operation.
     *
     * @param requestId Request id.
     * @param processorId Processor Id.
     * @param methodId Method Id.
     * @param reader Reader.
     * @return Response for client.
     */
    public static ClientResponse call(long requestId, String processorId, byte methodId, BinaryRawReader reader) {
        CustomQueryProcessor processor = processors.get(processorId);
        if(processor == null)
            return new ClientResponse(requestId, "Cannot find processor with id = " + processorId);

        return processor.call(requestId, methodId, reader);
    }

    /**
     * Registers processor.
     *
     * @param processor Custom query processor.
     */
    public static void register(CustomQueryProcessor processor) {
        String processorId = processor.id();
        if(processors.putIfAbsent(processorId, processor) != null)
            throw new IgniteException("Processor is already registered [id=" + processorId + "]");
    }
}
