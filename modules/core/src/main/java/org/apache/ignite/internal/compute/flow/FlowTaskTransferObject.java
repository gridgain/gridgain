/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.internal.compute.flow;

import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper for task parameters and results that is transferred in flow between tasks. It stores the data
 * that should be transferred, and if the task has failed with exception, it stores the exception.
 */
public class FlowTaskTransferObject {
    private final Map<String, Object> data;

    private final Throwable exception;

    public FlowTaskTransferObject(String key, Object value) {
        this.data = new HashMap<>();
        this.data.put(key, value);
        this.exception = null;
    }

    public FlowTaskTransferObject(Map<String, Object> data) {
        this.data = data;
        this.exception = null;
    }

    public FlowTaskTransferObject(Throwable exception) {
        this.data = null;
        this.exception = exception;
    }

    /**
     * Data that should be transferred between tasks.
     *
     * @return Data.
     */
    public Map<String, Object> data() {
        return data;
    }

    /**
     * Flag of successfullness of the task execution.
     *
     * @return Whether is successfull.
     */
    public boolean successfull() {
        return exception == null;
    }

    /**
     * Returns an exception that task execution failed with, if it did.
     *
     * @return Exception.
     */
    public Throwable exception() {
        return exception;
    }
}
