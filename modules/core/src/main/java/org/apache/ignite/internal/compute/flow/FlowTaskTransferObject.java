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
import org.apache.ignite.lang.IgniteBiTuple;

public class FlowTaskTransferObject {
    private final Map<String, Object> data;

    private final Throwable exception;

    public FlowTaskTransferObject(IgniteBiTuple<String, Object> data) {
        this.data = new HashMap<>();
        this.data.put(data.getKey(), data.getValue());
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

    public Map<String, Object> data() {
        return data;
    }

    public boolean successfull() {
        return exception == null;
    }

    public Throwable exception() {
        return exception;
    }
}
