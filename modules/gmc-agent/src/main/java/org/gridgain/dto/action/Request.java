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

package org.gridgain.dto.action;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.UUID;

/**
 * DTO for action request.
 */
@JsonDeserialize(using = RequestDeserializer.class)
public class Request {
    /** Request id. */
    private UUID id;

    /** Argument. */
    private Object arg;

    /** Action name. */
    private String actName;

    /**
     * @return Request id.
     */
    public UUID getId() {
        return id;
    }

    /**
     * @param id Request id.
     * @return This for chaining method calls.
     */
    public Request setId(UUID id) {
        this.id = id;
        return this;
    }

    /**
     * @return Action name.
     */
    public String getActionName() {
        return actName;
    }

    /**
     * @param actName Action name.
     * @return This for chaining method calls.
     */
    public Request setActionName(String actName) {
        this.actName = actName;
        return this;
    }

    /**
     * @return Action argument.
     */
    public Object getArgument() {
        return arg;
    }

    /**
     * @param arg Action argument.
     * @return This for chaining method calls.
     */
    public Request setArgument(Object arg) {
        this.arg = arg;
        return this;
    }
}
