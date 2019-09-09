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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.gridgain.action.ActionMethod;

import java.io.IOException;
import java.util.UUID;

import static org.gridgain.action.ActionControllerAnnotationProcessor.getActions;

/**
 * Request deserializer.
 */
public class RequestDeserializer extends StdDeserializer<Request> {
    /** Parse error code. */
    private static final int PARSE_ERROR_CODE = -32700;

    /**
     * Default constructor.
     */
    public RequestDeserializer() {
        super(Request.class);
    }

    /** {@inheritDoc} */
    @Override public Request deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        Object arg;
        JsonNode node = p.getCodec().readTree(p);

        UUID id = p.getCodec().treeToValue(node.get("id"), UUID.class);
        String act = node.get("action").asText();
        ActionMethod actMtd = getActions().get(act);

        try {
            Class<?> argType = actMtd.getMethod().getParameters()[0].getType();
            arg = p.getCodec().treeToValue(node.get("argument"), argType);
        }
        catch (Exception e) {
            arg = new ResponseError(PARSE_ERROR_CODE, e.getMessage(), e.getStackTrace());
        }

        return new Request().setId(id).setAction(act).setArgument(arg);
    }
}
