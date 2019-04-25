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

package org.apache.ignite.internal.processors.odbc;

/**
 * Client listener message parser.
 */
public interface ClientListenerMessageParser {
    /**
     * Decode request from byte array.
     *
     * @param msg Message.
     * @return Request.
     */
    ClientListenerRequest decode(byte[] msg);

    /**
     * Encode response to byte array.
     *
     * @param resp Response.
     * @return Message.
     */
    byte[] encode(ClientListenerResponse resp);

    /**
     * Decode command type. Allows to recognize the command (message type) without decoding the entire message.
     *
     * @param msg Message.
     * @return Command type.
     */
    int decodeCommandType(byte[] msg);

    /**
     * Decode request Id. Allows to recognize the request Id, if any, without decoding the entire message.
     *
     * @param msg Message.
     * @return Request Id.
     */
    long decodeRequestId(byte[] msg);
}
