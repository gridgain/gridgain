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

package org.apache.ignite.plugin.extensions.communication;

/**
 * Common interface for responses.
 */
public interface TimeLoggableResponse extends Message {
    /** */
    long INVALID_TIMESTAMP = -1;

    /**
     * @return Send timestamp of request that triggered this response
     * in request sender node time.
     */
    long getReqSentTimestamp();

    /**
     * Sets request send timestamp in sender node time.
     */
    void setReqSendTimestamp(long reqSentTimestamp);

    /**
     * @return Received timestamp of request that triggered this response
     * in request receiver node time.
     */
    long getReqReceivedTimestamp();

    /**
     * Sets request receive timestamp in receiver time.
     */
    void setReqReceivedTimestamp(long reqReceivedTimestamp);

    /**
     * @return Response send timestamp which is sum of request send
     * timestamp and request processing time.
     */
    long getResponseSendTimestamp();

    /**
     * Sets request send timestamp.
     */
    void setResponseSendTimestamp(long responseSendTimestamp);
}
