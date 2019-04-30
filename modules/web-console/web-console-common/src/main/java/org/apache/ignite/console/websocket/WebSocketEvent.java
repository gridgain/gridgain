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

package org.apache.ignite.console.websocket;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Websocket event POJO.
 */
public class WebSocketEvent {
    /** */
    private String reqId;

    /** */
    private String evtType;

    /** */
    private String payload;

    /**
     * Default constructor for serialization.
     */
    public WebSocketEvent() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param reqId Request ID.
     * @param evtType Event type.
     * @param payload Payload.
     */
    public WebSocketEvent(String reqId, String evtType, String payload) {
        this.reqId = reqId;
        this.evtType = evtType;
        this.payload = payload;
    }

    /**
     * Constructor with auto generated ID.
     *
     * @param evtType Event type.
     * @param payload Payload.
     */
    public WebSocketEvent(String evtType, String payload) {
        this(UUID.randomUUID().toString(), evtType, payload);
    }

    /**
     * @return Request ID.
     */
    public String getRequestId() {
        return reqId;
    }

    /**
     * @param reqId New request ID.
     * @return {@code this} for chaining.
     */
    public WebSocketEvent setRequestId(String reqId) {
        this.reqId = reqId;

        return this;
    }

    /**
     * @return Event type.
     */
    public String getEventType() {
        return evtType;
    }

    /**
     * @param evtType New event type.
     * @return {@code this} for chaining.
     */
    public WebSocketEvent setEventType(String evtType) {
        this.evtType = evtType;

        return this;
    }

    /**
     * @return Payload.
     */
    public String getPayload() {
        return payload;
    }

    /**
     * @param payload New payload.
     * @return {@code this} for chaining.
     */
    public WebSocketEvent setPayload(String payload) {
        this.payload = payload;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WebSocketEvent.class, this);
    }
}
