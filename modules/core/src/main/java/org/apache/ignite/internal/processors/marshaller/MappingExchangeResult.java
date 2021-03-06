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
package org.apache.ignite.internal.processors.marshaller;

import org.apache.ignite.IgniteCheckedException;

/**
 *
 */
public class MappingExchangeResult {
    /**  */
    private final String acceptedClsName;

    /** */
    private final IgniteCheckedException error;

    /** */
    private final ResultType resType;

    /** */
    private enum ResultType {
        /** */
        SUCCESS,

        /** */
        FAILURE,

        /** */
        EXCHANGE_DISABLED
    }

    /**
     */
    private MappingExchangeResult(ResultType resType, String acceptedClsName, IgniteCheckedException error) {
        this.resType = resType;
        this.acceptedClsName = acceptedClsName;
        this.error = error;
    }

    /**  */
    public String className() {
        return acceptedClsName;
    }

    /**  */
    public IgniteCheckedException error() {
        return error;
    }

    /** */
    public boolean successful() {
        return resType == ResultType.SUCCESS;
    }

    /** */
    public boolean exchangeDisabled() {
        return resType == ResultType.EXCHANGE_DISABLED;
    }

    /**
     * @param acceptedClsName Accepted class name.
     */
    static MappingExchangeResult createSuccessfulResult(String acceptedClsName) {
        assert acceptedClsName != null;

        return new MappingExchangeResult(ResultType.SUCCESS, acceptedClsName, null);
    }

    /**
     * @param error Error.
     */
    static MappingExchangeResult createFailureResult(IgniteCheckedException error) {
        assert error != null;

        return new MappingExchangeResult(ResultType.FAILURE, null, error);
    }

    /** */
    static MappingExchangeResult createExchangeDisabledResult() {
        return new MappingExchangeResult(ResultType.EXCHANGE_DISABLED, null, null);
    }
}
