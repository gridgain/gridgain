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
package org.apache.ignite.internal.processors.cache.binary;

import org.apache.ignite.binary.BinaryObjectException;

/**
 * Represents result of metadata remove.
 */
final class MetadataRemoveResult {
    /** */
    private final ResultType resType;

    /** */
    private final BinaryObjectException error;

    /**
     * @param resType Response type.
     * @param error Error.
     */
    private MetadataRemoveResult(ResultType resType, BinaryObjectException error) {
        this.resType = resType;
        this.error = error;
    }

    /**
     *
     */
    boolean rejected() {
        return resType == ResultType.REJECT;
    }

    /**
     *
     */
    BinaryObjectException error() {
        return error;
    }

    /**
     */
    static MetadataRemoveResult createSuccessfulResult() {
        return new MetadataRemoveResult(ResultType.SUCCESS, null);
    }

    /**
     * @param err Error lead to request failure.
     */
    static MetadataRemoveResult createFailureResult(BinaryObjectException err) {
        assert err != null;

        return new MetadataRemoveResult(ResultType.REJECT, err);
    }

    /**
     *
     */
    static MetadataRemoveResult createUpdateDisabledResult() {
        return new MetadataRemoveResult(ResultType.UPDATE_DISABLED, null);
    }

    /**
     *
     */
    private enum ResultType {
        /**
         * If request completed successfully.
         */
        SUCCESS,

        /**
         * If request was rejected for any reason.
         */
        REJECT,

        /**
         * If request arrived at the moment when node has been stopping.
         */
        UPDATE_DISABLED
    }
}
