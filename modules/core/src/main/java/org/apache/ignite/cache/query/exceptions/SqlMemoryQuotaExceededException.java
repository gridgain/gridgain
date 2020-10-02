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
package org.apache.ignite.cache.query.exceptions;

import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;

/**
 * The exception is thrown when query memory quota is exceeded.
 */
public class SqlMemoryQuotaExceededException extends SqlCacheException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param message Error message.
     */
    public SqlMemoryQuotaExceededException(String message) {
        super(message, IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY, null);
    }
}
