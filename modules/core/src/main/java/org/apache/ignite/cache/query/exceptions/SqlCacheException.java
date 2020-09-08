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

import java.sql.SQLException;
import javax.cache.CacheException;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;

/**
 * General SQL query exception.
 */
public class SqlCacheException extends CacheException {
    /** */
    private static final long serialVersionUID = 0L;

    /** State to return as {@link SQLException#getSQLState()}. */
    private final String sqlState;

    /** Code to return as {@link SQLException#getErrorCode()}. */
    private final int errCode;

    /**
     * @param message Error message.
     * @param errCode Error code.
     * @param cause Cause.
     */
    public SqlCacheException(String message, int errCode, Throwable cause) {
        super(message, cause);
        this.errCode = errCode;
        this.sqlState = IgniteQueryErrorCode.codeToSqlState(errCode);
    }

    /**
     * @return Error code.
     */
    public int statusCode() {
        return errCode;
    }

    /**
     * @return SQL state.
     */
    public String sqlState() {
        return sqlState;
    }

    /**
     * Converts this exception to {@link SQLException}.
     *
     * @return Converted {@link SQLException}
     */
    public SQLException toJdbcException() {
        return new SQLException(getMessage(), sqlState, errCode, this);
    }
}
