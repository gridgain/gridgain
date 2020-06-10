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

#ifndef IGNITE_BENCHMARKS_ODBC_UTILS_H
#define IGNITE_BENCHMARKS_ODBC_UTILS_H

#include <ignite/benchmarks/utils.h>

#include <sql.h>
#include <sqlext.h>

#include <vector>
#include <string>
#include <sstream>

namespace odbc_utils
{
    /** Read buffer size. */
    enum { ODBC_BUFFER_SIZE = 1024 };

    /**
     * Represents simple string buffer.
     */
    struct OdbcStringBuffer
    {
        SQLCHAR buffer[ODBC_BUFFER_SIZE];
        SQLLEN reallen;
    };

    /**
     * Fetch result set returned by query.
     *
     * @param stmt Statement.
     */
    void FetchOdbcResultSet(SQLHSTMT stmt)
    {
        SQLSMALLINT columnsCnt = 0;

        // Getting number of columns in result set.
        SQLNumResultCols(stmt, &columnsCnt);

        std::vector<OdbcStringBuffer> columns(columnsCnt);

        // Binding colums.
        for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
            SQLBindCol(stmt, i + 1, SQL_C_DEFAULT, columns[i].buffer, ODBC_BUFFER_SIZE, &columns[i].reallen);

        while (true)
        {
            SQLRETURN ret = SQLFetch(stmt);

            if (!SQL_SUCCEEDED(ret))
                break;
        }
    }

    /**
     * Extract error message.
     *
     * @param handleType Type of the handle.
     * @param handle Handle.
     * @return Error message.
     */
    std::string GetOdbcErrorMessage(SQLSMALLINT handleType, SQLHANDLE handle)
    {
        SQLCHAR sqlstate[7] = {};
        SQLINTEGER nativeCode;

        SQLCHAR message[ODBC_BUFFER_SIZE];
        SQLSMALLINT reallen = 0;

        SQLGetDiagRec(handleType, handle, 1, sqlstate, &nativeCode, message, ODBC_BUFFER_SIZE, &reallen);

        return std::string(reinterpret_cast<char*>(sqlstate)) + ": " +
            std::string(reinterpret_cast<char*>(message), reallen);
    }

    /**
     * Extract error from ODBC handle and throw it as runtime_error.
     *
     * @param handleType Type of the handle.
     * @param handle Handle.
     * @param msg Error message.
     */
    void ThrowOdbcError(SQLSMALLINT handleType, SQLHANDLE handle, std::string msg)
    {
        std::stringstream builder;

        builder << msg << ": " << GetOdbcErrorMessage(handleType, handle);

        throw std::runtime_error(builder.str());
    }

    /**
     * Check that ODBC operation complete successfully.
     *
     * @param ret Value returned by the operation.
     * @param handleType Type of the handle.
     * @param handle Handle.
     * @param msg Error message.
     */
    void CheckOdbcOperation(SQLRETURN ret, SQLSMALLINT handleType, SQLHANDLE handle, std::string msg)
    {
        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(handleType, handle, msg);
    }

    /**
     * Fetch cache data using ODBC interface.
     */
    void ExecuteAndFetch(SQLHDBC dbc, const std::string& query)
    {
        SQLHSTMT stmt;

        // Allocate a statement handle
        SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

        std::vector<SQLCHAR> buf(query.begin(), query.end());

        SQLRETURN ret = SQLExecDirect(stmt, &buf[0], static_cast<SQLSMALLINT>(buf.size()));

        if (SQL_SUCCEEDED(ret))
            FetchOdbcResultSet(stmt);
        else
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to execute query");

        // Releasing statement handle.
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }

    /**
     * Insert data using ODBC interface.
     */
    void ExecuteNoFetch(SQLHDBC dbc, const std::string& query)
    {
        SQLHSTMT stmt;

        // Allocate a statement handle
        SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

        std::vector<SQLCHAR> buf(query.begin(), query.end());

        SQLRETURN ret = SQLExecDirect(stmt, &buf[0], static_cast<SQLSMALLINT>(buf.size()));

        CheckOdbcOperation(ret, SQL_HANDLE_STMT, stmt, "Failed to execute query");

        // Releasing statement handle.
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }
} // namespace odbc_utils

#endif // IGNITE_BENCHMARKS_ODBC_UTILS_H
