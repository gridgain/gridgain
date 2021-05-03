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

#include <boost/chrono.hpp>
#include <boost/thread.hpp>

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
 * Execute SQL query using ODBC interface.
 */
SQLHSTMT Execute(SQLHDBC dbc, const std::string& query)
{
    SQLHSTMT stmt;

    // Allocate a statement handle
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

    std::vector<SQLCHAR> buf(query.begin(), query.end());

    SQLRETURN ret = SQLExecDirect(stmt, &buf[0], static_cast<SQLSMALLINT>(buf.size()));

    CheckOdbcOperation(ret, SQL_HANDLE_STMT, stmt, "Failed to execute query");

    return stmt;
}

/**
 * Fetch cache data using ODBC interface.
 */
void ExecuteAndFetch(SQLHDBC dbc, const std::string& query)
{
    SQLHSTMT stmt = Execute(dbc, query);

    FetchOdbcResultSet(stmt);

    // Releasing statement handle.
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/**
 * Insert data using ODBC interface.
 */
void ExecuteNoFetch(SQLHDBC dbc, const std::string& query)
{
    SQLHSTMT stmt = Execute(dbc, query);

    // Releasing statement handle.
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/**
 * Get next row.
 *
 * @param stmt Statement.
 */
void NextRow(SQLHSTMT stmt)
{
    SQLRETURN ret = SQLFetch(stmt);

    CheckOdbcOperation(ret, SQL_HANDLE_STMT, stmt, "Failed to get next row");
}

/**
 * Get single field of the current row.
 *
 * @param stmt Statement.
 * @param idx Column index.
 * @param resType Result type.
 */
template<typename T>
T GetSigleColumn(SQLHSTMT stmt, int32_t idx, int resType = SQL_C_SBIGINT)
{
    T val;

    SQLRETURN ret = SQLGetData(stmt, idx, resType, &val, 0, 0);
    CheckOdbcOperation(ret, SQL_HANDLE_STMT, stmt, "Failed to get field");

    return val;
}

/**
 * Utility OdbcLock class.
 *
 * Based on atomicity of table creation.
 */
class OdbcLock
{
public:
    OdbcLock(SQLHDBC dbc, const std::string& lockName) :
        locked(false),
        dbc(dbc),
        lockName(lockName)
    {
        // No-op.
    }

    /**
     *  Try get a lock for
     *
     * @return true if locked.
     */
    bool TryLock()
    {
        if (locked)
            return true;

        try
        {
            odbc_utils::ExecuteNoFetch(dbc, "CREATE TABLE PUBLIC." + lockName + " (id int primary key, field1 int)");

            locked = true;
        }
        catch (std::exception&)
        {
            locked = false;
        }

        return locked;
    }

    /**
     * Timed lock.
     * @param secs Timeout in seconds.
     * @return @c true if successfully locked within timeout.
     */
    bool TimedLock(int64_t secs)
    {
        using namespace boost;
        chrono::steady_clock::time_point begin = chrono::steady_clock::now();
        chrono::steady_clock::time_point now = begin;

        while (chrono::duration_cast<chrono::seconds>(now - begin).count() < secs)
        {
            TryLock();

            if (locked)
                return true;

            this_thread::sleep_for(chrono::milliseconds(500));
            now = chrono::steady_clock::now();
        }

        return locked;
    }

    /**
     * Unlock.
     */
    void Unlock()
    {
        if (!locked)
            return;

        try
        {
            odbc_utils::ExecuteNoFetch(dbc, "DROP TABLE PUBLIC." + lockName);
        }
        catch (std::exception&)
        {
            // No-op.
        }

        locked = false;
    }

    ~OdbcLock()
    {
        Unlock();
    }

private:
    /** Lock flag. */
    bool locked;

    /** Connection. */
    SQLHDBC dbc;

    /** Lock name. */
    std::string lockName;
};

} // namespace odbc_utils

#endif // IGNITE_BENCHMARKS_ODBC_UTILS_H
