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

#ifdef _WIN32
#   include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include <cstdio>

#include <vector>
#include <string>

#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "ignite/ignition.h"
#include "ignite/impl/binary/binary_utils.h"

#include "test_type.h"
#include "test_utils.h"
#include "odbc_test_suite.h"

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::common;
using namespace ignite_test;

using namespace boost::unit_test;

using ignite::impl::binary::BinaryUtils;

/**
 * Test setup fixture.
 */
struct CursorBindingTestSuiteFixture : public odbc::OdbcTestSuite
{
    static Ignite StartAdditionalNode(const char* name)
    {
        return StartPlatformNode("queries-test.xml", name);
    }

    /**
     * Constructor.
     */
    CursorBindingTestSuiteFixture() :
        testCache(0)
    {
        grid = StartAdditionalNode("NodeMain");

        testCache = grid.GetCache<int64_t, TestType>("cache");
    }

    /**
     * Destructor.
     */
    virtual ~CursorBindingTestSuiteFixture()
    {
        // No-op.
    }

    /** Node started during the test. */
    Ignite grid;

    /** Test cache instance. */
    Cache<int64_t, TestType> testCache;
};

BOOST_FIXTURE_TEST_SUITE(CursorBindingTestSuite, CursorBindingTestSuiteFixture)


#define CHECK_TEST_VALUES(idx, testIdx)                                                                             \
    do {                                                                                                            \
        BOOST_TEST_CONTEXT("Test idx: " << testIdx)                                                                 \
        {                                                                                                           \
            BOOST_CHECK(RowStatus[idx] == SQL_ROW_SUCCESS || RowStatus[idx] == SQL_ROW_SUCCESS_WITH_INFO);          \
                                                                                                                    \
            BOOST_CHECK(i32FieldsInd[idx] != SQL_NULL_DATA);                                                        \
            BOOST_CHECK(strFieldsLen[idx] != SQL_NULL_DATA);                                                        \
            BOOST_CHECK(doubleFieldsInd[idx] != SQL_NULL_DATA);                                                     \
                                                                                                                    \
            int32_t i32Field = static_cast<int32_t>(i32Fields[idx]);                                                \
            double doubleField = static_cast<double>(doubleFields[idx]);                                            \
            std::string strField(reinterpret_cast<char*>(&strFields[idx][0]),                                       \
                static_cast<size_t>(strFieldsLen[idx]));                                                            \
                                                                                                                    \
            CheckTestI32Value(testIdx, i32Field);                                                                   \
            CheckTestDoubleValue(testIdx, doubleField);                                                             \
            CheckTestStringValue(testIdx, strField);                                                                \
        }                                                                                                           \
    } while (false)

BOOST_AUTO_TEST_CASE(TestCursorBindingColumnWise)
{
    enum { ROWS_COUNT = 15 };
    enum { ROW_ARRAY_SIZE = 10 };
    enum { STRING_SIZE = 1024 };

    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache;PAGE_SIZE=8");

    // Preloading data.
    InsertTestBatch(0, ROWS_COUNT, ROWS_COUNT);

    SQLINTEGER i32Fields[ROW_ARRAY_SIZE] = {0};
    SQLLEN i32FieldsInd[ROW_ARRAY_SIZE];

    SQLCHAR strFields[ROW_ARRAY_SIZE][STRING_SIZE];
    SQLLEN strFieldsLen[ROW_ARRAY_SIZE];

    SQLDOUBLE doubleFields[ROW_ARRAY_SIZE];
    SQLLEN doubleFieldsInd[ROW_ARRAY_SIZE];

    SQLUSMALLINT RowStatus[ROW_ARRAY_SIZE];
    SQLUINTEGER NumRowsFetched;

    SQLRETURN ret;

    // Setting attributes.

    ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE, SQL_BIND_BY_COLUMN, 0);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, reinterpret_cast<SQLPOINTER*>(ROW_ARRAY_SIZE), 0);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_STATUS_PTR, RowStatus, 0);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &NumRowsFetched, 0);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    // Binding collumns.

    ret = SQLBindCol(stmt, 1, SQL_C_LONG, i32Fields, 0, i32FieldsInd);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLBindCol(stmt, 2, SQL_C_CHAR, strFields, STRING_SIZE, strFieldsLen);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLBindCol(stmt, 3, SQL_C_DOUBLE, doubleFields, 0, doubleFieldsInd);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    // Execute a statement to retrieve rows from the Orders table.
    SQLCHAR sql[] = "SELECT i32Field, strField, doubleField FROM TestType ORDER BY _key";
    ret = SQLExecDirect(stmt, sql, SQL_NTS);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLFetchScroll(stmt, SQL_FETCH_NEXT, 0);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    BOOST_CHECK_EQUAL(NumRowsFetched, (SQLUINTEGER)ROW_ARRAY_SIZE);

    for (int64_t i = 0; i < NumRowsFetched; i++)
    {
        CHECK_TEST_VALUES(i, i);
    }

    ret = SQLFetch(stmt);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    BOOST_CHECK_EQUAL(NumRowsFetched, ROWS_COUNT - ROW_ARRAY_SIZE);

    for (int64_t i = 0; i < NumRowsFetched; i++)
    {
        int64_t testIdx = i + ROW_ARRAY_SIZE;
        CHECK_TEST_VALUES(i, testIdx);
    }

    for (int64_t i = NumRowsFetched; i < ROW_ARRAY_SIZE; i++)
    {
        BOOST_TEST_INFO("Checking row status for row: " << i);
        BOOST_CHECK(RowStatus[i] == SQL_ROW_NOROW);
    }

    ret = SQLFetchScroll(stmt, SQL_FETCH_NEXT, 0);
    BOOST_CHECK_EQUAL(ret, SQL_NO_DATA);

    // Close the cursor.
    ret = SQLCloseCursor(stmt);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);
}

BOOST_AUTO_TEST_SUITE_END()
