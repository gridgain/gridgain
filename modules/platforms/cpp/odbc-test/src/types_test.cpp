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

#include <vector>
#include <string>
#include <algorithm>

#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "test_type.h"
#include "test_utils.h"
#include "odbc_test_suite.h"

using namespace ignite;
using namespace ignite_test;

using namespace boost::unit_test;

/**
 * Test setup fixture.
 */
struct TypesTestSuiteFixture : odbc::OdbcTestSuite
{
    /**
     * Constructor.
     */
    TypesTestSuiteFixture()
    {
        // No-op.
    }

    /**
     * Destructor.
     */
    virtual ~TypesTestSuiteFixture()
    {
        // No-op.
    }

    /**
     * Merge specified date, time and timestamp columns into cache, select them and check they are the same.
     * @param key Key.
     * @param date Date.
     * @param time Time.
     * @param timestamp Timestamp.
     */
    void MergeSelectDateTime(SQLINTEGER key, const SQL_DATE_STRUCT& date,
        const SQL_TIME_STRUCT& time, const SQL_TIMESTAMP_STRUCT& timestamp)
    {
        SQLCHAR merge[] = "merge into TestType(_key, dateField, timeField, timestampField) VALUES(?, ?, ?, ?)";
        SQLRETURN ret = SQLPrepare(stmt, merge, SQL_NTS);
        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, 0);
        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        SQLLEN dateLenInd = sizeof(date);
        ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_DATE, SQL_DATE,
            (SQLSMALLINT)sizeof(date), 0, (SQLPOINTER)&date, sizeof(date), &dateLenInd);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        SQLLEN timeLenInd = sizeof(time);
        ret = SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_TIME, SQL_TIME,
            (SQLSMALLINT)sizeof(time), 0, (SQLPOINTER)&time, sizeof(time), &timeLenInd);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        SQLLEN timestampLenInd = sizeof(time);
        ret = SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_TIMESTAMP, SQL_TIMESTAMP,
            (SQLSMALLINT)sizeof(timestamp), 0, (SQLPOINTER)&timestamp, sizeof(timestamp), &timestampLenInd);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLExecute(stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        SQLCHAR select[] = "select dateField, timeField, timestampField from TestType where "
                           "_key = ? and dateField = ? and timeField = ? and timestampField = ?";

        ret = SQLPrepare(stmt, select, SQL_NTS);
        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        SQL_DATE_STRUCT outDate;
        SQLLEN outDateInd;

        SQL_TIME_STRUCT outTime;
        SQLLEN outTimeInd;

        SQL_TIMESTAMP_STRUCT outTimestamp;
        SQLLEN outTimestampInd;

        ret = SQLBindCol(stmt, 1, SQL_C_TYPE_DATE, &outDate, 0, &outDateInd);
        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ret = SQLBindCol(stmt, 2, SQL_C_TYPE_TIME, &outTime, 0, &outTimeInd);
        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ret = SQLBindCol(stmt, 3, SQL_C_TYPE_TIMESTAMP, &outTimestamp, 0, &outTimestampInd);
        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ret = SQLExecute(stmt);
        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLFetch(stmt);
        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECK_EQUAL(date.year, outDate.year);
        BOOST_CHECK_EQUAL(date.month, outDate.month);
        BOOST_CHECK_EQUAL(date.day, outDate.day);

        BOOST_CHECK_EQUAL(time.hour, outTime.hour);
        BOOST_CHECK_EQUAL(time.minute, outTime.minute);
        BOOST_CHECK_EQUAL(time.second, outTime.second);

        BOOST_CHECK_EQUAL(timestamp.year, outTimestamp.year);
        BOOST_CHECK_EQUAL(timestamp.month, outTimestamp.month);
        BOOST_CHECK_EQUAL(timestamp.day, outTimestamp.day);
        BOOST_CHECK_EQUAL(timestamp.hour, outTimestamp.hour);
        BOOST_CHECK_EQUAL(timestamp.minute, outTimestamp.minute);
        BOOST_CHECK_EQUAL(timestamp.second, outTimestamp.second);
        BOOST_CHECK_EQUAL(timestamp.fraction, outTimestamp.fraction);

        ret = SQLFreeStmt(stmt, SQL_CLOSE);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    /**
     * Check all date/time types.
     */
    void CheckDateTimeTimestamp(SQLINTEGER key, int year, int month, int day, int hour, int minute, int second, int fr)
    {
        SQL_TIMESTAMP_STRUCT timestamp;
        memset(&timestamp, 0, sizeof(timestamp));

        timestamp.year = year;
        timestamp.month = month;
        timestamp.day = day;
        timestamp.hour = hour;
        timestamp.minute = minute;
        timestamp.second = second;
        timestamp.fraction = fr;

        SQL_DATE_STRUCT date;
        memset(&date, 0, sizeof(date));

        date.year = timestamp.year;
        date.month = timestamp.month;
        date.day = timestamp.day;

        SQL_TIME_STRUCT time;
        memset(&time, 0, sizeof(time));

        time.hour = timestamp.hour;
        time.minute = timestamp.minute;
        time.second = timestamp.second;

        MergeSelectDateTime(key, date, time, timestamp);
    }

    /**
     * Check that time types work as intended in specified timezone.
     * @param timezone Timezone to check.
     */
    void CheckTimezone(const std::string& timezone)
    {
        IgniteConfiguration config;
        InitConfig(config, "queries-test.xml");

        std::string timezoneJvmOpt = "-Duser.timezone=" + timezone;
        std::replace(config.jvmOpts.begin(), config.jvmOpts.end(), std::string("-Duser.timezone=GMT"), timezoneJvmOpt);

        Ignite node = Ignition::Start(config, "NodeMain");

        Connect("DRIVER={Apache Ignite};SERVER=127.0.0.1;PORT=11110;SCHEMA=cache");

        CheckDateTimeTimestamp(1, 1999, 12, 31, 23, 55, 55, 0);
        CheckDateTimeTimestamp(2, 2000, 1, 1, 0, 5, 5, 0);
    }
};

BOOST_FIXTURE_TEST_SUITE(TypesTestSuite, TypesTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestZeroDecimal)
{
    Ignite node = StartPlatformNode("queries-test.xml", "NodeMain");

    Connect("DRIVER={Apache Ignite};SERVER=127.0.0.1;PORT=11110;SCHEMA=PUBLIC");

    SQLCHAR ddl[] = "CREATE TABLE IF NOT EXISTS TestTable "
        "(RecId varchar PRIMARY KEY, RecValue DECIMAL(4,2))"
        "WITH \"template=replicated, cache_name=TestTable_Cache\";";

    SQLRETURN ret = SQLExecDirect(stmt, ddl, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLCHAR dml[] = "INSERT INTO TestTable (RecId, RecValue) VALUES ('1', ?)";

    ret = SQLPrepare(stmt, dml, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQL_NUMERIC_STRUCT num;

    memset(&num, 0, sizeof(num));

    num.sign = 1;
    num.precision = 1;

    ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_NUMERIC, SQL_DECIMAL, 0, 0, &num, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLExecute(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFreeStmt(stmt, SQL_RESET_PARAMS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQL_NUMERIC_STRUCT num0;
    SQLLEN num0Len = static_cast<SQLLEN>(sizeof(num0));

    // Filling data to avoid acidental equality
    memset(&num0, 0xFF, sizeof(num0));

    ret = SQLBindCol(stmt, 1, SQL_C_NUMERIC, &num0, sizeof(num0), &num0Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLCHAR qry[] = "SELECT RecValue FROM TestTable WHERE RecId = '1'";

    ret = SQLExecDirect(stmt, qry, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(num.precision, num0.precision);
    BOOST_CHECK_EQUAL(num.scale, num0.scale);
    BOOST_CHECK_EQUAL(num.sign, num0.sign);

    BOOST_CHECK_EQUAL(num.val[0], num0.val[0]);
    BOOST_CHECK_EQUAL(num.val[1], num0.val[1]);
    BOOST_CHECK_EQUAL(num.val[2], num0.val[2]);
    BOOST_CHECK_EQUAL(num.val[3], num0.val[3]);
    BOOST_CHECK_EQUAL(num.val[4], num0.val[4]);
    BOOST_CHECK_EQUAL(num.val[5], num0.val[5]);
    BOOST_CHECK_EQUAL(num.val[6], num0.val[6]);
    BOOST_CHECK_EQUAL(num.val[7], num0.val[7]);
    BOOST_CHECK_EQUAL(num.val[8], num0.val[8]);
    BOOST_CHECK_EQUAL(num.val[9], num0.val[9]);
    BOOST_CHECK_EQUAL(num.val[10], num0.val[10]);
    BOOST_CHECK_EQUAL(num.val[11], num0.val[11]);
    BOOST_CHECK_EQUAL(num.val[12], num0.val[12]);
    BOOST_CHECK_EQUAL(num.val[13], num0.val[13]);
    BOOST_CHECK_EQUAL(num.val[14], num0.val[14]);
    BOOST_CHECK_EQUAL(num.val[15], num0.val[15]);
}

BOOST_AUTO_TEST_CASE(TestTimezoneUtc)
{
    CheckTimezone("UTC");
}

BOOST_AUTO_TEST_CASE(TestTimezoneGmt5)
{
    CheckTimezone("GMT+5");
}

BOOST_AUTO_TEST_CASE(TestTimezoneGmtM3)
{
    CheckTimezone("GMT-3");
}

BOOST_AUTO_TEST_SUITE_END()
