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

#include <stdint.h>

#include <sstream>
#include <iterator>

#include <boost/test/unit_test.hpp>

#include <ignite/common/utils.h>

#include "ignite/cache/cache.h"
#include "ignite/cache/query/query_cursor.h"
#include "ignite/cache/query/query_sql.h"
#include "ignite/cache/query/query_text.h"
#include "ignite/cache/query/query_sql_fields.h"
#include "ignite/ignite.h"
#include "ignite/ignition.h"
#include "ignite/test_utils.h"
#include "teamcity_messages.h"

using namespace boost::unit_test;

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::common;

using ignite::impl::binary::BinaryUtils;

/**
 * Ensure that GetNext() fails.
 *
 * @param cur Cursor.
 */
template<typename Cursor>
void CheckGetNextRowFail(Cursor& cur)
{
    BOOST_CHECK_EXCEPTION(cur.GetNext(), IgniteError, ignite_test::IsGenericError);
}

/**
 * Check single row through iteration.
 *
 * @param cur Cursor.
 * @param c1 First column.
 */
template<typename T1>
void CheckSingleRow(QueryFieldsCursor& cur, const T1& c1)
{
    BOOST_REQUIRE(cur.HasNext());

    QueryFieldsRow row = cur.GetNext();

    BOOST_REQUIRE_EQUAL(row.GetNext<T1>(), c1);

    BOOST_REQUIRE(!row.HasNext());
    BOOST_REQUIRE(!cur.HasNext());

    CheckGetNextRowFail(cur);
}

/**
 * Check single row through iteration.
 *
 * @param cur Cursor.
 * @param c1 First column.
 */
template<typename T1, typename T2>
void CheckSingleRow(QueryFieldsCursor& cur, const T1& c1, const T2& c2)
{
    BOOST_REQUIRE(cur.HasNext());

    QueryFieldsRow row = cur.GetNext();

    BOOST_REQUIRE_EQUAL(row.GetNext<T1>(), c1);
    BOOST_REQUIRE_EQUAL(row.GetNext<T2>(), c2);

    BOOST_REQUIRE(!row.HasNext());
    BOOST_REQUIRE(!cur.HasNext());

    CheckGetNextRowFail(cur);
}

static const std::string TABLE_NAME = "T1";

/**
 * Test setup fixture.
 */
struct CacheQuerySchemaTestSuiteFixture
{
    Ignite StartNode(const char* name)
    {
#ifdef IGNITE_TESTS_32
        return ignite_test::StartNode("cache-query-32.xml", name);
#else
        return ignite_test::StartNode("cache-query.xml", name);
#endif
    }

    /**
     * Constructor.
     */
    CacheQuerySchemaTestSuiteFixture() :
        grid(StartNode("Node1"))
    {
        // No-op.
    }

    /**
     * Destructor.
     */
    ~CacheQuerySchemaTestSuiteFixture()
    {
        Ignition::StopAll(true);
    }

    /** Perform SQL in cluster. */
    QueryFieldsCursor Sql(const std::string& sql)
    {
        return grid
            .GetOrCreateCache<int, int>("SchemaTestCache")
            .Query(SqlFieldsQuery(sql));
    }

    std::string TableName(bool withSchema)
    {
        return withSchema ? "PUBLIC." + TABLE_NAME : TABLE_NAME;
    }

    template<typename Predicate>
    void ExecuteStatementsAndVerify(Predicate& pred)
    {
        Sql("CREATE TABLE " + TableName(pred()) + " (id INT PRIMARY KEY, val INT)");

        Sql("CREATE INDEX t1_idx_1 ON " + TableName(pred()) + "(val)");

        Sql("INSERT INTO " + TableName(pred()) + " (id, val) VALUES(1, 2)");

        QueryFieldsCursor cursor = Sql("SELECT * FROM " + TableName(pred()));
        CheckSingleRow<int32_t, int32_t>(cursor, 1, 2);

        Sql("UPDATE " + TableName(pred()) + " SET val = 5");
        cursor = Sql("SELECT * FROM " + TableName(pred()));
        CheckSingleRow<int32_t, int32_t>(cursor, 1, 5);

        Sql("DELETE FROM " + TableName(pred()) + " WHERE id = 1");
        cursor = Sql("SELECT COUNT(*) FROM " + TableName(pred()));
        CheckSingleRow<int64_t>(cursor, 0);

        cursor = Sql("SELECT COUNT(*) FROM SYS.TABLES WHERE schema_name = 'PUBLIC' "
            "AND table_name = \'" + TABLE_NAME + "\'");
        CheckSingleRow<int64_t>(cursor, 1);

        Sql("DROP TABLE " + TableName(pred()));
    }

    /** Node started during the test. */
    Ignite grid;
};

BOOST_FIXTURE_TEST_SUITE(CacheQuerySchemaTestSuite, CacheQuerySchemaTestSuiteFixture)

bool TruePred()
{
    return true;
}

BOOST_AUTO_TEST_CASE(TestBasicOpsExplicitPublicSchema)
{
    ExecuteStatementsAndVerify(TruePred);
}

bool FalsePred()
{
    return false;
}

BOOST_AUTO_TEST_CASE(TestBasicOpsImplicitPublicSchema)
{
    ExecuteStatementsAndVerify(FalsePred);
}

struct MixedPred
{
    int i;

    MixedPred() : i(0)
    {
        // No-op.
    }

    bool operator()()
    {
        return (++i & 1) == 0;
    }
};

BOOST_AUTO_TEST_CASE(TestBasicOpsMixedPublicSchema)
{
    MixedPred pred;

    ExecuteStatementsAndVerify(pred);
}

BOOST_AUTO_TEST_SUITE_END()
