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

#include <ignite/benchmarks/odbc_utils.h>
#include <ignite/benchmarks/odbc_benchmark.h>
#include <ignite/benchmarks/replicate_thread_runner.h>

#include <stdint.h>
#include <cstdlib>

#include <iostream>
#include <iomanip>
#include <vector>
#include <string>
#include <sstream>
#include <exception>

struct Person
{
    SQLINTEGER id;
    odbc_utils::OdbcStringBuffer firstName;
    odbc_utils::OdbcStringBuffer lastName;
    SQLDOUBLE salary;
};

std::ostream& operator<<(std::ostream& os, const Person& val)
{
    std::string firstName(reinterpret_cast<const char*>(val.firstName.buffer), static_cast<size_t>(val.firstName.reallen));
    std::string lastName(reinterpret_cast<const char*>(val.firstName.buffer), static_cast<size_t>(val.firstName.reallen));

    os << "[id=" << val.id << ",firstName=" << firstName << ",lastName=" << lastName << ",salary=" << val.salary << ']';

    return os;
}

class OdbcSqlBenchmark : public OdbcBenchmark
{
public:
    OdbcSqlBenchmark()
    {
        // No-op.
    }

    virtual ~OdbcSqlBenchmark()
    {
        // No-op.
    }

    virtual void LoadData()
    {
        if (GetConfig().cacheName == "PUBLIC")
        {
            odbc_utils::ExecuteNoFetch(dbc, "DROP TABLE IF EXISTS PUBLIC.Person");
            odbc_utils::ExecuteNoFetch(dbc, "CREATE TABLE PUBLIC.Person ("
                                            "   id int PRIMARY KEY, "
                                            "   orgId int, "
                                            "   firstName varchar, "
                                            "   lastName varchar, "
                                            "   salary double"
                                            ") WITH \""
                                            "   template=partitioned, "
                                            "   atomicity=ATOMIC, "
                                            "   backups=1, "
                                            "   key_type=java.lang.Integer"
                                            "\";");

            odbc_utils::ExecuteNoFetch(dbc, "CREATE INDEX ON PUBLIC.Person (id)");

            odbc_utils::ExecuteNoFetch(dbc, "CREATE INDEX ON PUBLIC.Person (orgId)");

            odbc_utils::ExecuteNoFetch(dbc, "CREATE INDEX ON PUBLIC.Person (salary)");

            odbc_utils::ExecuteNoFetch(dbc, "CREATE INDEX ON PUBLIC.Person (id, salary)");
        }

        std::cout << "Cleaning table" << std::endl;
        odbc_utils::ExecuteNoFetch(dbc, "DELETE FROM " + fullTableName);

        std::cout << "Filling table..." << std::endl;
        for (int32_t i = GetConfig().cacheRangeBegin; i < GetConfig().cacheRangeEnd; ++i)
        {
            std::stringstream query;
            query << "INSERT INTO " << fullTableName << " "
                     "(_key, id, firstName, lastName, salary) "
                     "VALUES (" << i << ", " << i << ", 'firstName" << i << "', 'lastName" << i << "', " << i * 1000 << ')';

            odbc_utils::ExecuteNoFetch(dbc, query.str());
        }

        std::cout << "Done" << std::endl;
    }

    virtual void SetUp()
    {
        OdbcBenchmark::SetUp();

        if (GetConfig().cacheName != "PUBLIC")
            fullTableName = '"' + GetConfig().cacheName + '"' + ".Person";
        else
            fullTableName = "PUBLIC.Person";

        std::string selectQuery0 = "SELECT salary, id, firstName, lastName "
                                   "FROM " + fullTableName + " "
                                   "WHERE salary >= ? and salary <= ?";

        selectQuery.assign(selectQuery0.begin(), selectQuery0.end());

        persons.reserve(1024);
    }

    virtual void DoAction()
    {
        double salary = NextRandomDouble(GetConfig().cacheRangeBegin, GetConfig().cacheRangeEnd) * 1000;

        double maxSalary = salary + 1000.0;

        fetchPersonsBetween(salary, maxSalary);

        for (std::vector<Person>::iterator it = persons.begin(); it != persons.end(); ++it)
        {
            if (it->salary < salary || it->salary > maxSalary)
            {
                std::stringstream msg;
                msg << "Invalid person retrieved [min=" << salary << ", max=" << maxSalary << ", person=" << *it << ']';

                throw std::runtime_error(msg.str());
            }
        }
    }

    void fetchPersonsBetween(SQLDOUBLE minSalary, SQLDOUBLE maxSalary)
    {
        SQLRETURN ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, &minSalary, 0, 0);
        odbc_utils::CheckOdbcOperation(ret, SQL_HANDLE_STMT, stmt, "Failed to bind 1st param");

        ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, &maxSalary, 0, 0);
        odbc_utils::CheckOdbcOperation(ret, SQL_HANDLE_STMT, stmt, "Failed to bind 2nd param");

        ret = SQLExecDirect(stmt, &selectQuery[0], selectQuery.size());
        odbc_utils::CheckOdbcOperation(ret, SQL_HANDLE_STMT, stmt, "Failed to execute query");

        persons.clear();
        while (true)
        {
            persons.resize(persons.size() + 1);
            Person& current = persons.back();

            ret = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &current.salary, 0, 0);
            odbc_utils::CheckOdbcOperation(ret, SQL_HANDLE_STMT, stmt, "Failed to bind 1st collumn");

            ret = SQLBindCol(stmt, 2, SQL_C_SLONG, &current.id, 0, 0);
            odbc_utils::CheckOdbcOperation(ret, SQL_HANDLE_STMT, stmt, "Failed to bind 2nd collumn");

            ret = SQLBindCol(stmt, 3, SQL_C_SLONG, &current.firstName.buffer, odbc_utils::ODBC_BUFFER_SIZE, &current.firstName.reallen);
            odbc_utils::CheckOdbcOperation(ret, SQL_HANDLE_STMT, stmt, "Failed to bind 3rd collumn");

            ret = SQLBindCol(stmt, 4, SQL_C_SLONG, &current.lastName.buffer, odbc_utils::ODBC_BUFFER_SIZE, &current.lastName.reallen);
            odbc_utils::CheckOdbcOperation(ret, SQL_HANDLE_STMT, stmt, "Failed to bind 4th collumn");

            ret = SQLFetch(stmt);
            if (ret == SQL_NO_DATA)
            {
                persons.pop_back();

                break;
            }

            odbc_utils::CheckOdbcOperation(ret, SQL_HANDLE_STMT, stmt, "Failed to fetch row");
        }

        ret = SQLCloseCursor(stmt);
        odbc_utils::CheckOdbcOperation(ret, SQL_HANDLE_STMT, stmt, "Failed to close cursor");
    }

private:
    /** Full table name including schema. */
    std::string fullTableName;

    /** Pre-made select query. */
    std::vector<SQLCHAR> selectQuery;

    /** Array for fetched values. */
    std::vector<Person> persons;
};

/**
 * Program entry point.
 *
 * @return Exit code.
 */
int main()
{
    int exitCode = 0;

    try
    {
        ReplicateThreadRunner<OdbcSqlBenchmark> runner;

        runner.Run();

        std::cout << "Done" << std::endl;
    }
    catch (std::exception& err)
    {
        std::cout << "An error occurred: " << err.what() << std::endl;

        exitCode = 1;
    }

    std::cout << "Benchmark is finished" << std::endl;

    return exitCode;
}
