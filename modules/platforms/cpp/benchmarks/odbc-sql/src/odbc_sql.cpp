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
#include <boost/program_options.hpp>

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

/**
 * The Person struct.
 */
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

/**
 * ODBC SQL Benchmark.
 *
 * It's implemented to match native SQL benchmark as best as possible using ODBC.
 */
class OdbcSqlBenchmark : public benchmark::OdbcBenchmark
{
public:
    OdbcSqlBenchmark(boost::shared_ptr<const ConfigType> cfg) :
        OdbcBenchmark(cfg)
    {
        // No-op.
    }

    virtual ~OdbcSqlBenchmark()
    {
        // No-op.
    }

    bool IsDataLoaded()
    {
        try
        {
            int64_t expectedRows = config->cacheRangeEnd - config->cacheRangeBegin;

            SQLHSTMT stmt = odbc_utils::Execute(dbc, "SELECT COUNT(*) FROM " + fullTableName);

            odbc_utils::NextRow(stmt);
            int64_t rowsActual = odbc_utils::GetSigleColumn<int64_t>(stmt, 1);

            SQLFreeHandle(SQL_HANDLE_STMT, stmt);

            return expectedRows == rowsActual;
        }
        catch (...)
        {
            return false;
        }
    }

    virtual void LoadData()
    {
        if (IsDataLoaded())
            return;

        odbc_utils::OdbcLock loadLock(dbc, "load_lock");

        bool locked = loadLock.TimedLock(60);

        if (!locked)
            throw std::runtime_error("It takes too long to load data. Possible data corruption. Try restarting cluster.");

        if (IsDataLoaded())
            return;

        std::cout << "Loading data to the server" << std::endl;

        if (config->cacheName == "PUBLIC")
        {
            std::cout << "Schema is 'PUBLIC'. Creating table and indices..." << std::endl;

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

        std::cout << "Cleaning table..." << std::endl;
        odbc_utils::ExecuteNoFetch(dbc, "DELETE FROM " + fullTableName);

        std::cout << "Filling table..." << std::endl;
        for (int32_t i = config->cacheRangeBegin; i < config->cacheRangeEnd; ++i)
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

        if (config->cacheName != "PUBLIC")
            fullTableName = '"' + config->cacheName + '"' + ".Person";
        else
            fullTableName = "PUBLIC.Person";

        std::string selectQuery0 = "SELECT salary, id, firstName, lastName "
                                   "FROM " + fullTableName + " "
                                   "WHERE salary >= ? and salary <= ?";

        selectQuery.assign(selectQuery0.begin(), selectQuery0.end());

        persons.reserve(1024);
    }

    virtual void CleanUp()
    {
        OdbcBenchmark::CleanUp();
    }

    virtual void DoAction()
    {
        double salary = NextRandomDouble(config->cacheRangeBegin, config->cacheRangeEnd) * 1000;

        double maxSalary = salary + 1000.0;

        FetchPersonsBetween(salary, maxSalary);

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

    void FetchPersonsBetween(SQLDOUBLE minSalary, SQLDOUBLE maxSalary)
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

class OdbcSqlBenchmarkFactory : public benchmark::BenchmarkFactory<OdbcSqlBenchmark>
{
public:
    /**
     * Constructor.
     *
     *  @param
     */
    OdbcSqlBenchmarkFactory(const benchmark::OdbcBenchmarkConfig& cfg) :
        config(new benchmark::OdbcBenchmarkConfig(cfg))
    {
        // No-op.
    }

    /**
     * Construct a new instance of the benchmark.
     *
     * @return New instance of the benchmark.
     */
    virtual boost::shared_ptr<OdbcSqlBenchmark> Construct()
    {
        return boost::make_shared<OdbcSqlBenchmark>(config);
    }

private:
    boost::shared_ptr<const benchmark::OdbcBenchmarkConfig> config;
};

/**
 * Program entry point.
 *
 * @return Exit code.
 */
int main(int argc, char *argv[])
{
    int exitCode = 0;

    try
    {
        using namespace boost::program_options;

        options_description desc("Options");

        desc.add_options()
            ("help,h", "Help screen.")

            ("connection_string,s", value<std::string>()->required(),
             "Used as a connection string for ODBC client. Example: DRIVER={Apache Ignite};ADDRESS=127.0.0.1.")

            ("duration,d", value<int32_t>()->default_value(600),
             "Benchmark duration, in seconds. Does no include warmup duration.")

            ("warmup,w", value<int32_t>()->default_value(0),
             "Warmup duration in seconds. During warmup no data is printed.")

            ("threads,t", value<int32_t>()->default_value(64),
             "Number of thread to use from a single client when performing benchmark.")

            ("cache,n", value<std::string>()->default_value("PUBLIC"),
             "Name of the cache to use. If 'PUBLIC' cache is used all neccessary tables created by client. If not, "
             "cache should be already created.")

            ("range,r", value<std::string>()->default_value("1-10000"),
             "Range of keys in cache to be used by benchmark. Format is '<begin>-<end>', where <begin> and <end> are "
             "numbers and <begin> less than <end>.");

        variables_map vm;

        store(parse_command_line(argc, argv, desc), vm);

        if (vm.count("help"))
        {
            std::cout << desc << std::endl;

            return 0;
        }

        notify(vm);

        benchmark::OdbcBenchmarkConfig odbcCfg = benchmark::OdbcBenchmarkConfig::GetFromVm(vm);
        OdbcSqlBenchmarkFactory factory(odbcCfg);

        benchmark::RunnerConfig runnerCfg = benchmark::RunnerConfig::GetFromVm(vm);
        benchmark::ReplicateThreadRunner<OdbcSqlBenchmark> runner(runnerCfg, factory);

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
