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
#else
#   include <thread>
#   include <sys/time.h>
#   include <unistd.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include <stdint.h>
#include <cstdlib>

#include <iostream>
#include <iomanip>
#include <vector>
#include <string>
#include <sstream>
#include <exception>

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

#ifdef _WIN32
uint64_t GetMicros()
{
    FILETIME ft;
    GetSystemTimeAsFileTime(&ft);
    uint64_t tt = ft.dwHighDateTime;
    tt <<=32;
    tt |= ft.dwLowDateTime;

    return tt / 10;
}
#else
uint64_t GetMicros()
{
    struct timeval now;

    gettimeofday(&now, NULL);

    return (now.tv_sec) * 1000 + now.tv_usec / 1000;
}

void Sleep(unsigned ms)
{
    usleep(ms * 1000);
}

#endif

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

    if (!SQL_SUCCEEDED(ret))
        ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to execute query");

    // Releasing statement handle.
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

inline unsigned GetRandomUnsigned(unsigned max)
{
    return static_cast<unsigned>(rand()) % max;
}

inline char GetRandomChar()
{
    static std::string alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    return alphabet[static_cast<size_t>(rand()) % alphabet.size()];
}

std::string GetRandomString(size_t size)
{
    std::string res;

    res.reserve(size + 2);

    res.push_back('\'');

    for (size_t i = 0; i < size; ++i)
        res.push_back(GetRandomChar());
    
    res.push_back('\'');

    return res;
}

void Initialize(SQLHDBC dbc, unsigned departmentsNum, unsigned employeesNum)
{
    ExecuteNoFetch(dbc, "drop table if exists EMPLOYEES");
    ExecuteNoFetch(dbc, "drop table if exists DEPARTMENTS");

    ExecuteNoFetch(dbc, ""
        "create table EMPLOYEES("
            "FIRST_NAME varchar,"
            "LAST_NAME varchar,"
            "EMAIL varchar,"
            "PHONE_NUMBER varchar,"
            "HIRE_DATE varchar,"
            "JOB_ID varchar,"
            "SALARY float,"
            "COMMISSION_PCT float,"
            "MANAGER_ID int,"
            "DEPARTMENT_ID smallint,"
            "EMPLOYEE_ID int primary key"
        ")");

    ExecuteNoFetch(dbc, ""
        "create table DEPARTMENTS("
            "DEPARTMENT_NAME varchar,"
            "MANAGER_ID int,"
            "LOCATION_ID smallint,"
            "DEPARTMENT_ID smallint primary key"
        ")");

    for (unsigned i = 0; i < departmentsNum; ++i)
    {
        // This is slow but this is does not matter for us.
        std::stringstream queryCtor;

        queryCtor <<
            "MERGE INTO DEPARTMENTS("
                "DEPARTMENT_NAME,"
                "MANAGER_ID,"
                "LOCATION_ID,"
                "DEPARTMENT_ID"
            ") VALUES("
                << GetRandomString(16) << ','
                << GetRandomUnsigned(employeesNum) << ','
                << GetRandomUnsigned(departmentsNum) << ','
                << (i + 1) << 
            ')';

        std::string query = queryCtor.str();

        ExecuteNoFetch(dbc, query);
    }
    
    for (unsigned i = 0; i < employeesNum; ++i)
    {
        // This is slow but this is does not matter for us.
        std::stringstream queryCtor;

        queryCtor <<
            "MERGE INTO EMPLOYEES("
                "FIRST_NAME,"
                "LAST_NAME,"
                "EMAIL,"
                "PHONE_NUMBER,"
                "HIRE_DATE,"
                "JOB_ID,"
                "SALARY,"
                "COMMISSION_PCT,"
                "MANAGER_ID,"
                "DEPARTMENT_ID,"
                "EMPLOYEE_ID"
            ") VALUES("
                << GetRandomString(10) << ','
                << GetRandomString(12) << ','
                << GetRandomString(16) << ','
                << GetRandomString(15) << ','
                << GetRandomString(10) << ','
                << GetRandomString(64) << ','
                << GetRandomUnsigned(200000) << ','
                << GetRandomUnsigned(200000) << ','
                << GetRandomUnsigned(employeesNum) << ','
                << GetRandomUnsigned(departmentsNum) << ','
                << (i + 1) << 
            ')';

        std::string query = queryCtor.str();

        ExecuteNoFetch(dbc, query);
    }
}

void Initialize(const std::string& addr, unsigned departmentsNum, unsigned employeesNum)
{
    SQLHENV env;

    // Allocate an environment handle
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

    // We want ODBC 3 support
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3), 0);

    SQLHDBC dbc;

    // Allocate a connection handle
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    // Combining connect string
    std::string connectStr = "DRIVER={Apache Ignite};ADDRESS=" + addr + ";SCHEMA=PUBLIC;";

    SQLCHAR outstr[ODBC_BUFFER_SIZE];
    SQLSMALLINT outstrlen;

    // Connecting to ODBC server.
    SQLRETURN ret = SQLDriverConnect(dbc, NULL, reinterpret_cast<SQLCHAR*>(&connectStr[0]),
        static_cast<SQLSMALLINT>(connectStr.size()), outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

    if (!SQL_SUCCEEDED(ret))
        ThrowOdbcError(SQL_HANDLE_DBC, dbc, "Failed to connect");

    Initialize(dbc, departmentsNum, employeesNum);

    // Disconnecting from the server.
    SQLDisconnect(dbc);

    // Releasing allocated handles.
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}


#ifdef _WIN32
class Thread
{
public:
    Thread() :
        lastMicros(0),
        handle(INVALID_HANDLE_VALUE),
        running(false)
    {
        // No-op.
    }

    virtual ~Thread()
    {
        Stop();
        Join();
        Close();
    }

    void Start()
    {
        if (running)
            return;

        Close();

        running = true;

        handle = CreateThread(NULL, 0, &Thread::Run, this, 0, 0);
    }

    void Stop()
    {
        if (running)
        {
            running = false;
        }
    }

    void Join()
    {
        if (handle != INVALID_HANDLE_VALUE)
        {
            WaitForSingleObject(handle, INFINITE);
        }
    }

    uint64_t GetLastTaskDuration() const
    {
        return lastMicros;
    }

    virtual void Init()
    {
        // No-op.
    }

    virtual void Deinit()
    {
        // No-op.
    }

    virtual void Task() = 0;

private:
    static DWORD Run(void* vself)
    {
        Thread* self = static_cast<Thread*>(vself);

        try
        {
            self->Init();

            while (self->running)
            {
                uint64_t begin = GetMicros();

                self->Task();
                
                uint64_t end = GetMicros();

                self->lastMicros = end - begin;
            }

            self->Deinit();
        }
        catch (std::exception& err)
        {
            std::cout << "An error occurred: " << err.what() << std::endl;

            throw;
        }
        catch (...)
        {
            std::cout << "An unknown error occurred" << std::endl;

            throw;
        }

        return 0;
    }

    void Close()
    {
        if (handle != INVALID_HANDLE_VALUE)
        {
            CloseHandle(handle);

            handle = INVALID_HANDLE_VALUE;
        }
    }

    volatile uint64_t lastMicros;

    HANDLE handle;

    volatile bool running;
};
#else
class Thread
{
public:
    Thread() :
        lastMicros(0),
        running(false)
    {
        // No-op.
    }

    virtual ~Thread()
    {
        Stop();
        Join();
        Close();
    }

    void Start()
    {
        if (running)
            return;

        Close();

        running = true;

        thread.reset(new std::thread(&Thread::Run, this));
    }

    void Stop()
    {
        if (running)
        {
            running = false;
        }
    }

    void Join()
    {
        if (thread)
            thread->join();
    }

    uint64_t GetLastTaskDuration() const
    {
        return lastMicros;
    }

    virtual void Init()
    {
        // No-op.
    }

    virtual void Deinit()
    {
        // No-op.
    }

    virtual void Task() = 0;

private:
    static void Run(void* vself)
    {
        Thread* self = static_cast<Thread*>(vself);

        try
        {
            self->Init();

            while (self->running)
            {
                uint64_t begin = GetMicros();

                self->Task();

                uint64_t end = GetMicros();

                self->lastMicros = end - begin;
            }

            self->Deinit();
        }
        catch (std::exception& err)
        {
            std::cout << "An error occurred: " << err.what() << std::endl;

            throw;
        }
        catch (...)
        {
            std::cout << "An unknown error occurred" << std::endl;

            throw;
        }
    }

    void Close()
    {
        if (thread)
            thread.reset();
    }

    volatile uint64_t lastMicros;

    std::unique_ptr<std::thread> thread;

    volatile bool running;
};
#endif

class OdbcClient : public Thread
{
public:
    OdbcClient(const std::string& addr) :
        addr(addr),
        env(NULL),
        dbc(NULL)
    {
        // No-op.
    }

#ifndef _WIN32
    OdbcClient(const OdbcClient& other) :
        addr(other.addr),
        env(other.env),
        dbc(other.dbc)
    {
        // No-op.
    }

    OdbcClient operator=(const OdbcClient& other)
    {
        addr = other.addr;
        env = other.env;
        dbc = other.dbc;

        return *this;
    }

    OdbcClient(OdbcClient&& other) :
        addr(other.addr),
        env(other.env),
        dbc(other.dbc)
    {
        other.env = NULL;
        other.dbc = NULL;
    }
#endif
    
    virtual void Init()
    {
        // Allocate an environment handle
        SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

        // We want ODBC 3 support
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3), 0);

        // Allocate a connection handle
        SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

        // Combining connect string
        std::string connectStr = "DRIVER={Apache Ignite};ADDRESS=" + addr + ";SCHEMA=PUBLIC;";

        SQLCHAR outstr[ODBC_BUFFER_SIZE];
        SQLSMALLINT outstrlen;

        // Connecting to ODBC server.
        SQLRETURN ret = SQLDriverConnect(dbc, NULL, reinterpret_cast<SQLCHAR*>(&connectStr[0]),
            static_cast<SQLSMALLINT>(connectStr.size()), outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_DBC, dbc, "Failed to connect");
    }

    virtual void Deinit()
    {
        // Disconnecting from the server.
        SQLDisconnect(dbc);

        // Releasing allocated handles.
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);
    }

    virtual void Task()
    {
        ExecuteAndFetch(dbc, "SELECT DEPARTMENTS.DEPARTMENT_NAME AS DEPARTMENT_NAME FROM EMPLOYEES EMPLOYEES "
            "  INNER JOIN DEPARTMENTS DEPARTMENTS ON (EMPLOYEES.DEPARTMENT_ID = DEPARTMENTS.DEPARTMENT_ID) GROUP BY DEPARTMENT_NAME");

        // ExecuteAndFetch(dbc, "SELECT DEPARTMENTS.DEPARTMENT_NAME AS DEPARTMENT_NAME, "
        //     "  EMPLOYEES.EMPLOYEE_ID AS EMPLOYEE_ID FROM EMPLOYEES EMPLOYEES "
        //     "  INNER JOIN DEPARTMENTS DEPARTMENTS ON (EMPLOYEES.DEPARTMENT_ID = DEPARTMENTS.DEPARTMENT_ID) GROUP BY DEPARTMENT_NAME,   EMPLOYEE_ID");
        //
        // ExecuteAndFetch(dbc, "SELECT DEPARTMENTS.DEPARTMENT_NAME AS DEPARTMENT_NAME, "
        //     "  EMPLOYEES.EMPLOYEE_ID AS EMPLOYEE_ID,   SUM(EMPLOYEES.SALARY) AS sum_SALARY_ok FROM EMPLOYEES EMPLOYEES "
        //     "  INNER JOIN DEPARTMENTS DEPARTMENTS ON (EMPLOYEES.DEPARTMENT_ID = DEPARTMENTS.DEPARTMENT_ID) GROUP BY DEPARTMENT_NAME,   EMPLOYEE_ID");
    }

private:
    std::string addr;

    SQLHENV env;

    SQLHDBC dbc;
};


class Measurer: public Thread
{
public:
    Measurer(std::vector<OdbcClient>& clients) :
        clients(clients)
    {
        // No-op.
    }

    virtual void Task()
    {
        Sleep(1000);

        uint64_t sum = 0;
        for (size_t i = 0; i < clients.size(); ++i)
            sum += clients[i].GetLastTaskDuration();

        std::cout << "Average latency: " << (sum / clients.size()) << "us" << std::endl;
    }

private:
    const std::vector<OdbcClient>& clients;
};

template<typename TO, typename TI>
TO LexicalCast(const TI& in)
{
    TO out;

    std::stringstream converter;
    converter << in;
    converter >> out;

    return out;
}

template<typename OutType = std::string>
OutType GetEnvVar(const std::string& name, const OutType& dflt)
{
    char* var = std::getenv(name.c_str());

    if (!var)
        return dflt;

    return LexicalCast<OutType>(var);
}

/**
 * Program entry point.
 *
 * @return Exit code.
 */
int main()
{
    const unsigned departmentsNum = GetEnvVar<unsigned>("DEPARTMENTS_NUM", 11);
    const unsigned employeesNum = GetEnvVar<unsigned>("EMPLOYEES_NUM", 1083);
    const unsigned clientNum = GetEnvVar<unsigned>("CLIENTS_NUM", 100);
    const std::string address = GetEnvVar<std::string>("NODES_ADDRESS", "localhost");

    std::cout << "DEPARTMENTS_NUM=" << departmentsNum << std::endl;
    std::cout << "EMPLOYEES_NUM=" << employeesNum << std::endl;
    std::cout << "CLIENTS_NUM=" << clientNum << std::endl;
    std::cout << "NODES_ADDRESS=" << address << std::endl;

    int exitCode = 0;

    srand(static_cast<unsigned>(time(NULL)));

    try
    {
        std::cout << "Initializing..." << std::endl;

        Initialize(address, departmentsNum, employeesNum);
        
        std::cout << "Done" << std::endl;

        std::cout << "Starting threads..." << std::endl;

        std::vector<OdbcClient> clients;

        clients.resize(clientNum, OdbcClient(address));

        for (size_t i = 0; i < clients.size(); ++i)
            clients[i].Start();

        Measurer measurer(clients);

        measurer.Start();

        std::cout << "Done" << std::endl;

        std::cout << "Press 'Enter' to stop the benchmark" << std::endl;

        std::cin.get();
        
        std::cout << "Stopping clients..." << std::endl;

        measurer.Stop();

        for (size_t i = 0; i < clients.size(); ++i)
            clients[i].Stop();
        
        for (size_t i = 0; i < clients.size(); ++i)
            clients[i].Join();

        std::cout << "Done" << std::endl;

        std::cout << "Benchmark is finished" << std::endl;
    }
    catch (std::exception& err)
    {
        std::cout << "An error occurred: " << err.what() << std::endl;

        exitCode = 1;
    }

    return exitCode;
}
