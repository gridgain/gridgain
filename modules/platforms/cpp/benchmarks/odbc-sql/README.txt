GridGain ODBC-SQL Benchmark

This benchmark created to provide results that could be comparable with native IgniteSqlQueryBenchmark

For building instruction please refer to ../README.txt

Running benchmarks on Linux manually
----------------------------------

Benchmark only starts clients. Prior to running this benchmark one should start server[s] and provide
their addresses using CONNECTION_STRING evn variable.

The following environment variables should be set before running benchmark:

1. CONNECTION_STRING - used as a connection string for ODBC client. By default will try to connect to localhost

2. THREAD_CNT - Number of threads to use from a single client when performing operations. Default value: 64

3. WARMUP - Number of seconds to warmup and do not produce any results. Default value is 60.

4. DURATION - Duration of benchmark in seconds. Default value is 600.

5. CACHE_NAME - Name of the cache to use. If "PUBLIC" cache is used all neccessary tables created by client. If not,
cache should be already created. Default value is "PUBLIC".

6. CACHE_RANGE - Range of keys in cache to be used by benchmark. Format is "<begin>-<end>", where <begin> and <end> are
numbers and <begin> < <end>. Default value is "0-10000".

Building and running benchmarks using Docker
----------------------------------
1. Go to modules/platforms/cpp
$ cd modules/platforms/cpp

2. Edit benchmarks/odbc-sql/Dockerfile if needed (to change arguments)

3. Run the following command (from modules/platforms/cpp) to build docker image:
$ docker build -f benchmarks/odbc-sql/Dockerfile --tag odbc-sql .

4. Run the following command to run docker image (from anywhere):
$ docker run odbc-sql
