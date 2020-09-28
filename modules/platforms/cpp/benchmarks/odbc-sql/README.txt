GridGain ODBC-SQL Benchmark

This benchmark created to provide results that could be comparable with native IgniteSqlQueryBenchmark

For building instruction please refer to ../README.txt

Running benchmarks on Linux manually
----------------------------------

Benchmark only start clients. Prior to running this benchmark one should start server[s] and provide
their addresses using '--connection_string' option. The full list of options with their description can
be requested using '--help' option.

Building and running benchmarks using Docker
----------------------------------
1. Go to modules/platforms/cpp
$ cd modules/platforms/cpp

2. Run the following command (from modules/platforms/cpp) to build docker image:
$ docker build -f benchmarks/odbc-sql/Dockerfile --tag odbc-sql .

3. Run the following command to run docker image (from anywhere):
$ docker run odbc-sql [options]
