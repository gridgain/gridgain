GridGain C++ Benchmarks
==================================

Common requirements
----------------------------------
 * Java Development Kit (JDK) must be installed: https://java.com/en/download/index.jsp
 * JAVA_HOME environment variable must be set pointing to Java installation directory.
 * IGNITE_HOME environment variable must be set to GridGain installation directory.
 * GridGain must be built and packaged using Maven. You can use the following Maven command: mvn clean package -DskipTests
 * GridGain C++ must be built according to instructions for your platform. Refer to
   $IGNITE_HOME/platforms/cpp/DEVNOTES.txt for instructions.
 * For ODBC benchmarks additionally ODBC Driver Manager must be present and installed on your platform and
   GridGain ODBC driver must be built and installed according to instructions for your platform. Refer to
   $IGNITE_HOME/platforms/cpp/DEVNOTES.txt for build instructions and to $IGNITE_HOME/platforms/cpp/odbc/README.txt.
   for installation instructions.

Building and benchmarks on Linux manually
----------------------------------

Prerequisites:
 * GCC, g++, autotools, automake, and libtool must be installed.

To build benchmarks execute the following commands one by one from benchmarks root directory:
 * libtoolize && aclocal && autoheader && automake --add-missing && autoreconf
 * ./configure
 * make

As a result executables will appear in every benchmark's directory.

For running instructions refer to benchmarks/<benchmark_name>/README.txt

Building and running benchmarks using Docker
----------------------------------
1. Go to modules/platforms/cpp
$ cd modules/platforms/cpp

2. Run the following command (from modules/platforms/cpp) to build docker image:
$ docker build -f benchmarks/<benchmark_name>/Dockerfile --tag <benchmark_tag> .

3. Run the following command to run docker image (from anywhere):
$ docker run <benchmark_tag> [options]
