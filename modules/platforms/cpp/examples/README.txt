GridGain C++ Examples
==================================

Common requirements
----------------------------------
 * Java Development Kit (JDK) must be installed: https://java.com/en/download/index.jsp
 * JAVA_HOME environment variable must be set pointing to Java installation directory.
 * IGNITE_HOME environment variable must be set to GridGain installation directory.
 * GridGain must be built and packaged using Maven. You can use the following Maven command: mvn clean package -DskipTests
 * GridGain C++ must be built according to instructions for your platform. Refer to
   $IGNITE_HOME/platforms/cpp/DEVNOTES.txt for instructions.
 * For odbc-example additionally ODBC Driver Manager must be present and installed on your platform and
   GridGain ODBC driver must be built and installed according to instructions for your platform. Refer to
   $IGNITE_HOME/platforms/cpp/DEVNOTES.txt for build instructions and to $IGNITE_HOME/platforms/cpp/odbc/README.txt.
   for installation instructions.

Building on Linux with Autotools
----------------------------------
Prerequisites:
 * GCC, g++, autotools, automake, and libtool must be installed.

To build examples execute the following commands one by one from examples root directory:
 * libtoolize && aclocal && autoheader && automake --add-missing && autoreconf
 * ./configure
 * make

Bulding with CMake
----------------------------------

Prerequisites:
 * C++ compiler and SDK:
  Linux and Mac OS X:
   * clang >= 3.9 or gcc >= 3.6
  Windows:
   * Visual Studio 2010 or later
   * Windows SDK 7.1 or later
 * CMake >= 3.6 must be installed
 * IMPORTANT: Apache Ignite C++ should be installed. Refer to $IGNITE_HOME/platforms/cpp/DEVNOTES.txt for instructions.

To build examples execute the following commands one by one from examples root directory:
 * mkdir cmake-build-[debug|release]
 * cd ./cmake-build-[debug|release]
 * run CMake configuration:
  * on Linux or Mac OS X:
     cmake .. -DCMAKE_BUILD_TYPE=[Release|Debug] [-DIGNITE_CPP_DIR=<ignite_install_dir>]
  * on Windows:
     cmake .. -DCMAKE_GENERATOR_PLATFORM=[Win32|x64] -DCMAKE_BUILD_TYPE=[Release|Debug]
           -DIGNITE_CPP_DIR=<ignite_install_dir>

    IMPORTANT: Ignite C++ should be built and installed for this command to work correctly. If you have installed
    Ignite C++ in non-default directory or getting "IGNITE_INCLUDE_DIR-NOTFOUND" errors while executing this command,
    you should make sure you've set IGNITE_CPP_DIR option correctly, pointing to the installation directory of
    Ignite C++ (it should be the same path that was used in CMAKE_INSTALL_PREFIX during Ignite C++ installation)

 * cmake --build . --config [Release|Debug]

CMake by default generate on Windows Visual Studio projects. You can find generated projects in CMake
build directory (./cmake-build-[release|debug]) and open examples.sln in Visual Studio.

As a result executables will appear in every example's directory.

Running examples.
----------------------------------

Before running examples ensure that:
 * LD_LIBRARY_PATH environment variable is set and pointing to a directory with "libjvm.so" library. Typically, this
   library is located in $JAVA_HOME/jre/lib/amd64/server directory.
 * For odbc-example additionally ODBC Driver Manager must be present and installed on your platform and
   GridGain ODBC driver must be built and installed according to instructions for your platform.
 * For odbc-example make sure that path to GridGain libraries is added to LD_LIBRARY_PATH (usually it is /usr/local/lib).


Importing CMake projects to Visual Studio (tm) (since 2015):
------------------------------------------------------------
 Use CMakeSettings.json.in files in examples root directory as a template of real CMakeSettings.json.
 Edit it manually to set up correct environment variables and import CMake projects as usual.
