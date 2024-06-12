@echo off
setlocal

if defined IGNITE_SCRIPT_STRICT_MODE (
    setlocal EnableDelayedExpansion
)

:: Copyright 2019 GridGain Systems, Inc. and Contributors.
:: Licensed under the GridGain Community Edition License (the "License");
:: You may not use this file except in compliance with the License.
:: You may obtain a copy of the License at
:: https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.

:: Grid cluster control.

:: Check JAVA_HOME.
if not defined JAVA_HOME (
    echo ERROR: JAVA_HOME environment variable is not found.
    echo Please point JAVA_HOME variable to location of JDK 1.8 or later.
    echo You can also download the latest JDK at http://java.com/download.
    goto :error_finish
)

:: Check that JDK is where it should be.
if not exist "%JAVA_HOME%\bin\java.exe" (
    echo ERROR: JAVA is not found in JAVA_HOME=%JAVA_HOME%.
    echo Please point JAVA_HOME variable to installation of JDK 1.8 or later.
    echo You can also download the latest JDK at http://java.com/download.
    goto :error_finish
)

:: Check IGNITE_HOME.
if not defined IGNITE_HOME (
    pushd "%~dp0\.."
    set IGNITE_HOME=%CD%
    popd
)

:: Strip double quotes from IGNITE_HOME
set IGNITE_HOME=%IGNITE_HOME:"=%

:: Remove all trailing slashes from IGNITE_HOME.
:checkIgniteHome
if "%IGNITE_HOME:~-1%"=="\" set IGNITE_HOME=%IGNITE_HOME:~0,-1%
if "%IGNITE_HOME:~-1%"=="/" set IGNITE_HOME=%IGNITE_HOME:~0,-1%
if "%IGNITE_HOME:~-1%"=="\" goto checkIgniteHome
if "%IGNITE_HOME:~-1%"=="/" goto checkIgniteHome

if not exist "%IGNITE_HOME%\config" (
    echo ERROR: Ignite installation folder is not found or IGNITE_HOME environment variable is not valid.
    echo Please create IGNITE_HOME environment variable pointing to the location of
    echo Ignite installation folder.
    goto :error_finish
)

set SCRIPTS_HOME=%IGNITE_HOME%\bin

:: Remove trailing spaces
for /l %%a in (1,1,31) do if /i "%SCRIPTS_HOME:~-1%"==" " set SCRIPTS_HOME=%SCRIPTS_HOME:~0,-1%

:: Set SCRIPTS_HOME - base path to scripts.
if /i "%SCRIPTS_HOME%\"=="%~dp0" goto setProgName
    echo WARN: IGNITE_HOME environment variable may be pointing to the wrong folder: %IGNITE_HOME%

:setProgName
set PROG_NAME=ignite.bat
if "%OS%"=="Windows_NT" set PROG_NAME=%~nx0%

:: Determine Java version function
:determineJavaVersion
setlocal
for /f "tokens=2 delims=[] " %%a in ('"java -version 2>&1 | findstr /i "version""') do set ver=%%a
set ver=%ver:"=%
for /f "tokens=1-2 delims=." %%a in ("%ver%") do (
    set MAJOR=%%a
    set MINOR=%%b
)
if "%MAJOR%"=="1" set MAJOR=%MINOR%
endlocal & set JAVA_VERSION=%MAJOR%

:: Set IGNITE_LIBS
call "%SCRIPTS_HOME%\include\setenv.bat"
set CP=%IGNITE_LIBS%;%IGNITE_HOME%\libs\optional\ignite-zookeeper\*

:: JVM options.
if not defined CONTROL_JVM_OPTS (
    set CONTROL_JVM_OPTS=-Xms256m -Xmx1g
)

:: Enable assertions if set.
if "%ENABLE_ASSERTIONS%"=="1" (
    set CONTROL_JVM_OPTS=%CONTROL_JVM_OPTS% -ea
)

:: Set main class to start service (grid node by default).
if not defined MAIN_CLASS (
    set MAIN_CLASS=org.apache.ignite.internal.commandline.CommandHandler
)

:: Uncomment to enable experimental commands [--wal]
:: set CONTROL_JVM_OPTS=%CONTROL_JVM_OPTS% -DIGNITE_ENABLE_EXPERIMENTAL_COMMAND=true

:: Uncomment the following GC settings if you see spikes in your throughput due to Garbage Collection.
:: set CONTROL_JVM_OPTS=%CONTROL_JVM_OPTS% -XX:+UseG1GC

:: Uncomment if you get StackOverflowError.
:: On 64 bit systems, this value can be larger, e.g. -Xss16m
:: set CONTROL_JVM_OPTS=%CONTROL_JVM_OPTS% -Xss4m

:: Uncomment to set preference to IPv4 stack.
:: set CONTROL_JVM_OPTS=%CONTROL_JVM_OPTS% -Djava.net.preferIPv4Stack=true

:: Final CONTROL_JVM_OPTS for Java 9+ compatibility
if %JAVA_VERSION% GEQ 9 (
    set CONTROL_JVM_OPTS=%CONTROL_JVM_OPTS% --illegal-access=warn
    set CONTROL_JVM_OPTS=%CONTROL_JVM_OPTS% --add-opens=java.base/java.nio=ALL-UNNAMED
)

if defined JVM_OPTS (
    echo JVM_OPTS environment variable is set, but will not be used. To pass JVM options use CONTROL_JVM_OPTS
    echo JVM_OPTS=%JVM_OPTS%
)

:: Execute Java command based on OS
set osname=%OS%
if /i "%osname%"=="Windows_NT" (
    "%JAVA_HOME%\bin\java.exe" %CONTROL_JVM_OPTS% %QUIET% -DIGNITE_HOME="%IGNITE_HOME%" -DIGNITE_PROG_NAME="%PROG_NAME%" %JVM_XOPTS% -cp "%CP%" %MAIN_CLASS% %*
) else (
    "%JAVA_HOME%\bin\java.exe" %CONTROL_JVM_OPTS% %QUIET% -DIGNITE_HOME="%IGNITE_HOME%" -DIGNITE_PROG_NAME="%PROG_NAME%" %JVM_XOPTS% -cp "%CP%" %MAIN_CLASS% %*
)

:error_finish
if not "%NO_PAUSE%"=="1" pause
goto :eof
