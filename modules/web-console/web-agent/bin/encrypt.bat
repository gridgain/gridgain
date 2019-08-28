::
:: Copyright 2019 GridGain Systems, Inc. and Contributors.
::
:: Licensed under the GridGain Community Edition License (the "License");
:: you may not use this file except in compliance with the License.
:: You may obtain a copy of the License at
::
::     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.
::

@ECHO OFF
Setlocal EnableDelayedExpansion

if "%OS%" == "Windows_NT"  setlocal

:: Check IGNITE_HOME.
pushd "%~dp0"
set IGNITE_HOME=%CD%

:checkIgniteHome2
:: Strip double quotes from IGNITE_HOME
set IGNITE_HOME=%IGNITE_HOME:"=%

:: remove all trailing slashes from IGNITE_HOME.
if %IGNITE_HOME:~-1,1% == \ goto removeTrailingSlash
if %IGNITE_HOME:~-1,1% == / goto removeTrailingSlash
goto checkIgniteHome3

:removeTrailingSlash
set IGNITE_HOME=%IGNITE_HOME:~0,-1%
goto checkIgniteHome2

:checkIgniteHome3

:: Check JAVA_HOME.
if defined JAVA_HOME  goto checkJdk
    echo %0, ERROR:
    echo JAVA_HOME environment variable is not found.
    echo Please point JAVA_HOME variable to location of JDK 1.8 or later.
    echo You can also download latest JDK at http://java.com/download.
goto error_finish

:checkJdk
:: Check that JDK is where it should be.
if not exist "%JAVA_HOME%\bin\java.exe" (
    echo %0, ERROR:
    echo JAVA is not found in JAVA_HOME=%JAVA_HOME%.
    echo Please point JAVA_HOME variable to installation of JDK 1.8 or later.
    echo You can also download latest JDK at http://java.com/download.
)
goto :run_java

:run_java

set CP=%IGNITE_HOME%\*;%IGNITE_HOME%\libs\*

"%JAVA_HOME%\bin\java.exe" %JVM_OPTS% -cp "%CP%" org.jasypt.intf.cli.JasyptPBEStringEncryptionCLI %*

goto :eof
