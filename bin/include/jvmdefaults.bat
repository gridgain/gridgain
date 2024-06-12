@echo off
::
:: Copyright 2022 GridGain Systems, Inc. and Contributors.
::
:: Licensed under the GridGain Community Edition License (the "License");
:: You may not use this file except in compliance with the License.
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

:: Gets java specific options like add-exports and add-opens
:: First argument is the version of the java
:: Second argument is the current value of the jvm options
:: Third value is the name of the environment variable that jvm options should be set to

setlocal
set java_version=%1
set current_value=%2
set value=

if %java_version% LSS 9 (
    :: Java 8 does not support --add-exports or --add-opens
    set value=%current_value%
) else if %java_version% LSS 11 (
    :: Java 9 and 10 require additional modules due to removed Java EE modules
    set value=--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED --illegal-access=permit --add-modules=java.xml.bind %current_value%
) else (
    :: From Java 11 onwards, reduce the use of aggressive exports and opens, focusing on necessary access only
    set value=--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED %current_value%
)

endlocal & set "%~3=%value%"

:: Export the function
@echo off
set java_version=%1
set current_value=%2
set value=

:: Gets java specific options like add-exports and add-opens
:: First argument is the version of the java
:: Second argument is the current value of the jvm options
:: Third value is the name of the environment variable that jvm options should be set to

if %java_version% LSS 9 (
    :: Java 8 does
