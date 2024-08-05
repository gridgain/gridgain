#!/bin/sh
#
# Copyright 2022 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

getIbmSslOpts() {
  version=$1
  OS390_SSL_ALGO="IbmX509"

  if [ "${version}" -ge 11 ]; then
    OS390_SSL_ALGO="SunX509"
  fi

  echo "-Dcom.ibm.jsse2.overrideDefaultTLS=true -Dssl.KeyManagerFactory.algorithm=${OS390_SSL_ALGO}"
}

# Gets java specific options like add-exports and add-opens
# First argument is the version of the java
# Second argument is the current value of the jvm options
getJavaSpecificOpts() {
  version=$1
  current_value=$2
  value=""

  if [ "$version" -eq 8 ]; then
      # Keep options minimal and avoid deprecated ones for Java 8
      value="-XX:+AggressiveOpts $current_value"

  elif [ "$version" -ge 9 ] && [ "$version" -lt 11 ]; then
      # Java 9 and 10 require additional modules due to removed Java EE modules
      value="\
          -XX:+AggressiveOpts \
          --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED \
          --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
          --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED \
          --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED \
          --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED \
          --illegal-access=permit \
          --add-modules=java.xml.bind \
          $current_value"

  elif [ "$version" -ge 11 ]; then
      # From Java 11 onwards, reduce the use of aggressive exports and opens, focusing on necessary access only
      value="\
          --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED \
          --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
          --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED \
          --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED \
          --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED \
          --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
          --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
          --add-opens=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED \
          --add-opens=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED \
          --add-opens=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED \
          --add-opens=java.base/java.io=ALL-UNNAMED \
          --add-opens=java.base/java.net=ALL-UNNAMED \
          --add-opens=java.base/java.nio=ALL-UNNAMED \
          --add-opens=java.base/java.security.cert=ALL-UNNAMED \
          --add-opens=java.base/java.util=ALL-UNNAMED \
          --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
          --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED \
          --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
          --add-opens=java.base/java.lang=ALL-UNNAMED \
          --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
          --add-opens=java.base/java.math=ALL-UNNAMED \
          --add-opens=java.base/java.time=ALL-UNNAMED \
          --add-opens=java.base/sun.security.ssl=ALL-UNNAMED \
          --add-opens=java.base/sun.security.x509=ALL-UNNAMED \
          --add-opens=java.sql/java.sql=ALL-UNNAMED \
          $current_value"
  fi

  echo $value
}
