#!/usr/bin/env bash
if [ ! -z "${IGNITE_SCRIPT_STRICT_MODE:-}" ]
then
    set -o nounset
    set -o errexit
    set -o pipefail
    set -o errtrace
    set -o functrace
fi

#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
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

#
# Ignite database connector.
#

#
# Import common functions.
#
if [ "${IGNITE_HOME:-}" = "" ];
    then IGNITE_HOME_TMP="$(dirname "$(cd "$(dirname "$0")"; "pwd")")";
    else IGNITE_HOME_TMP=${IGNITE_HOME};
fi

#
# Set SCRIPTS_HOME - base path to scripts.
#
SCRIPTS_HOME="${IGNITE_HOME_TMP}/bin"

source "${SCRIPTS_HOME}"/include/functions.sh
source "${SCRIPTS_HOME}"/include/jvmdefaults.sh

#
# Discover path to Java executable and check it's version.
#
checkJava

#
# Discover IGNITE_HOME environment variable.
#
setIgniteHome

#
# Set supported terminal type for z/OS.
#
osname=`uname`

if [ $osname = "OS/390" ] ; then
    export TERM=dumb
    JVM_OPTS="-Dfile.encoding=IBM-1047 $JVM_OPTS"
    JVM_OPTS="$(getIbmSslOpts $version) $JVM_OPTS"
fi

#
# Set IGNITE_LIBS.
#
. "${SCRIPTS_HOME}"/include/setenv.sh

JVM_OPTS=${JVM_OPTS:-}

#
# Final JVM_OPTS for Java 9+ compatibility
#
JVM_OPTS=$(getJavaSpecificOpts $version "$JVM_OPTS")

JDBCLINK="jdbc:ignite:thin://${HOST_AND_PORT:-}${SCHEMA_DELIMITER:-}${SCHEMA:-}${PARAMS:-}"

CP="${IGNITE_LIBS}"

CP="${CP}${SEP}${IGNITE_HOME_TMP}/bin/include/sqlline/*"

"$JAVA" ${JVM_OPTS} -cp ${CP} sqlline.SqlLine -d org.apache.ignite.IgniteJdbcThinDriver $@
