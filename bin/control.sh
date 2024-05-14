#!/usr/bin/env bash
if [ ! -z "${IGNITE_SCRIPT_STRICT_MODE:-}" ]; then
    set -o nounset
    set -o errexit
    set -o pipefail
    set -o errtrace
    set -o functrace
fi

# Copyright 2019 GridGain Systems, Inc. and Contributors.
# Licensed under the GridGain Community Edition License (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Grid cluster control.

# Import common functions.
if [ "${IGNITE_HOME:-}" = "" ]; then
    IGNITE_HOME_TMP="$(dirname "$(cd "$(dirname "$0")"; pwd)")"
else
    IGNITE_HOME_TMP=${IGNITE_HOME}
fi

# Set SCRIPTS_HOME - base path to scripts.
SCRIPTS_HOME="${IGNITE_HOME_TMP}/bin"
. "${SCRIPTS_HOME}/include/functions.sh"
. "${SCRIPTS_HOME}/include/jvmdefaults.sh" || exit 1

# Discover path to Java executable and check its version.
checkJava

# Determine Java version function
determineJavaVersion() {
    local version_output=$(java -version 2>&1)
    local version=$(echo "$version_output" | awk -F '"' '/version/ {print $2}' | awk -F '.' '{if ($1 == 1) {print $2} else {print $1}}')
    echo $version
}
JAVA_VERSION=$(determineJavaVersion) # Ensure this function is implemented correctly to fetch Java version

# Discover IGNITE_HOME environment variable.
setIgniteHome

if [ "${DEFAULT_CONFIG:-}" == "" ]; then
    DEFAULT_CONFIG=config/default-config.xml
fi

# Set IGNITE_LIBS.
. "${SCRIPTS_HOME}/include/setenv.sh"
CP="${IGNITE_LIBS}:${IGNITE_HOME}/libs/optional/ignite-zookeeper/*"
RANDOM_NUMBER=$("$JAVA" -cp "${CP}" org.apache.ignite.startup.cmdline.CommandLineRandomNumberGenerator)

# Mac OS specific support to display correct name in the dock.
osname=$(uname)
if [ "${DOCK_OPTS:-}" == "" ]; then
    DOCK_OPTS="-Xdock:name=Ignite Node"
fi



# Set JVM options with dynamic version check
CONTROL_JVM_OPTS=$(getJavaSpecificOpts $JAVA_VERSION "$CONTROL_JVM_OPTS")
CONTROL_JVM_OPTS+=" --illegal-access=warn"
CONTROL_JVM_OPTS+=" --add-opens=java.base/java.nio=ALL-UNNAMED"
CONTROL_JVM_OPTS+=" --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

# Enable assertions if set.
if [ "${ENABLE_ASSERTIONS:-}" = "1" ]; then
    # CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} -ea"
    CONTROL_JVM_OPTS+=" -ea"
fi

# Set main class to start service (grid node by default).
if [ "${MAIN_CLASS:-}" = "" ]; then
    MAIN_CLASS=org.apache.ignite.internal.commandline.CommandHandler
fi

# Execute Java command based on OS
case $osname in
    Darwin*)
        "$JAVA" ${CONTROL_JVM_OPTS} ${QUIET:-} "${DOCK_OPTS}" -DIGNITE_HOME="${IGNITE_HOME}" \
         -DIGNITE_PROG_NAME="$0" ${JVM_XOPTS:-} -cp "${CP}" ${MAIN_CLASS} "$@"
        ;;
    OS/390*)
        "$JAVA" ${CONTROL_JVM_OPTS} ${QUIET:-} -DIGNITE_UPDATE_NOTIFIER=false -DIGNITE_HOME="${IGNITE_HOME}" \
         $(getIbmSslOpts $JAVA_VERSION) -DIGNITE_PROG_NAME="$0" ${JVM_XOPTS:-} -cp "${CP}" ${MAIN_CLASS} "$@"
        ;;
    *)
        "$JAVA" ${CONTROL_JVM_OPTS} ${QUIET:-} -DIGNITE_HOME="${IGNITE_HOME}" \
         -DIGNITE_PROG_NAME="$0" ${JVM_XOPTS:-} -cp "${CP}" ${MAIN_CLASS} "$@"
        ;;
esac
