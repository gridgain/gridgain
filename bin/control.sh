#!/usr/bin/env bash
if [ ! -z "${IGNITE_SCRIPT_STRICT_MODE:-}" ]; then
    set -o nounset
    set -o errexit
    set -o pipefail
    set -o errtrace
    set -o functrace
fi

# Copyright and license information...

# Grid cluster control.

# Import common functions.
if [ "${IGNITE_HOME:-}" = "" ]; then
    IGNITE_HOME_TMP="$(dirname "$(cd "$(dirname "$0")"; pwd)")"
else
    IGNITE_HOME_TMP=${IGNITE_HOME}
fi

# Set SCRIPTS_HOME - base path to scripts.
SCRIPTS_HOME="${IGNITE_HOME_TMP}/bin"

source "${SCRIPTS_HOME}/include/functions.sh"
source "${SCRIPTS_HOME}/include/jvmdefaults.sh"

# Discover path to Java executable and check its version.
checkJava

# Determine Java version function
determineJavaVersion() {
    local version_output=$(java -version 2>&1)
    local version=$(echo "$version_output" | awk -F '"' '/version/ {print $2}' | awk -F '.' '{if ($1 == 1) {print $2} else {print $1}}')
    echo $version
}

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

# JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
# ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
# Set JVM options with dynamic version check
JAVA_VERSION=$(determineJavaVersion)
CONTROL_JVM_OPTS=$(getJavaSpecificOpts $JAVA_VERSION "$CONTROL_JVM_OPTS")

# Enable assertions if set.
if [ "${ENABLE_ASSERTIONS:-}" = "1" ]; then
    CONTROL_JVM_OPTS+=" -ea"
fi

# Set main class to start service (grid node by default).
if [ "${MAIN_CLASS:-}" = "" ]; then
    MAIN_CLASS=org.apache.ignite.internal.commandline.CommandHandler
fi

# Uncomment to enable experimental commands [--wal]
# CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} -DIGNITE_ENABLE_EXPERIMENTAL_COMMAND=true"

# Uncomment the following GC settings if you see spikes in your throughput due to Garbage Collection.
# CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} -XX:+UseG1GC"

# Uncomment if you get StackOverflowError.
# On 64 bit systems this value can be larger, e.g. -Xss16m
# CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} -Xss4m"

# Uncomment to set preference for IPv4 stack.
# CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} -Djava.net.preferIPv4Stack=true"

if [ -n "${JVM_OPTS}" ]; then
    echo "JVM_OPTS environment variable is set, but will not be used. To pass JVM options use CONTROL_JVM_OPTS"
    echo "JVM_OPTS=${JVM_OPTS}"
fi

case $osname in
    Darwin*)
        "$JAVA" ${CONTROL_JVM_OPTS} ${QUIET:-} "${DOCK_OPTS}" -DIGNITE_UPDATE_NOTIFIER=false -DIGNITE_HOME="${IGNITE_HOME}" \
        -DIGNITE_PROG_NAME="$0" ${JVM_XOPTS:-} -cp "${CP}" ${MAIN_CLASS} "$@"
    ;;
    OS/390*)
        "$JAVA" ${CONTROL_JVM_OPTS} ${QUIET:-} -DIGNITE_UPDATE_NOTIFIER=false -DIGNITE_HOME="${IGNITE_HOME}" \
        $(getIbmSslOpts $JAVA_VERSION) -DIGNITE_PROG_NAME="$0" ${JVM_XOPTS:-} -cp "${CP}" ${MAIN_CLASS} "$@"
    ;;
    *)
        "$JAVA" ${CONTROL_JVM_OPTS} ${QUIET:-} -DIGNITE_UPDATE_NOTIFIER=false -DIGNITE_HOME="${IGNITE_HOME}" \
        -DIGNITE_PROG_NAME="$0" ${JVM_XOPTS:-} -cp "${CP}" ${MAIN_CLASS} "$@"
    ;;
esac
