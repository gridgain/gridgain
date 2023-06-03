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
# Grid command line loader.
#

GRIDGAIN_USER=$(id -u -n)
GRIDGAIN_GROUP=$(id -g -n)

#
# Check if it is Linux.
#

OSNAME=$(uname)
if [[ "$OSNAME" != "Linux" ]] ; then
    echo "This script works only on Linux"
    exit 1
fi

#
# Check if systemd is present.
#

SYSTEMCTL=$(which systemctl)
if [[ "$SYSTEMCTL" == "" ]] ; then
    echo "systemd is not present, this script is only applicable in a systemd environment"
    exit 1
fi

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
GRIDGAIN_RELEASE=${IGNITE_HOME##*/}

if [ "${DEFAULT_CONFIG:-}" == "" ]; then
    DEFAULT_CONFIG=config/default-config.xml
fi

#
# Parse command line parameters.
#
. "${SCRIPTS_HOME}"/include/parseargs.sh

#
# Set IGNITE_LIBS.
#
. "${SCRIPTS_HOME}"/include/setenv.sh

CP="${IGNITE_LIBS}"

#
# JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
#
# ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
#
if [ -z "$JVM_OPTS" ] ; then
    JVM_OPTS="-Xms1g -Xmx1g -server -XX:MaxMetaspaceSize=256m"
else
    JVM_OPTS="${JVM_OPTS} -Xms1g -Xmx1g -server -XX:MaxMetaspaceSize=256m"
fi

#
# Uncomment if you get StackOverflowError.
# On 64 bit systems this value can be larger, e.g. -Xss16m
#
# JVM_OPTS="${JVM_OPTS} -Xss4m"

#
# Assertions are disabled by default since version 3.5.
# If you want to enable them - set 'ENABLE_ASSERTIONS' flag to '1'.
#
ENABLE_ASSERTIONS="0"

#
# Set '-ea' options if assertions are enabled.
#
if [ "${ENABLE_ASSERTIONS}" = "1" ]; then
    JVM_OPTS="${JVM_OPTS} -ea"
fi

#
# Set main class to start service (grid node by default).
#
if [ "${MAIN_CLASS:-}" = "" ]; then
    MAIN_CLASS=org.apache.ignite.startup.cmdline.CommandLineStartup
fi

#
# Remote debugging (JPDA).
# Uncomment and change if remote debugging is required.
#
# JVM_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8787 ${JVM_OPTS}"

#
# Uncomment if you want to see all the flags.
#
#JVM_OPTS="${JVM_OPTS} -XX:+PrintFlagsFinal"

#
# Final JVM_OPTS for Java 9+ compatibility.
#
JVM_OPTS="${JVM_OPTS} $(getJavaSpecificOpts $version)"

#
# GC Options.
#
GC_OPTS="-Xlog:gc:/var/log/${GRIDGAIN_RELEASE}/gc.log"
GC_ENGINE="-XX:+UseG1GC"
! [[ -e ${IGNITE_HOME}/logs ]] && mkdir -p ${IGNITE_HOME}/logs

#
# Uncomment if you want to run gridgain in IPv4 only environment.
#
IP_OPTS="-Djava.net.preferIPv4Stack=true"

#
# JMX options
#
JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.rmi.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"

#
# Building service environment file
#

echo "JAVA=${JAVA}"                              > ${IGNITE_HOME}/config/service.properties
echo "JVM_OPTS=${JVM_OPTS}"                     >> ${IGNITE_HOME}/config/service.properties
echo "JMX_OPTS=${JMX_OPTS}"                     >> ${IGNITE_HOME}/config/service.properties
echo "QUIET=${QUIET}"                           >> ${IGNITE_HOME}/config/service.properties
echo "IGNITE_HOME=-DIGNITE_HOME=${IGNITE_HOME}" >> ${IGNITE_HOME}/config/service.properties
echo "GC_OPTS=${GC_OPTS}"                       >> ${IGNITE_HOME}/config/service.properties
echo "GC_ENGINE=${GC_ENGINE}"                   >> ${IGNITE_HOME}/config/service.properties
echo "IP_OPTS=${IP_OPTS}"                       >> ${IGNITE_HOME}/config/service.properties
echo "CP=-cp ${CP}"                             >> ${IGNITE_HOME}/config/service.properties
echo "MAIN_CLASS=${MAIN_CLASS}"                 >> ${IGNITE_HOME}/config/service.properties
echo "CONFIG=${IGNITE_HOME}/${CONFIG}"          >> ${IGNITE_HOME}/config/service.properties

#
# Building service file
#

SERVICE_PATH=/etc/systemd/system/${GRIDGAIN_RELEASE}.service
sudo cp "${IGNITE_HOME}/config/template.service"            ${SERVICE_PATH}
sudo sed -i "s|{{ GridGainHome }}|${IGNITE_HOME}|g"         ${SERVICE_PATH}
sudo sed -i "s|{{ JavaHome }}|${JAVA}|g"                    ${SERVICE_PATH}
sudo sed -i "s|{{ GridGainRelease }}|${GRIDGAIN_RELEASE}|g" ${SERVICE_PATH}
sudo sed -i "s|{{ GridGainUser }}|${GRIDGAIN_USER}|g"       ${SERVICE_PATH}
sudo sed -i "s|{{ GridGainGroup }}|${GRIDGAIN_GROUP}|g"     ${SERVICE_PATH}
sudo mkdir -p "/var/log/${GRIDGAIN_RELEASE}" && sudo chown ${GRIDGAIN_USER}.${GRIDGAIN_GROUP} "/var/log/${GRIDGAIN_RELEASE}"
sudo systemctl daemon-reload

#
