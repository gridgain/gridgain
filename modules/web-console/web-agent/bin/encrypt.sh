#!/usr/bin/env bash
set -o nounset
set -o errexit
set -o pipefail
set -o errtrace
set -o functrace

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

MAIN_CLASS=org.jasypt.intf.cli.JasyptPBEStringEncryptionCLI
EXEC_CLASSPATH="."

SOURCE="${BASH_SOURCE[0]}"

# Resolve $SOURCE until the file is no longer a symlink.
while [ -h "$SOURCE" ]
    do
        IGNITE_HOME="$(cd -P "$( dirname "$SOURCE"  )" && pwd)"

        SOURCE="$(readlink "$SOURCE")"

        # If $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located.
        [[ $SOURCE != /* ]] && SOURCE="$IGNITE_HOME/$SOURCE"
    done

#
# Set IGNITE_HOME.
#
export IGNITE_HOME="$(cd -P "$( dirname "$SOURCE" )" && pwd)"

source "${IGNITE_HOME}"/include/functions.sh

#
# OS specific support.
#
getClassPathSeparator;

#
# Libraries included in classpath.
#
IGNITE_LIBS="${IGNITE_HOME}/*${SEP}${IGNITE_HOME}/libs/*"

for file in ${IGNITE_HOME}/libs/*
do
    if [ -d ${file} ] && [ "${file}" != "${IGNITE_HOME}"/libs/optional ]; then
        IGNITE_LIBS=${IGNITE_LIBS:-}${SEP}${file}/*
    fi
done

CP="${IGNITE_LIBS}"
#
# Discover path to Java executable and check it's version.
#
checkJava

osname=`uname`

if [ "${DOCK_OPTS:-}" == "" ]; then
    DOCK_OPTS="-Xdock:name=Web Agent"
fi

case $osname in
    Darwin*)
        "$JAVA" "${DOCK_OPTS}" -cp "${CP}" ${MAIN_CLASS} "$@"
    ;;
    *)
        "$JAVA" -cp "${CP}" ${MAIN_CLASS} "$@"
    ;;
esac
