#!/bin/sh

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

SCRIPT_NAME=encrypt.sh
EXECUTABLE_CLASS=org.jasypt.intf.cli.JasyptPBEStringEncryptionCLI
BIN_DIR=`dirname $0`
LIB_DIR=$BIN_DIR/libs
EXEC_CLASSPATH="."

if [ -n "$JASYPT_CLASSPATH" ]
then
  EXEC_CLASSPATH=$EXEC_CLASSPATH:$JASYPT_CLASSPATH
fi

for a in `find $LIB_DIR -name '*.jar'`
do
  EXEC_CLASSPATH=$EXEC_CLASSPATH:$a
done

JAVA_EXECUTABLE=java
if [ -n "$JAVA_HOME" ]
then
  JAVA_EXECUTABLE=$JAVA_HOME/bin/java
fi

if [ "$OSTYPE" = "cygwin" ]
then
  EXEC_CLASSPATH=`echo $EXEC_CLASSPATH | sed 's/:/;/g' | sed 's/\/cygdrive\/\([a-z]\)/\1:/g'`
  JAVA_EXECUTABLE=`cygpath --unix "$JAVA_EXECUTABLE"`
fi

"$JAVA_EXECUTABLE" -classpath $EXEC_CLASSPATH $EXECUTABLE_CLASS $SCRIPT_NAME "$@"
