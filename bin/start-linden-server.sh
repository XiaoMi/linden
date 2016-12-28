# Copyright 2016 Xiaomi, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

usage="Usage: start-linden-server config-dir"
if [ $# -ne 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

OS=`uname`

lib=$bin/../linden-core/target/lib
dist=$bin/../linden-core/target

HEAP_OPTS="-Xmx1g -Xms1g -XX:NewSize=256m"
JAVA_OPTS="-server -d64"

MAIN_CLASS="com.xiaomi.linden.service.LindenServer"

CLASSPATH=$CLASSPATH:$lib/*:$dist/*

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

exec $JAVA $JAVA_OPTS $HEAP_OPTS $GC_OPTS $JAVA_DEBUG -classpath $CLASSPATH $MAIN_CLASS $1
