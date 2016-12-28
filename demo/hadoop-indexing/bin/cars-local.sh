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
if [  -z  "$HADOOP_HOME" ]
then
   echo ERROR: HADOOP_HOME is not set
   exit
fi
export YARN_CLIENT_OPTS="-Dhadoop.property.hadoop.security.authentication=simple"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

cd $bin/..

$HADOOP_HOME/bin/yarn jar target/hadoop-indexing-demo-0.0.2-SNAPSHOT-with-dependencies.jar \
com.xiaomi.linden.hadoop.indexing.job.LindenJob -conf conf/hadoop-indexing-local.xml
