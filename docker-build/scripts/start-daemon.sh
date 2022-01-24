#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

cd "$(dirname "$0")"

cd ..

echo `pwd`

mkdir /opt/sh/logs

# memory option
if [ ! -n "$HEAP_MEN" ]; then
  HEAP_MEN="1G"
fi
if [ ! -n "$DIR_MEN" ]; then
  HEAP_MEN="1G"
fi
JVM_OPT="-Xmx${HEAP_MEN} -Xms${HEAP_MEN} -XX:MaxDirectMemorySize=${DIR_MEN}"
JVM_OPT="${JVM_OPT} -XX:+UseG1GC -XX:MaxGCPauseMillis=10 -XX:+ParallelRefProcEnabled -XX:+UnlockExperimentalVMOptions -XX:+DoEscapeAnalysis -XX:ParallelGCThreads=4 -XX:ConcGCThreads=4"
# gc log option
JVM_OPT="${JVM_OPT} -Xlog:gc*=info,gc+phases=debug:/opt/sh/logs/gc.log:time,uptime:filecount=10,filesize=100M"

java $JAVA_OPT $JVM_OPT -jar /opt/sh/pf-consumer.jar >>/opt/sh/logs/stdout.log 2>>/opt/sh/logs/stderr.log
