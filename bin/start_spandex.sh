#!/bin/bash
# Start spandex http server
BASEDIR=$(dirname $0)/..
CONFIG=$BASEDIR/configs/application.conf
JARFILE=$BASEDIR/spandex-http/target/scala-2.10/spandex-http-assembly-*.jar
if [ ! -e $JARFILE ]; then
  pushd $BASEDIR && sbt assembly && popd
fi
java -Dconfig=$CONFIG -jar $JARFILE
