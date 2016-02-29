#!/bin/bash

exec_name=`basename "$0"`
exec_name=`cd "$exec_name"; pwd`
#DHORSE HOME
DHORSE_HOME=${exec_name}
export DHORSE_HOME

lib_dir=$exec_name/lib
DPUMPCLASSPATH=$DPUMPCLASSPATH:"$exec_name"/dhorse-1.0-SNAPSHOT.jar

for i in "$lib_dir"/*.jar
do
	 CLASSPATH="$CLASSPATH":"$i"
done

export CLASSPATH=.:$CLASSPATH:$DPUMPCLASSPATH

java cn.wanda.dataserv.engine.EngineCli $@
