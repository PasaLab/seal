#!/bin/bash

if [ $# -ne 1 ]
then
    echo "parameters can't match"
fi

recvdir='~/logs/process_monitor_script'
cat $1 | while read line
do
	echo $line
	ssh $line mkdir -p $recvdir </dev/null
   	scp Monitor.sh $line:$recvdir/Monitor.sh
        scp kill.sh $line:$recvdir/kill.sh
done  
