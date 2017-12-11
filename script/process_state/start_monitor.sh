#!/bin/bash

if [ $# -ne 4 ]
then
    echo "parameters can't match"
    exit -1
fi


flag=$(ps aux | grep -e "start_monitor.sh.*$1.*$5$" | grep -v "grep"  | awk ' NR==3  {print $2 }') 
echo $flag
if [ $flag ]
then
	echo "the monitor process is running!"
	exit -1 
else
     echo "start monitoring"
fi

#process_name=$1
time_interval=$1
maxtimes=$2
hostfile=$3
rdir=$4

username=$(whoami)
myhostname=$(hostname)
senddir="~/logs/process_monitor_script"
recvdir="~/logs/process_monitor_script"
monitordir="~/logs/process_monitor_script"
#
pssh -i -h $hostfile "~/logs/process_monitor_script/Monitor.sh $time_interval $maxtimes $rdir" &

echo "Monitoring started"
#
while ps aux | grep "pssh.*$line.*cd $recvdir;$monitordir/Monitor.sh $time_interval $maxtimes $rdir" | grep -v "grep" > /dev/null 
do
sleep 1
done
#
#mkdir $rdir 2> /dev/null
#pslurp -r -L "$rdir" -h $hostfile "$(pwd)/$rdir/" "$rdir"
#
#while [ $? -ne 0 ]
#do	
###echo copy fail
#pslurp -r -L "$rdir" -h $hostfile "$(pwd)/$rdir" "$rdir"
#done
#pssh -h $hostfile  "rm -rf $4 2> /dev/null"
#
echo "monitoring done!"
#
