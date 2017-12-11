#!/bin/bash
if [ $# -ne 3 ];
then
    echo "the number of parameters can't match"
    exit -1
fi

if [ $? -ne 0 ]
then
    echo "please start $1 first!"
    exit -1
fi 
#ps aux | grep -e "Monitor\.sh.*$1.*$4" | grep -v "grep" | grep -v "$$" | awk '{print $2}' | while read line ; do kill -2 $line > /dev/null; done

senddir='~/logs/process_monitor_script'
#recvdir=$3
inv=$1
maxtime=$2

rm -rf $3

if [ ! -d $3 ]
then
   mkdir $3
fi
cd $3

logfile="program.log"

if [ -e $logfile ]
then
    rm $logfile
fi

flag=true;

if [ $maxtime -eq 0 ]
then
pidstat -r -C "java|mgiza" $inv >> out_mem &
pidstat -u -C "java|mgiza" $inv >> out_cpu &
pidstat -d -C "java|mgiza" $inv >> out_disk &

sar -r $inv >> sar_mem &
sar -u $inv >> sar_cpu &
sar -d $inv >> sar_disk &
sar -n DEV $inv >> sar_net &

elif [ $maxtime -gt 0 ]
then

pidstat -r -C "java|mgiza" $inv $maxtime >> out_mem &
pidstat -u -C "java|mgiza" $inv $maxtime >> out_cpu &
pidstat -d -C "java|mgiza" $inv $maxtime >> out_disk &

sar -r $inv $maxtime >> sar_mem &
sar -u $inv $maxtime >> sar_cpu &
sar -d $inv $maxtime >> sar_disk &
sar -n DEV $inv $maxtime >> sar_net &

fi
#ps aux | grep -e "pidstat" | grep -v "grep" | grep -v "$$" | awk '{print $2}' | while read line ; do kill -2 $line > /dev/null; done
#ps aux | grep -e "sar" | grep -v "grep" | grep -v "$$" | awk '{print $2}' | while read line ; do kill -2 $line > /dev/null; done
wait
#echo "Memory Info" > mem.md
#cat out_mem | awk 'NR>1' | awk '{if (!NF) {next}}1' | grep "Average" >> mem.md 
#echo "CPU Info " > cpu.md
#cat out_cpu | awk 'NR>1' | awk '{if (!NF) {next}}1' | grep "Average" >> cpu.md
#echo "Disk Info " > disk.md
#cat out_disk | awk 'NR>1' | awk '{if (!NF) {next}}1' | grep "Average" >> disk.md
#
#echo "Memory Info" >> mem.md
#cat sar_mem | awk 'NR>1' | awk '{if (!NF) {next}}1' | grep "Average" >> mem.md
#echo "CPU Info " >> cpu.md
#cat sar_cpu | awk 'NR>1' | awk '{if (!NF) {next}}1' | grep "Average" >> cpu.md
#echo "Disk Info " >> disk.md
#cat sar_disk | awk 'NR>1' | awk '{if (!NF) {next}}1' | grep "Average" >> disk.md
#echo "NetWork Info " > net.md
#cat sar_net | awk 'NR>1' | awk '{if (!NF) {next}}1' | grep "Average" >> net.md
#
##paste -d ',' cpu.md mem.md disk.md net.md > program.log
#rm -rf out_* 2> /dev/null
#rm -rf sar_* 2> /dev/null
if [ $? -ne 0 ]
then
	echo $(hostname)
fi

