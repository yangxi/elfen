#!/bin/bash
ITERs=20
#for q in $QPSs; do
#BASETAG=${1}
#we have 20 invoks numbers for all benchmarks except pmd and xalan, so we run them seperiaey here
#for bench in self antlr bloat chart eclipse fop hsqldb jython luindex lusearch pmd xalan; do
#for bench in xalan; do
for bench in latency; do
#for bench in chart eclipse fop hsqldb jython luindex lusearch pmd xalan; do
    echo "start corunner $bench"
#    ssh -ft xeond "taskset 0x80 /home/yangxi/shimbatchDacapo/runexpcmp.sh ${bench}"
#    sleep 3
    BASETAG="${bench}cmp"
    for ((i=1; i<=20; i=i+1)); do
	for ((q=900;q<=900;q=q+50)); do
            echo "Invoking at QPS $q ITERATIONS $ITERs";
            TAG="${BASETAG}Invok${i}"
            ssh xeond "taskset 0x80 taskset 0x80 cat /proc/stat" > ./stat_${TAG}_${q}_${ITERs}_begin
            ssh xeond "taskset 0x80 cat /proc/interrupts" > ./interrupts_${TAG}_${q}_${ITERs}_begin
            sleep 3
            python ../script/sendTasks.py ../script/20K.term.tasks 192.168.1.185 7777 $q 1000000 200000 ${TAG}_${q}_${ITERs} ${ITERs} order > ./log_${TAG}_${q}_${ITERs}
            sleep 3
            ssh xeond "taskset 0x80 cat /proc/stat" > ./stat_${TAG}_${q}_${ITERs}_end
            ssh xeond "taskset 0x80 cat /proc/interrupts" > ./interrupts_${TAG}_${q}_${ITERs}_end
        done
    done
done
