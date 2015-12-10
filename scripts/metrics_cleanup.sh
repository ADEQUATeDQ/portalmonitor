#!/usr/bin/env bash

cores=8

. /home/opwu/ODPortalWatch_2/odpw_env/bin/activate
SCRIPT="/home/opwu/ODPortalWatch_2/odpw_env/bin/odpw --host localhost --port 5432"
LOG=/home/opwu/ODPortalWatch_2/logs
mkdir -p $LOG


for ((sn=1547; sn>=1544; sn--)); do
    echo $sn
    
    cmd="$SCRIPT  FetchSim -sn $sn -c $cores 1> $LOG/metrics_cleanup-$sn.out 2> $LOG/metrics_cleanup-$sn.err"
    echo $cmd
    eval $cmd
    
    cmd="$SCRIPT  HeadStats -sn $sn --rere 1> $LOG/headstats_cleanup-$sn.out 2> $LOG/headstats_cleanup-$sn.err"
    echo $cmd
    eval $cmd
done
