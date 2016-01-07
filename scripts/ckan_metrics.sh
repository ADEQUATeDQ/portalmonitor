#!/usr/bin/env bash

cores=4

. /home/opwu/ODPortalWatch_2/odpw_env/bin/activate
SCRIPT="/home/opwu/ODPortalWatch_2/odpw_env/bin/odpw --host localhost --port 5432"
LOG=/home/opwu/ODPortalWatch_2/logs
mkdir -p $LOG


for ((sn=1553; sn>=1534; sn--))
do
    echo $sn

    cmd="$SCRIPT  CKANFetch -sn $sn -c $cores -o quality.$sn.csv --pidfile $LOG/ckan_metrics-pid.out 1> $LOG/ckan_metrics-$sn.out 2> $LOG/ckan_metrics-$sn.err"
    echo $cmd
    eval $cmd

done
