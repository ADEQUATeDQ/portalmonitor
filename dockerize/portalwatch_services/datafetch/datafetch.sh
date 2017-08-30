#!/bin/bash

#initially kill running processes
pkill  -f Fetch

DATE=`date +%Y-%m-%d`
week=`date +"%y%V"`
echo "Writting logs to $LOGS for week $week"

PORTALS=("data_gv_at" "www_opendataportal_at")

for pName in "${PORTALS[@]}"
do
    LOGF=data-fetch_"$pName"_$week
    SCRIPT="odpw -c $ADEQUATE/portalmonitor.conf DataFetch -p $pName "
    cmd="$SCRIPT 1>> $LOGS/$LOGF.out 2> $LOGS/$LOGF.err"
    echo $cmd
    eval $cmd
    gzip $LOGS/$LOGF.*

    LOGF=git-store_$week
    SCRIPT="odpw -c $ADEQUATE/portalmonitor.conf GitDataStore --pid $pName "
    cmd="$SCRIPT 1>> $LOGS/$LOGF.out 2> $LOGS/$LOGF.err"
    echo $cmd
    eval $cmd
    gzip $LOGS/$LOGF.*
done

P=$(printf %s\\n "${PORTALS[@]}"|sed 's/["\]/\\&/g;s/.*/"&"/;1s/^/[/;$s/$/]/;$!s/$/,/')

printf '{"portals":%s,"snapshot":"%s","date":"%s"}\n' "$P" "$week" "$DATE" > /logs/fetched_portals.json
