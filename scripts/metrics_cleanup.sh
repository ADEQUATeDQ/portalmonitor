#!/usr/bin/env bash

cores=8

for ((sn=1547; sn>=1544; sn--)); do
    echo $sn
    echo "odpw --host localhost --port 5432 FetchSim -sn $sn -c $cores"
    echo "odpw --host localhost --port 5432 HeadStats -sn $sn --rere"
done
