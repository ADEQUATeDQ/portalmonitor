#!/bin/bash

BASE=`pwd`
PYTHON=$BASE/python
PYTHON_TAG=odpw_python
PW=$BASE/portalwatch
PW_TAG=odpw

PWSSERVICE=$BASE/portalwatch_services/
PWSSERVICE_META=$PWSSERVICE/metafetch
PWSSERVICE_META_TAG=odpw_meta

PWSSERVICE_HEAD=$PWSSERVICE/head
PWSSERVICE_HEAD_TAG=odpw_head

PWSSERVICE_DATA=$PWSSERVICE/datafetch
PWSSERVICE_DATA_TAG=odpw_data

PWSSERVICE_UI=$PWSSERVICE/ui
PWSSERVICE_UI_TAG=odpw_ui

DATA=$1
DATASTORE=$BASE/datastore
DATASTORE_TAG=datastore

DATADIR=$DATA/data/datadir
LOGS=$DATA/data/logs
DBDATA=$DATA/data/dbdata

DBDATA_TAG=dbdata
LOGS_TAG=logdata
DATADIR_TAG=datadir

preparePython(){
  docker rmi $PYTHON_TAG
  cd $PYTHON
  docker build --tag $PYTHON_TAG .
}

prepareDataContainers(){
  echo "prepareDataContainers"
  #build dbdata, logdata and datadir
  # data-only containers
  echo "cleanup"
  docker rm $DBDATA_TAG $LOGS_TAG $DATADIR_TAG $DATASTORE_TAG
  docker rmi $DATASTORE_TAG
  rm -rf $DBDATA/* $LOGS/* $DATADIR/*
  docker run --name $DBDATA_TAG -v $DBDATA:/var/lib/postgresql/data alpine /bin/echo Data-only
  docker run --name $LOGS_TAG -v $LOGS:/logs alpine /bin/echo Data-only
  docker run --name $DATADIR_TAG -v $DATADIR:/data alpine /bin/echo Data-only

  echo "build $DATASTORE_TAG"
  #build datastore
  # a postgres container
  cd $DATASTORE;
  docker build --tag $DATASTORE_TAG .

  #creata the datastore container
  docker run --name $DATASTORE_TAG -p 5433:5432 -d  --volumes-from dbdata -e POSTGRES_PASSWORD=4dequat3 $DATASTORE_TAG

  #wait 120 seconds until the datastore is up and running
  sleep 120
}

prepareODPW(){
  echo "prepareODPW"

  echo "cleanup"
  docker rmi $PW_TAG
  docker rm $PW_TAG
  cd $PW; docker build --tag $PW_TAG .
}


prepareODPWMeta(){
  docker stop $PWSSERVICE_META_TAG
  docker rm $PWSSERVICE_META_TAG
  docker rmi $PWSSERVICE_META_TAG

  #build PW_metafetch service
  cd $PWSSERVICE_META; docker build --tag $PWSSERVICE_META_TAG .
  docker run -d --name $PWSSERVICE_META_TAG --volumes-from logdata --link datastore:db $PWSSERVICE_META_TAG
}

prepareODPWHead(){
  docker stop $PWSSERVICE_HEAD_TAG
  docker rm $PWSSERVICE_HEAD_TAG
  docker rmi $PWSSERVICE_HEAD_TAG
  cd $PWSSERVICE_HEAD; docker build --tag $PWSSERVICE_HEAD_TAG .
  docker run -d --name $PWSSERVICE_HEAD_TAG --volumes-from logdata --link datastore:db $PWSSERVICE_HEAD_TAG
}

prepareODPWData(){
  docker stop $PWSSERVICE_DATA_TAG
  docker rm $PWSSERVICE_DATA_TAG
  docker rmi $PWSSERVICE_DATA_TAG
  cd $PWSSERVICE_DATA; docker build --tag $PWSSERVICE_DATA_TAG .
  docker run -d --name $PWSSERVICE_DATA_TAG --volumes-from logdata --volumes-from datadir --link datastore:db $PWSSERVICE_DATA_TAG
}

prepareODPWUI(){
  docker stop $PWSSERVICE_UI_TAG
  docker rm $PWSSERVICE_UI_TAG
  docker rmi $PWSSERVICE_UI_TAG
  cd $PWSSERVICE_UI; docker build --tag $PWSSERVICE_UI_TAG .
  docker run -d -p 82:80 --name $PWSSERVICE_UI_TAG --volumes-from logdata --link datastore:db $PWSSERVICE_UI_TAG
}

#preparePython
#prepareDataContainers
prepareODPW
#prepareODPWMeta
#prepareODPWHead
#prepareODPWData
prepareODPWUI


