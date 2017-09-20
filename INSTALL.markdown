# prerequirements
- python2.7 
- virtualenv
- pip
- postgresql

# database setup
- create a user for your database (eg. opendataportalwatch)

`sudo -u postgres psql opendataportalwatch`

- create a database (eg. opendataportalwatchdb)

`sudo -u postgres createdb opendataportalwatchdb`

- give to the user a password (eg. letmein)

`sudo -u postgres psql`

`psql=# alter user opendataportalwatch with encrypted password 'letmein';`

- grant privileges on database

`psql=# grant all privileges on database opendataportalwatchdb to opendataportalwatch ;`

- exit from pgsql

`psql=# \q`

# installation
- clone the repository

`git clone https://github.com/ADEQUATeDQ/portalmonitor.git`

`cd portalmonitor`

- create a virtual enviroment

`virtualenv opendataportalwatch`

- activate it 

`. opendataportalwatch/bin/activate`

- install dependecies

`pip install -r requirements`

- install opendataportalwatch

`python setup.py install`

# configuration
- create a directory to archive the data fetched from the portal(s)

eg

`mkdir data`

- create a configuration file like this (you can choose the name eg. [portalmonitor.conf](https://raw.githubusercontent.com/ADEQUATeDQ/portalmonitor/master/dockerize/portalwatch/portalmonitor.conf) )

If your user is without root privileges, you need to choose ports from 49152 to 65535
```
db:
  host: localhost
  port: 5432
  user: opendataportalwatch
  password: letmein
  db: opendataportalwatchdb

data:
  datadir: data

rest:
    url_prefix: /api
    port: 8888

ui:
    url_prefix: /portalmonitor
    port: 8888
```
- choose a name and a path for the configuration file 
Eg. `etc/portalmonitor.conf`

- intialize database

``bin/odpw -c etc/portalmonitor.conf InitDB``

- add one or more portal to watch

eg. data.gov.at

``bin/odpw -c etc/portalmonitor.conf AddPortal -u http://data.gv.at/ -a http://www.data.gv.at/katalog/ -s CKAN -i AT``

eg. dati.trentino.it

``bin/odpw -c etc/portalmonitor.conf AddPortal -u http://dati.trentino.it/ -a http://dati.trentino.it/ -s CKAN -i IT``

- fetch metadata from the catalog(s)

``bin/odpw -c etc/portalmonitor.conf Fetch``

- fetch data from the catalog(s)

``bin/odpw -c etc/portalmonitor.conf DataFetch``

- run the web interface

``bin/odpw -c etc/portalmonitor.conf ODPWUI``

... prepare a cron to run all the commands to update the statistics

