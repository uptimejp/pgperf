#!/bin/sh

#
# get_snapshot.sh
#
# Copyright(C) 2012 Uptime Technologies, LLC. All rights reserved.
#

# ----------------------------------------
# Modify following values as you need
# ----------------------------------------
HOST=localhost
PORT=5432
USER=postgres
PGHOME=

DBNAME=$1

if [ -n ${PGHOME} ]; then
    PATH=${PGHOME}/bin:${PATH}
    export PATH
fi;

PSQL_OPTS="-h ${HOST} -p ${PORT} -U ${USER}"

function get_database_name ()
{
    q="SELECT datname FROM pg_database WHERE datistemplate = false AND datallowconn = true ORDER BY oid";

    DBNAME=`psql -q -A -t ${PSQL_OPTS} -c "$q" postgres | xargs`
}

function get_statistics_snapshot()
{
    db=$1

    psql -q -A -t ${PSQL_OPTS} -c 'SELECT pgperf.create_snapshot()' ${db} > /dev/null;
    if [ $? -ne 0 ]; then
	logger -s -p user.err -t pgperf "ERROR: get_snapshot failed to take a performance snapshot for database \"${d}\"."
    fi;
}

if [ -z $DBNAME ]; then
    get_database_name
fi;

for d in ${DBNAME};
  do get_statistics_snapshot ${d};
done;
