#!/bin/sh

#
# get_snapshot.sh
#
# Copyright(C) 2012 Uptime Technologies, LLC.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#

# ----------------------------------------
# Modify following values as you need
# ----------------------------------------
HOST=localhost
PORT=5432
USER=postgres
PGHOME=

DBNAME=$1
LEVEL=1

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
    level=$2

    psql -q -A -t ${PSQL_OPTS} -c "SELECT pgperf.create_snapshot(${level})" ${db} > /dev/null;
    if [ $? -ne 0 ]; then
	logger -s -p user.err -t pgperf "ERROR: get_snapshot failed to take a performance snapshot for database \"${d}\"."
    fi;
}

if [ -z $DBNAME ]; then
    get_database_name
fi;

for d in ${DBNAME};
  do get_statistics_snapshot ${d} ${LEVEL};
done;
