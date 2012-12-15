begin;
--
-- pgperf snapshot package
--
-- Copyright(C) 2012 Uptime Technologies, LLC.
--
-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; version 2 of the License.
-- 
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
-- 
-- You should have received a copy of the GNU General Public License along
-- with this program; if not, write to the Free Software Foundation, Inc.,
-- 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
--
CREATE SCHEMA pgperf;

--
-- pgperf stat tables
--
CREATE TABLE pgperf.snapshot (
  sid INTEGER PRIMARY KEY,
  ts TIMESTAMP NOT NULL
);

CREATE INDEX snapshot_ts_idx on pgperf.snapshot(ts);

--
-- Get a major version of the PostgreSQL server.
--
CREATE OR REPLACE FUNCTION pgperf._get_server_version (
) RETURNS INTEGER
AS '
DECLARE
  _version INTEGER;
BEGIN
  SELECT substr(replace(setting, ''.'', ''''), 1, 2)::integer INTO _version
    FROM pg_settings
   WHERE name = ''server_version'';

  IF _version < 83 THEN
    RAISE EXCEPTION ''Unsupported PostgreSQL version: %'', _version;
  END IF;

  RETURN _version;
END
' LANGUAGE 'plpgsql';

--
-- Check if a function exists.
--
CREATE OR REPLACE FUNCTION pgperf._check_function (
  NAME
) RETURNS BOOLEAN
AS '
DECLARE
  _name ALIAS FOR $1;
  _found BOOLEAN;
BEGIN
  SELECT CASE WHEN count(*)>0 THEN true ELSE false END INTO _found
    FROM pg_proc
   WHERE proname = _name;

  RETURN _found;
END
' LANGUAGE 'plpgsql';

--
-- Check if a table or a view exists.
--
CREATE OR REPLACE FUNCTION pgperf._check_table_or_view (
  NAME
) RETURNS BOOLEAN
AS '
DECLARE
  _name ALIAS FOR $1;
  _found BOOLEAN;
BEGIN
  SELECT CASE WHEN count(*)>0 THEN true ELSE false END INTO _found
    FROM pg_class
   WHERE relname = _name
     AND (relkind = ''r'' OR relkind = ''v'');

  RETURN _found;
END
' LANGUAGE 'plpgsql';

--
-- Create a snapshot.
--
CREATE OR REPLACE FUNCTION pgperf.create_snapshot (
  INTEGER
) RETURNS integer
AS '
DECLARE
  _level ALIAS FOR $1;
  _sid INTEGER;
  _version INTEGER;
  _found BOOLEAN;
BEGIN
  SELECT pgperf._get_server_version() INTO _version;

  SELECT max(sid) INTO _sid FROM pgperf.snapshot;
  IF _sid IS NULL THEN
    _sid := 0;
  ELSE
    _sid = _sid + 1;
  END IF;

  INSERT INTO pgperf.snapshot (sid,ts) VALUES (_sid, now());

  PERFORM pgperf.create_snapshot_pg_relation_size(_sid);
  PERFORM pgperf.create_snapshot_pg_stat_bgwriter(_sid);
  PERFORM pgperf.create_snapshot_pg_stat_database(_sid);
  PERFORM pgperf.create_snapshot_pg_stat_user_tables(_sid);
  PERFORM pgperf.create_snapshot_pg_statio_user_tables(_sid);
  PERFORM pgperf.create_snapshot_pg_stat_user_indexes(_sid);
  PERFORM pgperf.create_snapshot_pg_statio_user_indexes(_sid);
  PERFORM pgperf.create_snapshot_pg_current_xlog(_sid);
  PERFORM pgperf.create_snapshot_pg_stat_activity(_sid);
  PERFORM pgperf.create_snapshot_pg_locks(_sid);

  SELECT pgperf._check_table_or_view(''pg_stat_statements'') INTO _found;
  IF _version > 83 AND _found THEN
    PERFORM pgperf.create_snapshot_pg_stat_statements(_sid);
  END IF;

  IF _level >= 2 THEN
    PERFORM pgperf.create_snapshot_pg_statistic(_sid);
  END IF;

  SELECT pgperf._check_function(''pgstattuple'') INTO _found;
  IF _level >= 4 AND _found THEN
    PERFORM pgperf.create_snapshot_pgstattuple(_sid);
    PERFORM pgperf.create_snapshot_pgstatindex(_sid);
  END IF;

  RETURN _sid;
END
' LANGUAGE 'plpgsql';

--
-- Delete a snapshot.
--
CREATE OR REPLACE FUNCTION pgperf.delete_snapshot (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;
  _version INTEGER;
BEGIN
  SELECT pgperf._get_server_version() INTO _version;

  PERFORM pgperf.delete_snapshot_pgstatindex(_sid);
  PERFORM pgperf.delete_snapshot_pgstattuple(_sid);
  PERFORM pgperf.delete_snapshot_pg_stat_statements(_sid);

  PERFORM pgperf.delete_snapshot_pg_locks(_sid);
  PERFORM pgperf.delete_snapshot_pg_stat_activity(_sid);
  PERFORM pgperf.delete_snapshot_pg_current_xlog(_sid);
  PERFORM pgperf.delete_snapshot_pg_statistic(_sid);
  PERFORM pgperf.delete_snapshot_pg_statio_user_indexes(_sid);
  PERFORM pgperf.delete_snapshot_pg_stat_user_indexes(_sid);
  PERFORM pgperf.delete_snapshot_pg_statio_user_tables(_sid);
  PERFORM pgperf.delete_snapshot_pg_stat_user_tables(_sid);
  PERFORM pgperf.delete_snapshot_pg_stat_database(_sid);
  PERFORM pgperf.delete_snapshot_pg_stat_bgwriter(_sid);
  PERFORM pgperf.delete_snapshot_pg_relation_size(_sid);

  DELETE FROM pgperf.snapshot WHERE sid = _sid;

  RETURN true;
END
' LANGUAGE 'plpgsql';


--
-- Purge old snapshots at once.
--
CREATE OR REPLACE FUNCTION pgperf.purge_snapshots (
  INTERVAL
) RETURNS INTEGER
AS '
DECLARE
  _interval ALIAS FOR $1;
  _count INTEGER;
BEGIN
  SELECT count(*) INTO _count
    FROM pgperf.snapshot
   WHERE ts < now() - _interval::interval;

  PERFORM pgperf.delete_snapshot(sid)
     FROM pgperf.snapshot
    WHERE ts < now() - _interval::interval;

  RETURN _count;
END
' LANGUAGE 'plpgsql';


--
-- Get an interval between snaphsots in seconds.
--
CREATE OR REPLACE FUNCTION pgperf.get_interval (
  INTEGER,
  INTEGER
) RETURNS INTEGER
AS '
DECLARE
  _sid1 ALIAS FOR $1;
  _sid2 ALIAS FOR $2;
  _interval INTEGER;
BEGIN
  SELECT extract(EPOCH FROM (s2.ts - s1.ts))::INTEGER INTO _interval
    FROM (SELECT ts FROM pgperf.snapshot WHERE sid=_sid1 ) AS s1,
         (SELECT ts FROM pgperf.snapshot WHERE sid=_sid2 ) AS s2;

  RETURN _interval;
END
' LANGUAGE 'plpgsql';
CREATE TABLE pgperf.snapshot_pg_stat_bgwriter (
  sid INTEGER NOT NULL,

  checkpoints_timed bigint,
  checkpoints_req bigint,
  buffers_checkpoint bigint,
  buffers_clean bigint,
  maxwritten_clean bigint,
  buffers_backend bigint,
  buffers_alloc bigint
);

CREATE INDEX snapshot_pg_stat_bgwriter_sid_idx
  ON pgperf.snapshot_pg_stat_bgwriter(sid);

--
-- Create a snapshot for pg_stat_bgwriter
--
CREATE OR REPLACE FUNCTION pgperf.create_snapshot_pg_stat_bgwriter (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;

  _r RECORD;
BEGIN
  FOR _r IN SELECT checkpoints_timed,
                   checkpoints_req,
                   buffers_checkpoint,
                   buffers_clean,
                   maxwritten_clean,
                   buffers_backend,
                   buffers_alloc
              FROM pg_stat_bgwriter LOOP

    INSERT INTO pgperf.snapshot_pg_stat_bgwriter (sid,
                                                  checkpoints_timed,
                                                  checkpoints_req,
                                                  buffers_checkpoint,
                                                  buffers_clean,
                                                  maxwritten_clean,
                                                  buffers_backend,
                                                  buffers_alloc)
                                          VALUES (_sid,
                                                  _r.checkpoints_timed,
                                                  _r.checkpoints_req,
                                                  _r.buffers_checkpoint,
                                                  _r.buffers_clean,
                                                  _r.maxwritten_clean,
                                                  _r.buffers_backend,
                                                  _r.buffers_alloc);
  END LOOP;

  RETURN true;
END
' LANGUAGE 'plpgsql';

--
-- Delete a snapshot of pg_stat_bgwriter.
--
CREATE OR REPLACE FUNCTION pgperf.delete_snapshot_pg_stat_bgwriter (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;
BEGIN
  DELETE FROM pgperf.snapshot_pg_stat_bgwriter WHERE sid = _sid;

  RETURN true;
END
' LANGUAGE 'plpgsql';
CREATE TABLE pgperf.snapshot_pg_stat_database (
  sid INTEGER NOT NULL,

  datid oid,
  datname name,
  numbackends integer,
  xact_commit bigint,
  xact_rollback bigint,
  blks_read bigint,
  blks_hit bigint,
  tup_returned bigint,
  tup_fetched bigint,
  tup_inserted bigint,
  tup_updated bigint,
  tup_deleted bigint
);

CREATE INDEX snapshot_pg_stat_database_sid_idx
  ON pgperf.snapshot_pg_stat_database(sid);

--
-- Create a snapshot for pg_stat_database
--
CREATE OR REPLACE FUNCTION pgperf.create_snapshot_pg_stat_database (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;

  _r RECORD;
BEGIN
  FOR _r IN SELECT datid,
                   datname,
                   numbackends,
                   xact_commit,
                   xact_rollback,
                   blks_read,
                   blks_hit,
                   tup_returned,
                   tup_fetched,
                   tup_inserted,
                   tup_updated,
                   tup_deleted
              FROM pg_stat_database LOOP

    INSERT INTO pgperf.snapshot_pg_stat_database (sid,
                                                  datid,
                                                  datname,
                                                  numbackends,
                                                  xact_commit,
                                                  xact_rollback,
                                                  blks_read,
                                                  blks_hit,
                                                  tup_returned,
                                                  tup_fetched,
                                                  tup_inserted,
                                                  tup_updated,
                                                  tup_deleted)
                                          VALUES (_sid,
                                                  _r.datid,
                                                  _r.datname,
                                                  _r.numbackends,
                                                  _r.xact_commit,
                                                  _r.xact_rollback,
                                                  _r.blks_read,
                                                  _r.blks_hit,
                                                  _r.tup_returned,
                                                  _r.tup_fetched,
                                                  _r.tup_inserted,
                                                  _r.tup_updated,
                                                  _r.tup_deleted);
  END LOOP;

  RETURN true;
END
' LANGUAGE 'plpgsql';

--
-- Delete a snapshot of pg_stat_database.
--
CREATE OR REPLACE FUNCTION pgperf.delete_snapshot_pg_stat_database (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;
BEGIN
  DELETE FROM pgperf.snapshot_pg_stat_database WHERE sid = _sid;

  RETURN true;
END
' LANGUAGE 'plpgsql';
CREATE TABLE pgperf.snapshot_pg_relation_size (
  sid INTEGER NOT NULL,

  schemaname name,
  relid oid,
  relname name,
  pg_relation_size bigint,
  pg_total_relation_size bigint
);

CREATE INDEX snapshot_pg_relation_size_sid_idx
  ON pgperf.snapshot_pg_relation_size(sid);

--
-- Create a snapshot for pg_relation_size
--
CREATE OR REPLACE FUNCTION pgperf.create_snapshot_pg_relation_size (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;

  _r RECORD;
BEGIN
  FOR _r IN SELECT s.*,
                   c.relname,
                   pg_relation_size(relid),
                   CASE WHEN c.relkind = ''r'' THEN
                     pg_total_relation_size(relid)
                   ELSE
                     null
                   END AS pg_total_relation_size
              FROM (SELECT schemaname,relid FROM pg_stat_user_tables
                      UNION ALL
                    SELECT schemaname,indexrelid FROM pg_stat_user_indexes) AS s
                   LEFT OUTER JOIN pg_class c ON s.relid=c.oid
             WHERE schemaname <> ''pgperf''
               AND schemaname NOT LIKE ''pg\_%'' LOOP

    INSERT INTO pgperf.snapshot_pg_relation_size (sid,
                                                  schemaname,
                                                  relid,
                                                  relname,
                                                  pg_relation_size,
                                                  pg_total_relation_size)
                                          VALUES (_sid,
                                                  _r.schemaname,
                                                  _r.relid,
                                                  _r.relname,
                                                  _r.pg_relation_size,
                                                  _r.pg_total_relation_size);
  END LOOP;

  RETURN true;
END
' LANGUAGE 'plpgsql';

--
-- Delete a snapshot of pg_relation_size.
--
CREATE OR REPLACE FUNCTION pgperf.delete_snapshot_pg_relation_size (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;
BEGIN
  DELETE FROM pgperf.snapshot_pg_relation_size WHERE sid = _sid;

  RETURN true;
END
' LANGUAGE 'plpgsql';
CREATE TABLE pgperf.snapshot_pg_stat_user_tables (
  sid INTEGER NOT NULL,

  relid oid,
  schemaname name,
  relname name,
  seq_scan bigint,
  seq_tup_read bigint,
  idx_scan bigint,
  idx_tup_fetch bigint,
  n_tup_ins bigint,
  n_tup_upd bigint,
  n_tup_del bigint,
  n_tup_hot_upd bigint,
  n_live_tup bigint,
  n_dead_tup bigint,
  last_vacuum timestamp with time zone,
  last_autovacuum timestamp with time zone,
  last_analyze timestamp with time zone,
  last_autoanalyze timestamp with time zone
);

CREATE INDEX snapshot_pg_stat_user_tables_sid_idx
  ON pgperf.snapshot_pg_stat_user_tables(sid);

--
-- Create a snapshot for pg_stat_user_tables
--
CREATE OR REPLACE FUNCTION pgperf.create_snapshot_pg_stat_user_tables (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;

  _r RECORD;
BEGIN
  FOR _r IN SELECT relid,
                   schemaname,
                   relname,
                   seq_scan,
                   seq_tup_read,
                   idx_scan,
                   idx_tup_fetch,
                   n_tup_ins,
                   n_tup_upd,
                   n_tup_del,
                   n_tup_hot_upd,
                   n_live_tup,
                   n_dead_tup,
                   last_vacuum,
                   last_autovacuum,
                   last_analyze,
                   last_autoanalyze
              FROM pg_stat_user_tables
             WHERE schemaname NOT LIKE ''pg\_%''
               AND schemaname NOT LIKE ''information\_schema''
               AND schemaname NOT LIKE ''pgperf'' LOOP

    INSERT INTO pgperf.snapshot_pg_stat_user_tables (sid,
                                                     relid,
                                                     schemaname,
                                                     relname,
                                                     seq_scan,
                                                     seq_tup_read,
                                                     idx_scan,
                                                     idx_tup_fetch,
                                                     n_tup_ins,
                                                     n_tup_upd,
                                                     n_tup_del,
                                                     n_tup_hot_upd,
                                                     n_live_tup,
                                                     n_dead_tup,
                                                     last_vacuum,
                                                     last_autovacuum,
                                                     last_analyze,
                                                     last_autoanalyze)
                                             VALUES (_sid,
                                                     _r.relid,
                                                     _r.schemaname,
                                                     _r.relname,
                                                     _r.seq_scan,
                                                     _r.seq_tup_read,
                                                     _r.idx_scan,
                                                     _r.idx_tup_fetch,
                                                     _r.n_tup_ins,
                                                     _r.n_tup_upd,
                                                     _r.n_tup_del,
                                                     _r.n_tup_hot_upd,
                                                     _r.n_live_tup,
                                                     _r.n_dead_tup,
                                                     _r.last_vacuum,
                                                     _r.last_autovacuum,
                                                     _r.last_analyze,
                                                     _r.last_autoanalyze);
  END LOOP;

  RETURN true;
END
' LANGUAGE 'plpgsql';

--
-- Delete a snapshot of pg_stat_user_tables.
--
CREATE OR REPLACE FUNCTION pgperf.delete_snapshot_pg_stat_user_tables (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;
BEGIN
  DELETE FROM pgperf.snapshot_pg_stat_user_tables WHERE sid = _sid;

  RETURN true;
END
' LANGUAGE 'plpgsql';
CREATE TABLE pgperf.snapshot_pg_statio_user_tables (
  sid INTEGER NOT NULL,

  relid oid,
  schemaname name,
  relname name,
  heap_blks_read bigint,
  heap_blks_hit bigint,
  idx_blks_read bigint,
  idx_blks_hit bigint,
  toast_blks_read bigint,
  toast_blks_hit bigint,
  tidx_blks_read bigint,
  tidx_blks_hit bigint
);

CREATE INDEX snapshot_pg_statio_user_tables_sid_idx
  ON pgperf.snapshot_pg_statio_user_tables(sid);

--
-- Create a snapshot for pg_statio_user_tables
--
CREATE OR REPLACE FUNCTION pgperf.create_snapshot_pg_statio_user_tables (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;

  _r RECORD;
BEGIN
  FOR _r IN SELECT relid,
                   schemaname,
                   relname,
                   heap_blks_read,
                   heap_blks_hit,
                   idx_blks_read,
                   idx_blks_hit,
                   toast_blks_read,
                   toast_blks_hit,
                   tidx_blks_read,
                   tidx_blks_hit
              FROM pg_statio_user_tables
             WHERE schemaname NOT LIKE ''pg\_%''
               AND schemaname NOT LIKE ''information\_schema''
               AND schemaname NOT LIKE ''pgperf'' LOOP

    INSERT INTO pgperf.snapshot_pg_statio_user_tables (sid,
                                                       relid,
                                                       schemaname,
                                                       relname,
                                                       heap_blks_read,
                                                       heap_blks_hit,
                                                       idx_blks_read,
                                                       idx_blks_hit,
                                                       toast_blks_read,
                                                       toast_blks_hit,
                                                       tidx_blks_read,
                                                       tidx_blks_hit)
                                               VALUES (_sid,
                                                       _r.relid,
                                                       _r.schemaname,
                                                       _r.relname,
                                                       _r.heap_blks_read,
                                                       _r.heap_blks_hit,
                                                       _r.idx_blks_read,
                                                       _r.idx_blks_hit,
                                                       _r.toast_blks_read,
                                                       _r.toast_blks_hit,
                                                       _r.tidx_blks_read,
                                                       _r.tidx_blks_hit);
  END LOOP;

  RETURN true;
END
' LANGUAGE 'plpgsql';

--
-- Delete a snapshot of pg_statio_user_tables.
--
CREATE OR REPLACE FUNCTION pgperf.delete_snapshot_pg_statio_user_tables (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;
BEGIN
  DELETE FROM pgperf.snapshot_pg_statio_user_tables WHERE sid = _sid;

  RETURN true;
END
' LANGUAGE 'plpgsql';
CREATE TABLE pgperf.snapshot_pg_stat_user_indexes (
  sid INTEGER NOT NULL,

  relid oid,
  indexrelid oid,
  schemaname name,
  relname name,
  indexrelname name,
  idx_scan bigint,
  idx_tup_read bigint,
  idx_tup_fetch bigint
);

CREATE INDEX snapshot_pg_stat_user_indexes_sid_idx
  ON pgperf.snapshot_pg_stat_user_indexes(sid);

--
-- Create a snapshot for pg_stat_user_indexes
--
CREATE OR REPLACE FUNCTION pgperf.create_snapshot_pg_stat_user_indexes (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;

  _r RECORD;
BEGIN
  FOR _r IN SELECT relid,
                   indexrelid,
                   schemaname,
                   relname,
                   indexrelname,
                   idx_scan,
                   idx_tup_read,
                   idx_tup_fetch
              FROM pg_stat_user_indexes
             WHERE schemaname NOT LIKE ''pg\_%''
               AND schemaname NOT LIKE ''information\_schema''
               AND schemaname NOT LIKE ''pgperf'' LOOP

    INSERT INTO pgperf.snapshot_pg_stat_user_indexes (sid,
                                                      relid,
                                                      indexrelid,
                                                      schemaname,
                                                      relname,
                                                      indexrelname,
                                                      idx_scan,
                                                      idx_tup_read,
                                                      idx_tup_fetch)
                                              VALUES (_sid,
                                                      _r.relid,
                                                      _r.indexrelid,
                                                      _r.schemaname,
                                                      _r.relname,
                                                      _r.indexrelname,
                                                      _r.idx_scan,
                                                      _r.idx_tup_read,
                                                      _r.idx_tup_fetch);
  END LOOP;

  RETURN true;
END
' LANGUAGE 'plpgsql';

--
-- Delete a snapshot of pg_stat_user_indexes.
--
CREATE OR REPLACE FUNCTION pgperf.delete_snapshot_pg_stat_user_indexes (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;
BEGIN
  DELETE FROM pgperf.snapshot_pg_stat_user_indexes WHERE sid = _sid;

  RETURN true;
END
' LANGUAGE 'plpgsql';
CREATE TABLE pgperf.snapshot_pg_statio_user_indexes (
  sid INTEGER NOT NULL,

  relid oid,
  indexrelid oid,
  schemaname name,
  relname name,
  indexrelname name,
  idx_blks_read bigint,
  idx_blks_hit bigint
);

CREATE INDEX snapshot_pg_statio_user_indexes_sid_idx
  ON pgperf.snapshot_pg_statio_user_indexes(sid);

--
-- Create a snapshot for pg_statio_user_indexes
--
CREATE OR REPLACE FUNCTION pgperf.create_snapshot_pg_statio_user_indexes (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;

  _r RECORD;
BEGIN
  FOR _r IN SELECT relid,
                   indexrelid,
                   schemaname,
                   relname,
                   indexrelname,
                   idx_blks_read,
                   idx_blks_hit
              FROM pg_statio_user_indexes
             WHERE schemaname NOT LIKE ''pg\_%''
               AND schemaname NOT LIKE ''information\_schema''
               AND schemaname NOT LIKE ''pgperf'' LOOP

    INSERT INTO pgperf.snapshot_pg_statio_user_indexes (sid,
                                                        relid,
                                                        indexrelid,
                                                        schemaname,
                                                        relname,
                                                        indexrelname,
                                                        idx_blks_read,
                                                        idx_blks_hit)
                                                VALUES (_sid,
                                                        _r.relid,
                                                        _r.indexrelid,
                                                        _r.schemaname,
                                                        _r.relname,
                                                        _r.indexrelname,
                                                        _r.idx_blks_read,
                                                        _r.idx_blks_hit);
  END LOOP;

  RETURN true;
END
' LANGUAGE 'plpgsql';

--
-- Delete a snapshot of pg_statio_user_indexes.
--
CREATE OR REPLACE FUNCTION pgperf.delete_snapshot_pg_statio_user_indexes (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;
BEGIN
  DELETE FROM pgperf.snapshot_pg_statio_user_indexes WHERE sid = _sid;

  RETURN true;
END
' LANGUAGE 'plpgsql';
CREATE TABLE pgperf.snapshot_pg_statistic (
  sid INTEGER NOT NULL,

  starelid oid,
  starelname name,
  staattnum int2,
  staattname name,
  stanullfrac float4,
  stawidth int4,
  stadistinct float4,
  stakind1 int2,
  stakind2 int2,
  stakind3 int2,
  stakind4 int2,
  staop1 oid,
  staop2 oid,
  staop3 oid,
  staop4 oid,
  stanumbers1 float4[],
  stanumbers2 float4[],
  stanumbers3 float4[],
  stanumbers4 float4[],
  stavalues1 text,
  stavalues2 text,
  stavalues3 text,
  stavalues4 text
);

CREATE INDEX snapshot_pg_statistic_sid_idx
  ON pgperf.snapshot_pg_statistic(sid);

--
-- Create a snapshot for pg_statistic
--
CREATE OR REPLACE FUNCTION pgperf.create_snapshot_pg_statistic (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;

  _r RECORD;
BEGIN
  FOR _r IN SELECT s.starelid,
                   c.relname as starelname,
                   s.staattnum,
                   a.attname as staattname,
                   s.stanullfrac,
                   s.stawidth,
                   s.stadistinct,
                   s.stakind1,
                   s.stakind2,
                   s.stakind3,
                   s.stakind4,
                   s.staop1,
                   s.staop2,
                   s.staop3,
                   s.staop4,
                   s.stanumbers1,
                   s.stanumbers2,
                   s.stanumbers3,
                   s.stanumbers4,
                   s.stavalues1,
                   s.stavalues2,
                   s.stavalues3,
                   s.stavalues4
              FROM pg_statistic s, pg_class c, pg_namespace n, pg_attribute a
             WHERE n.nspname NOT LIKE ''pg\_%''
               AND n.nspname NOT LIKE ''information_schema''
               AND n.nspname NOT LIKE ''pgperf''
               AND n.oid = c.relnamespace
               AND c.oid = s.starelid
               AND a.attnum = s.staattnum
               AND a.attrelid = s.starelid LOOP
    INSERT INTO pgperf.snapshot_pg_statistic (sid,
                                              starelid,
                                              starelname,
                                              staattnum,
                                              staattname,
                                              stanullfrac,
                                              stawidth,
                                              stadistinct,
                                              stakind1,
                                              stakind2,
                                              stakind3,
                                              stakind4,
                                              staop1,
                                              staop2,
                                              staop3,
                                              staop4,
                                              stanumbers1,
                                              stanumbers2,
                                              stanumbers3,
                                              stanumbers4,
                                              stavalues1,
                                              stavalues2,
                                              stavalues3,
                                              stavalues4)
                                      VALUES (_sid,
                                              _r.starelid,
                                              _r.starelname,
                                              _r.staattnum,
                                              _r.staattname,
                                              _r.stanullfrac,
                                              _r.stawidth,
                                              _r.stadistinct,
                                              _r.stakind1,
                                              _r.stakind2,
                                              _r.stakind3,
                                              _r.stakind4,
                                              _r.staop1,
                                              _r.staop2,
                                              _r.staop3,
                                              _r.staop4,
                                              _r.stanumbers1,
                                              _r.stanumbers2,
                                              _r.stanumbers3,
                                              _r.stanumbers4,
                                              _r.stavalues1::text,
                                              _r.stavalues2::text,
                                              _r.stavalues3::text,
                                              _r.stavalues4::text);
  END LOOP;

  RETURN true;
END
' LANGUAGE 'plpgsql';

--
-- Delete a snapshot of pg_statistic.
--
CREATE OR REPLACE FUNCTION pgperf.delete_snapshot_pg_statistic (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;
BEGIN
  DELETE FROM pgperf.snapshot_pg_statistic WHERE sid = _sid;

  RETURN true;
END
' LANGUAGE 'plpgsql';
CREATE TABLE pgperf.snapshot_pg_current_xlog (
  sid INTEGER NOT NULL,

  location text,
  insert_location text
);

CREATE INDEX snapshot_pg_current_xlog_sid_idx
  ON pgperf.snapshot_pg_current_xlog(sid);

--
-- Create a snapshot for pg_current_xlog_location/current_location.
--
CREATE OR REPLACE FUNCTION pgperf.create_snapshot_pg_current_xlog (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;

  _r RECORD;
BEGIN
  FOR _r IN SELECT pg_current_xlog_location(),
                   pg_current_xlog_insert_location() LOOP

    INSERT INTO pgperf.snapshot_pg_current_xlog (sid,
                                                 location,
                                                 insert_location)
                                          VALUES (_sid,
                                                  _r.pg_current_xlog_location,
                                                  _r.pg_current_xlog_insert_location);
  END LOOP;

  RETURN true;
END
' LANGUAGE 'plpgsql';

--
-- Delete a snapshot of pg_current_xlog_location/current_location.
--
CREATE OR REPLACE FUNCTION pgperf.delete_snapshot_pg_current_xlog (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;
BEGIN
  DELETE FROM pgperf.snapshot_pg_current_xlog WHERE sid = _sid;

  RETURN true;
END
' LANGUAGE 'plpgsql';
CREATE TABLE pgperf.snapshot_pg_stat_activity (
  sid INTEGER NOT NULL,

  datid oid,
  datname name,
  procpid int4,
  usesysid oid,
  usename name,
  current_query text,
  waiting bool,
  xact_start timestamptz,
  query_start timestamptz,
  backend_start timestamptz,
  client_addr inet,
  client_port int4
);

CREATE INDEX snapshot_pg_stat_activity_sid_idx
  ON pgperf.snapshot_pg_stat_activity(sid);

--
-- Create a snapshot for pg_stat_activity
--
CREATE OR REPLACE FUNCTION pgperf.create_snapshot_pg_stat_activity (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;

  _r RECORD;
BEGIN
  FOR _r IN SELECT datid,
                   datname,
                   procpid,
                   usesysid,
                   usename,
                   current_query,
                   waiting,
                   xact_start,
                   query_start,
                   backend_start,
                   client_addr,
                   client_port
              FROM pg_stat_activity LOOP

    INSERT INTO pgperf.snapshot_pg_stat_activity (sid,
                                                  datid,
                                                  datname,
                                                  procpid,
                                                  usesysid,
                                                  usename,
                                                  current_query,
                                                  waiting,
                                                  xact_start,
                                                  query_start,
                                                  backend_start,
                                                  client_addr,
                                                  client_port)
                                          VALUES (_sid,
                                                  _r.datid,
                                                  _r.datname,
                                                  _r.procpid,
                                                  _r.usesysid,
                                                  _r.usename,
                                                  _r.current_query,
                                                  _r.waiting,
                                                  _r.xact_start,
                                                  _r.query_start,
                                                  _r.backend_start,
                                                  _r.client_addr,
                                                  _r.client_port);
  END LOOP;

  RETURN true;
END
' LANGUAGE 'plpgsql';

--
-- Delete a snapshot of pg_stat_activity
--
CREATE OR REPLACE FUNCTION pgperf.delete_snapshot_pg_stat_activity (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;
BEGIN
  DELETE FROM pgperf.snapshot_pg_stat_activity WHERE sid = _sid;

  RETURN true;
END
' LANGUAGE 'plpgsql';
CREATE TABLE pgperf.snapshot_pg_locks (
  sid INTEGER NOT NULL,

  locktype text,
  database oid,
  relation oid,
  page int4,
  tuple int2,
  virtualxid text,
  transactionid xid,
  classid oid,
  objid oid,
  objsubid int2,
  virtualtransaction text,
  pid int4,
  mode text,
  granted bool
);

CREATE INDEX snapshot_pg_locks_sid_idx
  ON pgperf.snapshot_pg_locks(sid);

--
-- Create a snapshot for pg_locks
--
CREATE OR REPLACE FUNCTION pgperf.create_snapshot_pg_locks (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;

  _r RECORD;
BEGIN
  FOR _r IN SELECT locktype,
                   database,
                   relation,
                   page,
                   tuple,
                   virtualxid,
                   transactionid,
                   classid,
                   objid,
                   objsubid,
                   virtualtransaction,
                   pid,
                   mode,
                   granted
              FROM pg_locks LOOP

    INSERT INTO pgperf.snapshot_pg_locks (sid,
                                          locktype,
                                          database,
                                          relation,
                                          page,
                                          tuple,
                                          virtualxid,
                                          transactionid,
                                          classid,
                                          objid,
                                          objsubid,
                                          virtualtransaction,
                                          pid,
                                          mode,
                                          granted)
                                  VALUES (_sid,
                                          _r.locktype,
                                          _r.database,
                                          _r.relation,
                                          _r.page,
                                          _r.tuple,
                                          _r.virtualxid,
                                          _r.transactionid,
                                          _r.classid,
                                          _r.objid,
                                          _r.objsubid,
                                          _r.virtualtransaction,
                                          _r.pid,
                                          _r.mode,
                                          _r.granted);
  END LOOP;

  RETURN true;
END
' LANGUAGE 'plpgsql';

--
-- Delete a snapshot of pg_locks
--
CREATE OR REPLACE FUNCTION pgperf.delete_snapshot_pg_locks (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;
BEGIN
  DELETE FROM pgperf.snapshot_pg_locks WHERE sid = _sid;

  RETURN true;
END
' LANGUAGE 'plpgsql';
--
-- PostgreSQL 8.4 does not have pg_stat_replication.
--

--
-- PostgreSQL 8.4 does not have pg_stat_database_conflicts.
--

CREATE TABLE pgperf.snapshot_pg_stat_statements (
  sid INTEGER NOT NULL,

  userid OID,
  dbid OID,
  query TEXT,
  calls bigint,
  total_time double precision,
  rows bigint
);

CREATE INDEX snapshot_pg_stat_statements_sid_idx
  ON pgperf.snapshot_pg_stat_statements(sid);

--
-- Create a snapshot for pg_stat_statements.
--
CREATE OR REPLACE FUNCTION pgperf.create_snapshot_pg_stat_statements (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;

  _r RECORD;
BEGIN
  FOR _r IN SELECT userid,
                        dbid,
                        query,
                        calls,
                        total_time,
                        rows
                   FROM pg_stat_statements LOOP

    INSERT INTO pgperf.snapshot_pg_stat_statements (
                        sid,
                        userid,
                        dbid,
                        query,
                        calls,
                        total_time,
                        rows
                   ) VALUES (
                        _sid,
                        _r.userid,
                        _r.dbid,
                        _r.query,
                        _r.calls,
                        _r.total_time,
                        _r.rows);
  END LOOP;

  RETURN true;
END
' LANGUAGE 'plpgsql';

--
-- Delete a snapshot of pg_stat_statements.
--
CREATE OR REPLACE FUNCTION pgperf.delete_snapshot_pg_stat_statements (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;
BEGIN
  DELETE FROM pgperf.snapshot_pg_stat_statements WHERE sid = _sid;

  RETURN true;
END
' LANGUAGE 'plpgsql';

CREATE TABLE pgperf.snapshot_pgstattuple (
  sid INTEGER NOT NULL,

  relname name,
  table_len int8,
  tuple_count int8,
  tuple_len int8,
  tuple_percent float8,
  dead_tuple_count int8,
  dead_tuple_len int8,
  dead_tuple_percent float8,
  free_space int8,
  free_percent float8
);

CREATE INDEX snapshot_pgstattuple_sid_idx
  ON pgperf.snapshot_pgstattuple(sid);

--
-- Create a snapshot for pgstattuple
--
CREATE OR REPLACE FUNCTION pgperf.create_snapshot_pgstattuple (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;

  _r RECORD;
  _relname NAME;
BEGIN
  FOR _relname IN SELECT c.relname FROM pg_namespace n, pg_class c
                  WHERE n.nspname NOT LIKE ''pg\_%''
                    AND n.nspname NOT LIKE ''information_schema''
                    AND n.nspname NOT LIKE ''pgperf''
                    AND n.oid=c.relnamespace
                    AND c.relkind=''r'' LOOP

    FOR _r IN SELECT _relname,
                     table_len,
                     tuple_count,
                     tuple_len,
                     tuple_percent,
                     dead_tuple_count,
                     dead_tuple_len,
                     dead_tuple_percent,
                     free_space,
                     free_percent
                FROM pgstattuple(_relname) LOOP

      INSERT INTO pgperf.snapshot_pgstattuple (sid,
                                               relname,
                                               table_len,
                                               tuple_count,
                                               tuple_len,
                                               tuple_percent,
                                               dead_tuple_count,
                                               dead_tuple_len,
                                               dead_tuple_percent,
                                               free_space,
                                               free_percent)
                                       VALUES (_sid,
                                               _relname,
                                               _r.table_len,
                                               _r.tuple_count,
                                               _r.tuple_len,
                                               _r.tuple_percent,
                                               _r.dead_tuple_count,
                                               _r.dead_tuple_len,
                                               _r.dead_tuple_percent,
                                               _r.free_space,
                                               _r.free_percent);
    END LOOP;
  END LOOP;

  RETURN true;
END
' LANGUAGE 'plpgsql';

--
-- Delete a snapshot of pgstattuple.
--
CREATE OR REPLACE FUNCTION pgperf.delete_snapshot_pgstattuple (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;
BEGIN
  DELETE FROM pgperf.snapshot_pgstattuple WHERE sid = _sid;

  RETURN true;
END
' LANGUAGE 'plpgsql';
CREATE TABLE pgperf.snapshot_pgstatindex (
  sid INTEGER NOT NULL,

  relname name,
  version int4,
  tree_level int4,
  index_size int8,
  root_block_no int8,
  internal_pages int8,
  leaf_pages int8,
  empty_pages int8,
  deleted_pages int8,
  avg_leaf_density float8,
  leaf_fragmentation float8
);

CREATE INDEX snapshot_pgstatindex_sid_idx
  ON pgperf.snapshot_pgstatindex(sid);

--
-- Create a snapshot for pgstatindex
--
CREATE OR REPLACE FUNCTION pgperf.create_snapshot_pgstatindex (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;

  _r RECORD;
  _relname NAME;
BEGIN
  FOR _relname IN SELECT c.relname
                    FROM pg_namespace n, pg_class c, pg_am a
                   WHERE n.nspname NOT LIKE ''pg\_%''
                     AND n.nspname NOT LIKE ''information_schema''
                     AND n.nspname NOT LIKE ''pgperf''
                     AND n.oid=c.relnamespace
                     AND c.relam=a.oid
                     AND a.amname=''btree''
                     AND c.relkind=''i'' LOOP

    FOR _r IN SELECT _relname,
                     version,
                     tree_level,
                     index_size,
                     root_block_no,
                     internal_pages,
                     leaf_pages,
                     empty_pages,
                     deleted_pages,
                     avg_leaf_density,
                     leaf_fragmentation
                FROM pgstatindex(_relname) LOOP

      INSERT INTO pgperf.snapshot_pgstatindex (sid,
                                               relname,
                                               version,
                                               tree_level,
                                               index_size,
                                               root_block_no,
                                               internal_pages,
                                               leaf_pages,
                                               empty_pages,
                                               deleted_pages,
                                               avg_leaf_density,
                                               leaf_fragmentation)
                                       VALUES (_sid,
                                               _relname,
                                               _r.version,
                                               _r.tree_level,
                                               _r.index_size,
                                               _r.root_block_no,
                                               _r.internal_pages,
                                               _r.leaf_pages,
                                               _r.empty_pages,
                                               _r.deleted_pages,
                                               _r.avg_leaf_density,
                                               _r.leaf_fragmentation);
    END LOOP;
  END LOOP;

  RETURN true;
END
' LANGUAGE 'plpgsql';

--
-- Delete a snapshot of pgstatindex.
--
CREATE OR REPLACE FUNCTION pgperf.delete_snapshot_pgstatindex (
  INTEGER
) RETURNS boolean
AS '
DECLARE
  _sid ALIAS FOR $1;
BEGIN
  DELETE FROM pgperf.snapshot_pgstatindex WHERE sid = _sid;

  RETURN true;
END
' LANGUAGE 'plpgsql';
commit;
