--
-- Report buffers written by checkpoint, bgwriter and backends in last 32 days.
--
-- Copyright(C) Uptime Technologies, LLC. All rights reserved.
--
SELECT date_trunc('second', s.ts) AS "timestamp",
       round( (( buffers_checkpoint - lag(buffers_checkpoint) OVER (ORDER BY s.ts) )::float /
         pgperf.get_interval(lag(s.sid) OVER (ORDER BY s.ts), s.sid))::numeric, 2) AS "blks_cp/sec",
       round( (( buffers_clean - lag(buffers_clean) OVER (ORDER BY s.ts) )::float /
         pgperf.get_interval(lag(s.sid) OVER (ORDER BY s.ts), s.sid))::numeric, 2) AS "blks_bg/sec",
       round( (( buffers_backend - lag(buffers_backend) OVER (ORDER BY s.ts) )::float /
         pgperf.get_interval(lag(s.sid) OVER (ORDER BY s.ts), s.sid))::numeric, 2) AS "blks_be/sec"
  FROM pgperf.snapshot s,
       pgperf.snapshot_pg_stat_bgwriter b
 WHERE s.sid=b.sid
   AND s.ts BETWEEN date_trunc('day', now()) - '32 days'::interval AND date_trunc('day', now()) + '1 day'::interval
 ORDER BY s.ts;
