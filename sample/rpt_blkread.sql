--
-- Report buffer reads and physical block reads in last 32 days.
--
-- Copyright(C) Uptime Technologies, LLC. All rights reserved.
--
SELECT date_trunc('second', s.ts) AS "timestamp",
       round( ((sum(blks_hit) - lag(sum(blks_hit)) OVER (ORDER BY s.sid))::float / 
         pgperf.get_interval(lag(s.sid) OVER (ORDER BY s.ts), s.sid))::numeric, 2) AS "blks_hit/sec",
       round( ((sum(blks_read) - lag(sum(blks_read)) OVER (ORDER BY s.sid))::float / 
         pgperf.get_interval(lag(s.sid) OVER (ORDER BY s.ts), s.sid))::numeric, 2) AS "blks_read/sec"
  FROM pgperf.snapshot_pg_stat_database d, pgperf.snapshot s
 WHERE d.sid = s.sid
   AND s.ts BETWEEN date_trunc('day', now()) - '32 days'::interval AND date_trunc('day', now()) + '1 day'::interval
 GROUP BY s.sid
 ORDER BY s.ts;

