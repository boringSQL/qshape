SELECT s.queryid, s.calls, s.query,
       s.total_exec_time, s.mean_exec_time, s.stddev_exec_time, s.rows
FROM pg_stat_statements s
