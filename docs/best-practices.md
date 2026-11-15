# AWS best practices and what this server checks

Each row maps one piece of AWS guidance to the tool or finding that checks it. The last section lists what the server
does not check. Thresholds here are the ones in the code, which are close to, but not always the same as, the AWS numbers.

## What is checked

| AWS guidance | Where it is checked |
| --- | --- |
| Watch `EngineCPUUtilization`, not only host CPU. Valkey and Redis OSS run commands on one thread. | `aws_metrics` and `aws_triage` raise findings above 80% (critical when the average is above 80% or the peak above 90%). Scale advice starts at 65% p95 on the busiest shard. |
| Alarm on `DatabaseMemoryUsagePercentage`, not raw bytes. | `aws_metrics` warns above 80% and goes critical above 90%. `node_health` and `node_memory` read the same ratio from `INFO`. |
| Keep room for snapshots and fork. `reserved-memory-percent` is 25 by default. | `node_config` and `node_health` explain `reserved-memory-percent` in the parameter quirks. `aws_snapshot_restore_readiness` checks that a restored node keeps about 25% of its memory free. |
| Pick a `maxmemory-policy` that fits the workload. With `noeviction`, writes fail when memory is full. | `node_health`, `node_memory` and `node_triage` flag `noeviction` near `maxmemory` as critical. `aws_parameters` flags risky values. |
| Alert on evictions and a falling hit rate. | `aws_metrics` and `node_triage` flag evictions. Memcached findings combine evictions with a hit ratio below 80%. |
| Use connection pooling and watch `CurrConnections` for leaks. | `node_clients_summary` groups clients by IP, name and idle time. `node_triage` flags many clients idle for over an hour (a leak or a missing idle timeout). |
| Set alarms on the metrics AWS lists. | `aws_alarms` lists alarms in ALARM or with actions off, and replication group members with no alarm on `EngineCPUUtilization`. |
| Serverless: alarm at 75% of the limit you set. | `aws_serverless_triage` compares ECPU and storage with the configured limits and flags a cache near its limit (80%). |
| Use cluster mode for horizontal scale and spread keys evenly. | `node_cluster_triage`, `node_hot_slots` and `node_cluster_health` check slot coverage and uneven shards. |
| Use encryption in transit, at rest and AUTH or RBAC. | `aws_security_audit` audits encryption, auth, user groups and parameter groups. |
| Enable Multi-AZ and automatic failover. | `aws_triage` and `aws_cluster_overview` report the failover and Multi-AZ state per replication group. |

## Not checked

- **Client settings.** AWS advises a socket timeout of at least 1 second, pooled connections and retry with backoff.
  The server only sees the effect (idle clients, connection storms), not the client configuration.
- **Host CPU scaled by core count.** AWS suggests dividing the 90% host `CPUUtilization` threshold by the number of
  cores on small nodes. The server uses a flat 90% for host CPU and relies on `EngineCPUUtilization` instead.
- **Auto scaling targets.** AWS suggests picking the target from four weeks of utilization data. The server reads the
  current window only.
- **Cost.** Reserved nodes and right-sizing are out of scope.

## Sources

- [Which metrics should I monitor?](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/CacheMetrics.WhichShouldIMonitor.html)
- [Monitoring best practices with ElastiCache and CloudWatch](https://aws.amazon.com/blogs/database/monitoring-best-practices-with-amazon-elasticache-for-redis-using-amazon-cloudwatch/)
- [Overall best practices](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/WorkingWithRedis.html)
- [Persistent connection issues](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/TroubleshootingConnections.html)
- [Well-Architected Lens: Performance Efficiency](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/PerformanceEfficiencyPillar.html)
- [Ensuring you have enough memory to make a snapshot](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/BestPractices.BGSAVE.html)

[Back to the README](../README.md)
