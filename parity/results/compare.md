
### churn (reference: upstream/sqlite/p1 run 0)

| impl/store/partitions | run | added | add err | delivered | lost | dup | sync % | lat p50 ms | lat p99 ms | backlog p50/p99 ms | sync p50/p99 ms | complete | elapsed s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| upstream/sqlite/p1 | 0 | 5686 | 0 | 5686 | 0 | 0 | 84.8 | 0.1 | 1244.4 | 378.6/1619.9 | 0.1/0.2 | true | 15.0 |
| upstream/sqlite/p1 | 1 | 5686 | 0 | 5686 | 0 | 0 | 84.8 | 0.1 | 1243.8 | 379.4/1619.5 | 0.1/0.2 | true | 15.0 |
| upstream/sqlite/p1 | 2 | 5686 | 0 | 5686 | 0 | 0 | 84.8 | 0.1 | 1244.5 | 379.1/1620.4 | 0.1/0.2 | true | 15.0 |
| ours/mem/p1 | 0 | 5686 | 0 | 5686 | 0 | 0 | 84.9 | 0.0 | 1244.6 | 383.9/1619.5 | 0.0/0.1 | true | 15.0 |
| ours/mem/p1 | 1 | 5686 | 0 | 5686 | 0 | 0 | 84.9 | 0.0 | 1244.7 | 384.1/1619.3 | 0.0/0.0 | true | 15.0 |
| ours/mem/p1 | 2 | 5686 | 0 | 5686 | 0 | 0 | 84.9 | 0.0 | 1243.4 | 382.2/1619.3 | 0.0/0.0 | true | 15.0 |
| ours/sqlite/p1 | 0 | 5686 | 0 | 5686 | 0 | 0 | 84.9 | 0.0 | 1244.1 | 384.3/1619.0 | 0.0/0.0 | true | 15.0 |
| ours/sqlite/p1 | 1 | 5686 | 0 | 5686 | 0 | 0 | 84.9 | 0.0 | 1244.2 | 382.7/1618.5 | 0.0/0.0 | true | 15.0 |
| ours/sqlite/p1 | 2 | 5686 | 0 | 5686 | 0 | 0 | 84.9 | 0.0 | 1244.1 | 383.5/1619.9 | 0.0/0.1 | true | 15.0 |
| upstream/mem/p1 | 0 | 5686 | 0 | 5686 | 0 | 445 | 84.8 | 0.1 | 1244.3 | 379.9/1619.6 | 0.1/0.2 | true | 15.0 |
| upstream/sqlite/p4 | 0 | 5686 | 0 | 5686 | 0 | 0 | 36.1 | 45.7 | 1375.6 | 138.8/1490.1 | 0.1/0.3 | true | 16.0 |
| upstream/sqlite/p4 | 1 | 5686 | 0 | 5686 | 0 | 0 | 37.5 | 39.1 | 1397.8 | 132.6/1527.4 | 0.1/0.3 | true | 15.0 |
| upstream/sqlite/p4 | 2 | 5686 | 0 | 5686 | 0 | 0 | 36.7 | 45.6 | 1323.2 | 144.5/1464.8 | 0.1/0.2 | true | 15.8 |

Order vs reference, per queue (exact = positional match rate; tau = Kendall rank correlation; L1 = mean per-key share distance over 100-delivery windows, 0 identical, 2 disjoint; wrong = priority wrongorderness):

| impl/store/partitions | run | queue | exact | tau | L1 | wrong | FIFO exact | FIFO L1 |
|---|---|---|---|---|---|---|---|---|
(30 queues; per-queue rows omitted)

Mean over queues and runs (excluding the reference itself):

| impl/store/partitions | exact | tau | L1 | queue-runs |
|---|---|---|---|---|
| ours/mem/p1 | 0.997 | 1.000 | 0.000 | 84 |
| ours/sqlite/p1 | 0.995 | 1.000 | 0.000 | 84 |
| upstream/mem/p1 | 0.994 | 1.000 | 0.000 | 28 |
| upstream/sqlite/p1 | 0.998 | 1.000 | 0.000 | 56 |
| upstream/sqlite/p4 | 0.334 | 0.972 | 0.018 | 84 |

### det-fair (reference: upstream/sqlite/p1 run 0)

| impl/store/partitions | run | added | add err | delivered | lost | dup | sync % | lat p50 ms | lat p99 ms | backlog p50/p99 ms | sync p50/p99 ms | complete | elapsed s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| upstream/sqlite/p1 | 0 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 986.4 | 1613.3 | 986.4/1613.3 | -/- | true | 1.6 |
| upstream/sqlite/p1 | 1 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 990.1 | 1603.9 | 990.1/1603.9 | -/- | true | 1.6 |
| upstream/sqlite/p1 | 2 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 924.7 | 1565.7 | 924.7/1565.7 | -/- | true | 1.6 |
| ours/mem/p1 | 0 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 934.8 | 1208.9 | 934.8/1208.9 | -/- | true | 1.3 |
| ours/mem/p1 | 1 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 927.1 | 1308.2 | 927.1/1308.2 | -/- | true | 1.4 |
| ours/mem/p1 | 2 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 837.0 | 1275.7 | 837.0/1275.7 | -/- | true | 1.3 |
| ours/sqlite/p1 | 0 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 1076.3 | 1881.2 | 1076.3/1881.2 | -/- | true | 2.0 |
| ours/sqlite/p1 | 1 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 963.7 | 1715.6 | 963.7/1715.6 | -/- | true | 1.8 |
| ours/sqlite/p1 | 2 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 1000.5 | 1758.0 | 1000.5/1758.0 | -/- | true | 1.8 |
| upstream/mem/p1 | 0 | 4500 | 0 | 4500 | 0 | 47 | 0.0 | 944.6 | 1444.9 | 944.6/1444.9 | -/- | true | 1.5 |
| upstream/sqlite/p4 | 0 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 974.5 | 2561.7 | 974.5/2561.7 | -/- | true | 18.7 |
| upstream/sqlite/p4 | 1 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 1017.7 | 2549.1 | 1017.7/2549.1 | -/- | true | 8.7 |
| upstream/sqlite/p4 | 2 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 995.3 | 2555.8 | 995.3/2555.8 | -/- | true | 4.6 |

Order vs reference, per queue (exact = positional match rate; tau = Kendall rank correlation; L1 = mean per-key share distance over 100-delivery windows, 0 identical, 2 disjoint; wrong = priority wrongorderness):

| impl/store/partitions | run | queue | exact | tau | L1 | wrong | FIFO exact | FIFO L1 |
|---|---|---|---|---|---|---|---|---|
| upstream/sqlite/p1 | 0 | det-override | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.892 |
| upstream/sqlite/p1 | 0 | det-pri | 1.000 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| upstream/sqlite/p1 | 0 | det-taskw | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.854 |
| upstream/sqlite/p1 | 1 | det-override | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.892 |
| upstream/sqlite/p1 | 1 | det-pri | 1.000 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| upstream/sqlite/p1 | 1 | det-taskw | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.854 |
| upstream/sqlite/p1 | 2 | det-override | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.892 |
| upstream/sqlite/p1 | 2 | det-pri | 1.000 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| upstream/sqlite/p1 | 2 | det-taskw | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.854 |
| ours/mem/p1 | 0 | det-override | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.892 |
| ours/mem/p1 | 0 | det-pri | 1.000 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| ours/mem/p1 | 0 | det-taskw | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.854 |
| ours/mem/p1 | 1 | det-override | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.892 |
| ours/mem/p1 | 1 | det-pri | 1.000 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| ours/mem/p1 | 1 | det-taskw | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.854 |
| ours/mem/p1 | 2 | det-override | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.892 |
| ours/mem/p1 | 2 | det-pri | 1.000 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| ours/mem/p1 | 2 | det-taskw | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.854 |
| ours/sqlite/p1 | 0 | det-override | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.892 |
| ours/sqlite/p1 | 0 | det-pri | 1.000 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| ours/sqlite/p1 | 0 | det-taskw | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.854 |
| ours/sqlite/p1 | 1 | det-override | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.892 |
| ours/sqlite/p1 | 1 | det-pri | 1.000 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| ours/sqlite/p1 | 1 | det-taskw | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.854 |
| ours/sqlite/p1 | 2 | det-override | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.892 |
| ours/sqlite/p1 | 2 | det-pri | 1.000 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| ours/sqlite/p1 | 2 | det-taskw | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.854 |
| upstream/mem/p1 | 0 | det-override | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.892 |
| upstream/mem/p1 | 0 | det-pri | 1.000 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| upstream/mem/p1 | 0 | det-taskw | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.854 |
| upstream/sqlite/p4 | 0 | det-override | 0.005 | 0.771 | 0.375 | 0.000 | 0.001 | 0.808 |
| upstream/sqlite/p4 | 0 | det-pri | 0.010 | 0.913 | 0.000 | 0.012 | 0.000 | 0.000 |
| upstream/sqlite/p4 | 0 | det-taskw | 0.004 | 0.871 | 0.529 | 0.017 | 0.001 | 0.628 |
| upstream/sqlite/p4 | 1 | det-override | 0.003 | 0.750 | 0.405 | 0.000 | 0.000 | 0.792 |
| upstream/sqlite/p4 | 1 | det-pri | 0.015 | 0.939 | 0.000 | 0.007 | 0.000 | 0.000 |
| upstream/sqlite/p4 | 1 | det-taskw | 0.004 | 0.849 | 0.570 | 0.027 | 0.000 | 0.599 |
| upstream/sqlite/p4 | 2 | det-override | 0.004 | 0.775 | 0.341 | 0.000 | 0.001 | 0.817 |
| upstream/sqlite/p4 | 2 | det-pri | 0.005 | 0.864 | 0.000 | 0.032 | 0.000 | 0.000 |
| upstream/sqlite/p4 | 2 | det-taskw | 0.004 | 0.875 | 0.511 | 0.017 | 0.000 | 0.633 |

Mean over queues and runs (excluding the reference itself):

| impl/store/partitions | exact | tau | L1 | queue-runs |
|---|---|---|---|---|
| ours/mem/p1 | 1.000 | 1.000 | 0.000 | 9 |
| ours/sqlite/p1 | 1.000 | 1.000 | 0.000 | 9 |
| upstream/mem/p1 | 1.000 | 1.000 | 0.000 | 3 |
| upstream/sqlite/p1 | 1.000 | 1.000 | 0.000 | 6 |
| upstream/sqlite/p4 | 0.006 | 0.845 | 0.303 | 9 |

### many-keys (reference: upstream/sqlite/p1 run 0)

| impl/store/partitions | run | added | add err | delivered | lost | dup | sync % | lat p50 ms | lat p99 ms | backlog p50/p99 ms | sync p50/p99 ms | complete | elapsed s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| upstream/sqlite/p1 | 0 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1119.1 | 1563.3 | 1119.1/1563.3 | -/- | true | 1.7 |
| upstream/sqlite/p1 | 1 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1117.3 | 1553.5 | 1117.3/1553.5 | -/- | true | 1.7 |
| upstream/sqlite/p1 | 2 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1096.4 | 1538.3 | 1096.4/1538.3 | -/- | true | 1.7 |
| ours/mem/p1 | 0 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 928.0 | 1254.6 | 928.0/1254.6 | -/- | true | 1.3 |
| ours/mem/p1 | 1 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 925.0 | 1240.3 | 925.0/1240.3 | -/- | true | 1.3 |
| ours/mem/p1 | 2 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 915.4 | 1226.6 | 915.4/1226.6 | -/- | true | 1.3 |
| ours/sqlite/p1 | 0 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1254.0 | 1846.3 | 1254.0/1846.3 | -/- | true | 2.1 |
| ours/sqlite/p1 | 1 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1248.8 | 1799.2 | 1248.8/1799.2 | -/- | true | 2.0 |
| ours/sqlite/p1 | 2 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1252.9 | 1803.6 | 1252.9/1803.6 | -/- | true | 2.1 |
| upstream/mem/p1 | 0 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 993.0 | 1348.2 | 993.0/1348.2 | -/- | true | 1.5 |
| upstream/sqlite/p4 | 0 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1133.9 | 1597.9 | 1133.9/1597.9 | -/- | true | 3.7 |
| upstream/sqlite/p4 | 1 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1108.4 | 1562.1 | 1108.4/1562.1 | -/- | true | 3.7 |
| upstream/sqlite/p4 | 2 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1129.4 | 1597.5 | 1129.4/1597.5 | -/- | true | 3.7 |

Order vs reference, per queue (exact = positional match rate; tau = Kendall rank correlation; L1 = mean per-key share distance over 100-delivery windows, 0 identical, 2 disjoint; wrong = priority wrongorderness):

| impl/store/partitions | run | queue | exact | tau | L1 | wrong | FIFO exact | FIFO L1 |
|---|---|---|---|---|---|---|---|---|
| upstream/sqlite/p1 | 0 | mk | 1.000 | 1.000 | 0.000 | 0.000 | 0.000 | 1.382 |
| upstream/sqlite/p1 | 1 | mk | 0.393 | 0.946 | 0.342 | 0.000 | 0.001 | 1.373 |
| upstream/sqlite/p1 | 2 | mk | 0.421 | 0.947 | 0.370 | 0.000 | 0.001 | 1.378 |
| ours/mem/p1 | 0 | mk | 0.398 | 0.946 | 0.352 | 0.000 | 0.000 | 1.364 |
| ours/mem/p1 | 1 | mk | 0.374 | 0.946 | 0.359 | 0.000 | 0.000 | 1.377 |
| ours/mem/p1 | 2 | mk | 0.407 | 0.946 | 0.385 | 0.000 | 0.000 | 1.386 |
| ours/sqlite/p1 | 0 | mk | 0.398 | 0.947 | 0.372 | 0.000 | 0.000 | 1.373 |
| ours/sqlite/p1 | 1 | mk | 0.395 | 0.940 | 0.380 | 0.000 | 0.000 | 1.369 |
| ours/sqlite/p1 | 2 | mk | 0.376 | 0.949 | 0.368 | 0.000 | 0.000 | 1.379 |
| upstream/mem/p1 | 0 | mk | 0.398 | 0.943 | 0.377 | 0.000 | 0.000 | 1.372 |
| upstream/sqlite/p4 | 0 | mk | 0.001 | 0.831 | 0.803 | 0.000 | 0.000 | 1.340 |
| upstream/sqlite/p4 | 1 | mk | 0.001 | 0.828 | 0.797 | 0.000 | 0.000 | 1.333 |
| upstream/sqlite/p4 | 2 | mk | 0.001 | 0.828 | 0.809 | 0.000 | 0.000 | 1.337 |

Mean over queues and runs (excluding the reference itself):

| impl/store/partitions | exact | tau | L1 | queue-runs |
|---|---|---|---|---|
| ours/mem/p1 | 0.393 | 0.946 | 0.365 | 3 |
| ours/sqlite/p1 | 0.389 | 0.946 | 0.373 | 3 |
| upstream/mem/p1 | 0.398 | 0.943 | 0.377 | 1 |
| upstream/sqlite/p1 | 0.407 | 0.947 | 0.356 | 2 |
| upstream/sqlite/p4 | 0.001 | 0.829 | 0.803 | 3 |

### ratelimit (reference: upstream/sqlite/p1 run 0)

| impl/store/partitions | run | added | add err | delivered | lost | dup | sync % | lat p50 ms | lat p99 ms | backlog p50/p99 ms | sync p50/p99 ms | complete | elapsed s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| upstream/sqlite/p1 | 0 | 6000 | 0 | 6000 | 0 | 0 | 2.5 | 4548.2 | 11004.2 | 4731.3/11007.3 | 0.1/0.2 | true | 14.1 |
| upstream/sqlite/p1 | 1 | 6000 | 0 | 6000 | 0 | 0 | 3.2 | 4609.4 | 11017.3 | 4807.6/11018.3 | 0.1/0.1 | true | 14.2 |
| upstream/sqlite/p1 | 2 | 6000 | 0 | 6000 | 0 | 0 | 3.6 | 4549.7 | 11008.6 | 4791.6/11009.7 | 0.1/0.1 | true | 14.1 |
| ours/mem/p1 | 0 | 6000 | 0 | 6000 | 0 | 0 | 3.1 | 4274.1 | 10699.8 | 4450.1/10700.5 | 0.0/0.0 | true | 12.7 |
| ours/mem/p1 | 1 | 6000 | 0 | 6000 | 0 | 0 | 3.5 | 4258.0 | 10717.7 | 4455.6/10718.4 | 0.0/0.0 | true | 12.7 |
| ours/mem/p1 | 2 | 6000 | 0 | 6000 | 0 | 0 | 3.2 | 4255.1 | 10720.8 | 4434.0/10721.5 | 0.0/0.0 | true | 12.7 |
| ours/sqlite/p1 | 0 | 6000 | 0 | 6000 | 0 | 0 | 2.5 | 4345.8 | 10962.3 | 4497.3/10967.7 | 0.0/0.0 | true | 13.2 |
| ours/sqlite/p1 | 1 | 6000 | 0 | 6000 | 0 | 0 | 2.5 | 4335.7 | 10891.9 | 4486.0/10893.7 | 0.0/0.1 | true | 13.1 |
| ours/sqlite/p1 | 2 | 6000 | 0 | 6000 | 0 | 0 | 3.9 | 4327.4 | 10878.1 | 4532.6/10879.8 | 0.0/0.0 | true | 13.2 |
| upstream/mem/p1 | 0 | 6000 | 0 | 6000 | 0 | 18 | 4.2 | 4564.6 | 10930.4 | 4816.2/10932.1 | 0.1/0.1 | true | 14.0 |
| upstream/sqlite/p4 | 0 | 6000 | 0 | 6000 | 0 | 0 | 0.8 | 5068.4 | 11106.9 | 5108.9/11109.4 | 0.1/2.3 | true | 14.9 |
| upstream/sqlite/p4 | 1 | 6000 | 0 | 6000 | 0 | 0 | 0.7 | 5054.2 | 11870.7 | 5106.9/11872.5 | 0.1/1.8 | true | 18.9 |
| upstream/sqlite/p4 | 2 | 6000 | 0 | 6000 | 0 | 0 | 0.5 | 5064.8 | 11725.1 | 5084.3/11739.7 | 0.1/1.9 | true | 19.9 |

Order vs reference, per queue (exact = positional match rate; tau = Kendall rank correlation; L1 = mean per-key share distance over 100-delivery windows, 0 identical, 2 disjoint; wrong = priority wrongorderness):

| impl/store/partitions | run | queue | exact | tau | L1 | wrong | FIFO exact | FIFO L1 |
|---|---|---|---|---|---|---|---|---|
| upstream/sqlite/p1 | 0 | rl-both | 1.000 | 1.000 | 0.000 | 0.000 | 0.011 | 0.278 |
| upstream/sqlite/p1 | 0 | rl-fair | 1.000 | 1.000 | 0.000 | 0.000 | 0.052 | 1.217 |
| upstream/sqlite/p1 | 0 | rl-key10 | 1.000 | 1.000 | 0.000 | 0.000 | 0.016 | 0.206 |
| upstream/sqlite/p1 | 0 | rl-queue50 | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.206 |
| upstream/sqlite/p1 | 1 | rl-both | 0.931 | 1.000 | 0.004 | 0.000 | 0.012 | 0.278 |
| upstream/sqlite/p1 | 1 | rl-fair | 0.128 | 0.968 | 0.211 | 0.000 | 0.052 | 1.206 |
| upstream/sqlite/p1 | 1 | rl-key10 | 0.203 | 0.992 | 0.044 | 0.000 | 0.025 | 0.210 |
| upstream/sqlite/p1 | 1 | rl-queue50 | 0.896 | 1.000 | 0.000 | 0.000 | 0.002 | 0.206 |
| upstream/sqlite/p1 | 2 | rl-both | 0.926 | 1.000 | 0.004 | 0.000 | 0.013 | 0.280 |
| upstream/sqlite/p1 | 2 | rl-fair | 0.120 | 0.951 | 0.227 | 0.000 | 0.051 | 1.191 |
| upstream/sqlite/p1 | 2 | rl-key10 | 0.126 | 0.988 | 0.046 | 0.000 | 0.018 | 0.214 |
| upstream/sqlite/p1 | 2 | rl-queue50 | 0.898 | 1.000 | 0.004 | 0.000 | 0.001 | 0.206 |
| ours/mem/p1 | 0 | rl-both | 0.940 | 1.000 | 0.004 | 0.000 | 0.011 | 0.282 |
| ours/mem/p1 | 0 | rl-fair | 0.119 | 0.972 | 0.189 | 0.000 | 0.051 | 1.203 |
| ours/mem/p1 | 0 | rl-key10 | 0.147 | 0.991 | 0.046 | 0.000 | 0.030 | 0.220 |
| ours/mem/p1 | 0 | rl-queue50 | 0.866 | 0.999 | 0.008 | 0.000 | 0.001 | 0.206 |
| ours/mem/p1 | 1 | rl-both | 0.946 | 1.000 | 0.004 | 0.000 | 0.013 | 0.280 |
| ours/mem/p1 | 1 | rl-fair | 0.128 | 0.961 | 0.228 | 0.000 | 0.053 | 1.197 |
| ours/mem/p1 | 1 | rl-key10 | 0.109 | 0.990 | 0.048 | 0.000 | 0.026 | 0.218 |
| ours/mem/p1 | 1 | rl-queue50 | 0.876 | 1.000 | 0.004 | 0.000 | 0.001 | 0.206 |
| ours/mem/p1 | 2 | rl-both | 0.908 | 0.999 | 0.000 | 0.000 | 0.011 | 0.278 |
| ours/mem/p1 | 2 | rl-fair | 0.151 | 0.975 | 0.177 | 0.000 | 0.051 | 1.209 |
| ours/mem/p1 | 2 | rl-key10 | 0.135 | 0.989 | 0.048 | 0.000 | 0.027 | 0.220 |
| ours/mem/p1 | 2 | rl-queue50 | 0.877 | 1.000 | 0.004 | 0.000 | 0.001 | 0.206 |
| ours/sqlite/p1 | 0 | rl-both | 0.939 | 1.000 | 0.004 | 0.000 | 0.012 | 0.280 |
| ours/sqlite/p1 | 0 | rl-fair | 0.477 | 0.996 | 0.049 | 0.000 | 0.052 | 1.220 |
| ours/sqlite/p1 | 0 | rl-key10 | 0.140 | 0.991 | 0.048 | 0.000 | 0.025 | 0.226 |
| ours/sqlite/p1 | 0 | rl-queue50 | 0.893 | 1.000 | 0.008 | 0.000 | 0.001 | 0.206 |
| ours/sqlite/p1 | 1 | rl-both | 0.941 | 1.000 | 0.004 | 0.000 | 0.012 | 0.280 |
| ours/sqlite/p1 | 1 | rl-fair | 0.539 | 0.997 | 0.035 | 0.000 | 0.051 | 1.218 |
| ours/sqlite/p1 | 1 | rl-key10 | 0.148 | 0.991 | 0.048 | 0.000 | 0.023 | 0.222 |
| ours/sqlite/p1 | 1 | rl-queue50 | 0.904 | 1.000 | 0.004 | 0.000 | 0.001 | 0.206 |
| ours/sqlite/p1 | 2 | rl-both | 0.942 | 1.000 | 0.004 | 0.000 | 0.013 | 0.280 |
| ours/sqlite/p1 | 2 | rl-fair | 0.134 | 0.947 | 0.265 | 0.000 | 0.051 | 1.186 |
| ours/sqlite/p1 | 2 | rl-key10 | 0.157 | 0.991 | 0.042 | 0.000 | 0.022 | 0.218 |
| ours/sqlite/p1 | 2 | rl-queue50 | 0.899 | 1.000 | 0.008 | 0.000 | 0.001 | 0.206 |
| upstream/mem/p1 | 0 | rl-both | 0.924 | 1.000 | 0.004 | 0.000 | 0.015 | 0.280 |
| upstream/mem/p1 | 0 | rl-fair | 0.120 | 0.940 | 0.236 | 0.000 | 0.053 | 1.189 |
| upstream/mem/p1 | 0 | rl-key10 | 0.175 | 0.990 | 0.032 | 0.000 | 0.024 | 0.208 |
| upstream/mem/p1 | 0 | rl-queue50 | 0.895 | 1.000 | 0.004 | 0.000 | 0.001 | 0.206 |
| upstream/sqlite/p4 | 0 | rl-both | 0.003 | 0.658 | 0.156 | 0.000 | 0.003 | 0.282 |
| upstream/sqlite/p4 | 0 | rl-fair | 0.010 | 0.812 | 0.589 | 0.000 | 0.010 | 1.168 |
| upstream/sqlite/p4 | 0 | rl-key10 | 0.000 | 0.763 | 0.098 | 0.000 | 0.007 | 0.208 |
| upstream/sqlite/p4 | 0 | rl-queue50 | 0.011 | 0.899 | 0.080 | 0.009 | 0.001 | 0.180 |
| upstream/sqlite/p4 | 1 | rl-both | 0.005 | 0.713 | 0.106 | 0.000 | 0.002 | 0.254 |
| upstream/sqlite/p4 | 1 | rl-fair | 0.012 | 0.807 | 0.593 | 0.000 | 0.012 | 1.163 |
| upstream/sqlite/p4 | 1 | rl-key10 | 0.003 | 0.762 | 0.100 | 0.000 | 0.007 | 0.198 |
| upstream/sqlite/p4 | 1 | rl-queue50 | 0.011 | 0.898 | 0.136 | 0.009 | 0.003 | 0.168 |
| upstream/sqlite/p4 | 2 | rl-both | 0.009 | 0.706 | 0.160 | 0.000 | 0.003 | 0.256 |
| upstream/sqlite/p4 | 2 | rl-fair | 0.008 | 0.849 | 0.522 | 0.000 | 0.007 | 1.183 |
| upstream/sqlite/p4 | 2 | rl-key10 | 0.006 | 0.758 | 0.104 | 0.000 | 0.004 | 0.198 |
| upstream/sqlite/p4 | 2 | rl-queue50 | 0.016 | 0.927 | 0.082 | 0.004 | 0.002 | 0.194 |

Mean over queues and runs (excluding the reference itself):

| impl/store/partitions | exact | tau | L1 | queue-runs |
|---|---|---|---|---|
| ours/mem/p1 | 0.517 | 0.990 | 0.063 | 12 |
| ours/sqlite/p1 | 0.593 | 0.992 | 0.043 | 12 |
| upstream/mem/p1 | 0.529 | 0.982 | 0.069 | 4 |
| upstream/sqlite/p1 | 0.529 | 0.987 | 0.067 | 8 |
| upstream/sqlite/p4 | 0.008 | 0.796 | 0.227 | 12 |

Throughput in the limited window [limited_start, limited_end] (deliveries/s; configured limit in brackets):

| impl/store/partitions | run | rl-queue50 [50] | rl-key10 [8 keys x 10 = 80] max-key | rl-both [min(100, 10x20)] max-key | rl-fair [100] |
|---|---|---|---|---|---|
| upstream/sqlite/p1 | 0 | 55.1 | 88.5 (11.1) | 100.0 (11.3) | 110.0 |
| upstream/sqlite/p1 | 1 | 54.9 | 87.8 (11.1) | 100.0 (11.3) | 109.9 |
| upstream/sqlite/p1 | 2 | 55.0 | 87.7 (11.0) | 100.0 (11.3) | 110.0 |
| ours/mem/p1 | 0 | 55.0 | 88.0 (11.0) | 100.0 (11.3) | 110.0 |
| ours/mem/p1 | 1 | 55.1 | 88.1 (11.1) | 100.0 (11.3) | 110.0 |
| ours/mem/p1 | 2 | 55.0 | 88.8 (11.1) | 100.0 (11.3) | 110.1 |
| ours/sqlite/p1 | 0 | 55.1 | 88.0 (11.0) | 100.0 (11.3) | 110.0 |
| ours/sqlite/p1 | 1 | 55.0 | 88.0 (11.0) | 100.0 (11.3) | 110.0 |
| ours/sqlite/p1 | 2 | 55.0 | 88.8 (11.1) | 100.0 (11.3) | 110.0 |
| upstream/mem/p1 | 0 | 55.0 | 87.7 (11.0) | 100.0 (11.3) | 110.0 |
| upstream/sqlite/p4 | 0 | 48.5 | 77.8 (9.9) | 94.1 (10.7) | 92.7 |
| upstream/sqlite/p4 | 1 | 51.0 | 74.3 (9.7) | 91.4 (9.8) | 93.9 |
| upstream/sqlite/p4 | 2 | 48.7 | 75.2 (9.5) | 93.4 (10.5) | 90.4 |
