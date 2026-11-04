
### churn (reference: upstream/sqlite/p1 run 0)

| impl/store/partitions | run | added | add err | delivered | lost | dup | sync % | lat p50 ms | lat p99 ms | backlog p50/p99 ms | sync p50/p99 ms | complete | elapsed s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| upstream/sqlite/p1 | 0 | 5686 | 0 | 5686 | 0 | 0 | 84.8 | 0.1 | 1244.5 | 378.1/1619.6 | 0.1/0.2 | true | 15.0 |
| upstream/sqlite/p1 | 1 | 5686 | 0 | 5686 | 0 | 0 | 84.8 | 0.1 | 1245.3 | 377.6/1619.9 | 0.1/0.2 | true | 15.0 |
| upstream/sqlite/p1 | 2 | 5686 | 0 | 5686 | 0 | 0 | 86.8 | 0.1 | 1390.3 | 127.1/10557.6 | 0.1/17.0 | true | 20.6 |
| ours/mem/p1 | 0 | 5686 | 0 | 5686 | 0 | 0 | 84.9 | 0.0 | 1244.7 | 383.4/1618.3 | 0.0/0.1 | true | 15.0 |
| ours/mem/p1 | 1 | 5686 | 0 | 5686 | 0 | 0 | 84.9 | 0.0 | 1244.9 | 383.0/1618.9 | 0.0/0.1 | true | 15.0 |
| ours/mem/p1 | 2 | 5686 | 0 | 5686 | 0 | 0 | 83.4 | 0.0 | 1340.2 | 343.7/1734.7 | 0.0/17.7 | true | 15.0 |
| ours/sqlite/p1 | 0 | 5686 | 0 | 5686 | 0 | 0 | 84.9 | 0.0 | 1244.8 | 380.5/1618.4 | 0.0/0.1 | true | 15.0 |
| ours/sqlite/p1 | 1 | 5686 | 0 | 5686 | 0 | 0 | 84.9 | 0.0 | 1243.2 | 383.6/1619.4 | 0.0/0.0 | true | 15.0 |
| ours/sqlite/p1 | 2 | 5686 | 0 | 5686 | 0 | 0 | 84.7 | 0.0 | 1244.3 | 375.6/1618.4 | 0.0/0.3 | true | 15.0 |
| upstream/mem/p1 | 0 | 5686 | 0 | 5686 | 0 | 434 | 84.8 | 0.1 | 1245.0 | 378.7/1619.4 | 0.1/0.2 | true | 15.0 |
| upstream/sqlite/p4 | 0 | 5686 | 0 | 5686 | 0 | 0 | 37.5 | 40.2 | 1326.2 | 140.1/1463.5 | 0.1/0.3 | true | 15.0 |
| upstream/sqlite/p4 | 1 | 5686 | 0 | 5686 | 0 | 0 | 38.2 | 38.6 | 1410.9 | 138.8/1545.5 | 0.1/0.3 | true | 16.0 |
| upstream/sqlite/p4 | 2 | 5686 | 0 | 5686 | 0 | 0 | 37.2 | 40.7 | 1326.9 | 131.4/1478.2 | 0.1/0.4 | true | 15.0 |

Order vs reference, per queue (exact = positional match rate; tau = Kendall rank correlation; L1 = mean per-key share distance over 100-delivery windows, 0 identical, 2 disjoint; wrong = priority wrongorderness):

| impl/store/partitions | run | queue | exact | tau | L1 | wrong | FIFO exact | FIFO L1 |
|---|---|---|---|---|---|---|---|---|
(30 queues; per-queue rows omitted)

Mean over queues and runs (excluding the reference itself):

| impl/store/partitions | exact | tau | L1 | queue-runs |
|---|---|---|---|---|
| ours/mem/p1 | 0.983 | 0.999 | 0.000 | 84 |
| ours/sqlite/p1 | 0.994 | 1.000 | 0.000 | 84 |
| upstream/mem/p1 | 0.992 | 1.000 | 0.000 | 28 |
| upstream/sqlite/p1 | 0.934 | 0.995 | 0.004 | 56 |
| upstream/sqlite/p4 | 0.340 | 0.973 | 0.015 | 84 |

### det-fair (reference: upstream/sqlite/p1 run 0)

| impl/store/partitions | run | added | add err | delivered | lost | dup | sync % | lat p50 ms | lat p99 ms | backlog p50/p99 ms | sync p50/p99 ms | complete | elapsed s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| upstream/sqlite/p1 | 0 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 1012.7 | 1613.9 | 1012.7/1613.9 | -/- | true | 1.7 |
| upstream/sqlite/p1 | 1 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 1004.6 | 1665.8 | 1004.6/1665.8 | -/- | true | 1.7 |
| upstream/sqlite/p1 | 2 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 916.1 | 1559.3 | 916.1/1559.3 | -/- | true | 1.6 |
| ours/mem/p1 | 0 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 844.6 | 1278.7 | 844.6/1278.7 | -/- | true | 1.3 |
| ours/mem/p1 | 1 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 920.9 | 1231.7 | 920.9/1231.7 | -/- | true | 1.4 |
| ours/mem/p1 | 2 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 843.7 | 1282.8 | 843.7/1282.8 | -/- | true | 1.3 |
| ours/sqlite/p1 | 0 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 1022.3 | 1853.7 | 1022.3/1853.7 | -/- | true | 1.9 |
| ours/sqlite/p1 | 1 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 1034.1 | 1852.9 | 1034.1/1852.9 | -/- | true | 1.9 |
| ours/sqlite/p1 | 2 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 964.9 | 1796.5 | 964.9/1796.5 | -/- | true | 1.9 |
| upstream/mem/p1 | 0 | 4500 | 0 | 4500 | 0 | 43 | 0.0 | 881.2 | 1444.7 | 881.2/1444.7 | -/- | true | 1.5 |
| upstream/sqlite/p4 | 0 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 942.9 | 1613.1 | 942.9/1613.1 | -/- | true | 12.6 |
| upstream/sqlite/p4 | 1 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 965.6 | 6632.7 | 965.6/6632.7 | -/- | true | 18.7 |
| upstream/sqlite/p4 | 2 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 932.7 | 5507.2 | 932.7/5507.2 | -/- | true | 18.6 |

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
| upstream/sqlite/p4 | 0 | det-override | 0.005 | 0.790 | 0.380 | 0.000 | 0.001 | 0.831 |
| upstream/sqlite/p4 | 0 | det-pri | 0.004 | 0.857 | 0.000 | 0.024 | 0.000 | 0.000 |
| upstream/sqlite/p4 | 0 | det-taskw | 0.001 | 0.849 | 0.596 | 0.024 | 0.001 | 0.629 |
| upstream/sqlite/p4 | 1 | det-override | 0.003 | 0.749 | 0.400 | 0.000 | 0.001 | 0.796 |
| upstream/sqlite/p4 | 1 | det-pri | 0.009 | 0.876 | 0.000 | 0.022 | 0.001 | 0.000 |
| upstream/sqlite/p4 | 1 | det-taskw | 0.006 | 0.890 | 0.516 | 0.010 | 0.001 | 0.649 |
| upstream/sqlite/p4 | 2 | det-override | 0.002 | 0.725 | 0.492 | 0.000 | 0.001 | 0.759 |
| upstream/sqlite/p4 | 2 | det-pri | 0.007 | 0.881 | 0.000 | 0.022 | 0.005 | 0.000 |
| upstream/sqlite/p4 | 2 | det-taskw | 0.008 | 0.880 | 0.509 | 0.014 | 0.001 | 0.637 |

Mean over queues and runs (excluding the reference itself):

| impl/store/partitions | exact | tau | L1 | queue-runs |
|---|---|---|---|---|
| ours/mem/p1 | 1.000 | 1.000 | 0.000 | 9 |
| ours/sqlite/p1 | 1.000 | 1.000 | 0.000 | 9 |
| upstream/mem/p1 | 1.000 | 1.000 | 0.000 | 3 |
| upstream/sqlite/p1 | 1.000 | 1.000 | 0.000 | 6 |
| upstream/sqlite/p4 | 0.005 | 0.833 | 0.321 | 9 |

### many-keys (reference: upstream/sqlite/p1 run 0)

| impl/store/partitions | run | added | add err | delivered | lost | dup | sync % | lat p50 ms | lat p99 ms | backlog p50/p99 ms | sync p50/p99 ms | complete | elapsed s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| upstream/sqlite/p1 | 0 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1089.5 | 1516.6 | 1089.5/1516.6 | -/- | true | 1.6 |
| upstream/sqlite/p1 | 1 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1094.8 | 1527.8 | 1094.8/1527.8 | -/- | true | 1.7 |
| upstream/sqlite/p1 | 2 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1088.2 | 1512.3 | 1088.2/1512.3 | -/- | true | 1.6 |
| ours/mem/p1 | 0 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1024.5 | 1345.5 | 1024.5/1345.5 | -/- | true | 1.4 |
| ours/mem/p1 | 1 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 978.2 | 1334.5 | 978.2/1334.5 | -/- | true | 1.4 |
| ours/mem/p1 | 2 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 904.5 | 1206.4 | 904.5/1206.4 | -/- | true | 1.3 |
| ours/sqlite/p1 | 0 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1662.3 | 2746.1 | 1662.3/2746.1 | -/- | true | 3.1 |
| ours/sqlite/p1 | 1 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1436.7 | 2199.8 | 1436.7/2199.8 | -/- | true | 2.5 |
| ours/sqlite/p1 | 2 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1354.9 | 2036.5 | 1354.9/2036.5 | -/- | true | 2.4 |
| upstream/mem/p1 | 0 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1219.5 | 1655.8 | 1219.5/1655.8 | -/- | true | 1.8 |
| upstream/sqlite/p4 | 0 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1127.0 | 1592.6 | 1127.0/1592.6 | -/- | true | 3.7 |
| upstream/sqlite/p4 | 1 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1105.6 | 1548.0 | 1105.6/1548.0 | -/- | true | 3.7 |
| upstream/sqlite/p4 | 2 | 6000 | 0 | 6000 | 0 | 0 | 0.0 | 1263.6 | 1817.3 | 1263.6/1817.3 | -/- | true | 4.0 |

Order vs reference, per queue (exact = positional match rate; tau = Kendall rank correlation; L1 = mean per-key share distance over 100-delivery windows, 0 identical, 2 disjoint; wrong = priority wrongorderness):

| impl/store/partitions | run | queue | exact | tau | L1 | wrong | FIFO exact | FIFO L1 |
|---|---|---|---|---|---|---|---|---|
| upstream/sqlite/p1 | 0 | mk | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 1.372 |
| upstream/sqlite/p1 | 1 | mk | 0.384 | 0.949 | 0.393 | 0.000 | 0.001 | 1.375 |
| upstream/sqlite/p1 | 2 | mk | 0.385 | 0.945 | 0.359 | 0.000 | 0.000 | 1.378 |
| ours/mem/p1 | 0 | mk | 0.388 | 0.948 | 0.342 | 0.000 | 0.000 | 1.381 |
| ours/mem/p1 | 1 | mk | 0.376 | 0.946 | 0.388 | 0.000 | 0.000 | 1.379 |
| ours/mem/p1 | 2 | mk | 0.378 | 0.944 | 0.384 | 0.000 | 0.000 | 1.374 |
| ours/sqlite/p1 | 0 | mk | 0.381 | 0.949 | 0.339 | 0.000 | 0.000 | 1.376 |
| ours/sqlite/p1 | 1 | mk | 0.393 | 0.939 | 0.446 | 0.000 | 0.001 | 1.363 |
| ours/sqlite/p1 | 2 | mk | 0.368 | 0.941 | 0.402 | 0.000 | 0.001 | 1.369 |
| upstream/mem/p1 | 0 | mk | 0.379 | 0.944 | 0.407 | 0.000 | 0.000 | 1.365 |
| upstream/sqlite/p4 | 0 | mk | 0.001 | 0.833 | 0.800 | 0.000 | 0.000 | 1.341 |
| upstream/sqlite/p4 | 1 | mk | 0.001 | 0.833 | 0.771 | 0.000 | 0.000 | 1.325 |
| upstream/sqlite/p4 | 2 | mk | 0.001 | 0.831 | 0.780 | 0.000 | 0.000 | 1.325 |

Mean over queues and runs (excluding the reference itself):

| impl/store/partitions | exact | tau | L1 | queue-runs |
|---|---|---|---|---|
| ours/mem/p1 | 0.380 | 0.946 | 0.371 | 3 |
| ours/sqlite/p1 | 0.381 | 0.943 | 0.396 | 3 |
| upstream/mem/p1 | 0.379 | 0.944 | 0.407 | 1 |
| upstream/sqlite/p1 | 0.384 | 0.947 | 0.376 | 2 |
| upstream/sqlite/p4 | 0.001 | 0.832 | 0.783 | 3 |

### ratelimit (reference: upstream/sqlite/p1 run 0)

| impl/store/partitions | run | added | add err | delivered | lost | dup | sync % | lat p50 ms | lat p99 ms | backlog p50/p99 ms | sync p50/p99 ms | complete | elapsed s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| upstream/sqlite/p1 | 0 | 6000 | 0 | 6000 | 0 | 0 | 3.1 | 4615.2 | 11039.5 | 4830.9/11041.9 | 0.1/2.0 | true | 14.2 |
| upstream/sqlite/p1 | 1 | 6000 | 0 | 6000 | 0 | 0 | 3.7 | 4584.4 | 10997.5 | 4828.9/10998.4 | 0.1/0.2 | true | 14.1 |
| upstream/sqlite/p1 | 2 | 6000 | 0 | 6000 | 0 | 0 | 3.0 | 4605.7 | 10997.8 | 4775.0/10998.7 | 0.1/0.3 | true | 14.1 |
| ours/mem/p1 | 0 | 6000 | 0 | 6000 | 0 | 0 | 3.1 | 4263.6 | 10719.8 | 4441.4/10721.5 | 0.0/0.0 | true | 12.7 |
| ours/mem/p1 | 1 | 6000 | 0 | 6000 | 0 | 0 | 3.4 | 4245.2 | 10717.7 | 4445.3/10720.4 | 0.0/0.0 | true | 12.7 |
| ours/mem/p1 | 2 | 6000 | 0 | 6000 | 0 | 0 | 3.1 | 4268.2 | 10698.2 | 4441.1/10699.2 | 0.0/0.0 | true | 12.7 |
| ours/sqlite/p1 | 0 | 6000 | 0 | 6000 | 0 | 0 | 4.7 | 4383.0 | 11003.1 | 4591.9/11004.9 | 0.0/0.0 | true | 13.3 |
| ours/sqlite/p1 | 1 | 6000 | 0 | 6000 | 0 | 0 | 2.5 | 4328.6 | 10902.0 | 4487.0/10903.8 | 0.0/0.1 | true | 13.3 |
| ours/sqlite/p1 | 2 | 6000 | 0 | 6000 | 0 | 0 | 2.5 | 4331.4 | 10899.1 | 4485.0/10899.9 | 0.0/0.0 | true | 13.1 |
| upstream/mem/p1 | 0 | 6000 | 0 | 6000 | 0 | 16 | 3.2 | 4573.4 | 10927.1 | 4782.3/10928.4 | 0.1/1.0 | true | 14.0 |
| upstream/sqlite/p4 | 0 | 6000 | 0 | 6000 | 0 | 0 | 0.7 | 5085.6 | 11244.7 | 5127.9/11249.0 | 0.1/1.8 | true | 17.2 |
| upstream/sqlite/p4 | 1 | 6000 | 0 | 6000 | 0 | 0 | 0.7 | 5080.5 | 10961.7 | 5124.6/10963.5 | 0.1/2.0 | true | 15.0 |
| upstream/sqlite/p4 | 2 | 6000 | 0 | 6000 | 0 | 0 | 0.5 | 5023.7 | 11800.9 | 5050.8/11806.7 | 0.1/2.3 | true | 16.9 |

Order vs reference, per queue (exact = positional match rate; tau = Kendall rank correlation; L1 = mean per-key share distance over 100-delivery windows, 0 identical, 2 disjoint; wrong = priority wrongorderness):

| impl/store/partitions | run | queue | exact | tau | L1 | wrong | FIFO exact | FIFO L1 |
|---|---|---|---|---|---|---|---|---|
| upstream/sqlite/p1 | 0 | rl-both | 1.000 | 1.000 | 0.000 | 0.000 | 0.009 | 0.280 |
| upstream/sqlite/p1 | 0 | rl-fair | 1.000 | 1.000 | 0.000 | 0.000 | 0.052 | 1.208 |
| upstream/sqlite/p1 | 0 | rl-key10 | 1.000 | 1.000 | 0.000 | 0.000 | 0.023 | 0.208 |
| upstream/sqlite/p1 | 0 | rl-queue50 | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.206 |
| upstream/sqlite/p1 | 1 | rl-both | 0.916 | 1.000 | 0.004 | 0.000 | 0.015 | 0.280 |
| upstream/sqlite/p1 | 1 | rl-fair | 0.133 | 0.945 | 0.259 | 0.000 | 0.051 | 1.202 |
| upstream/sqlite/p1 | 1 | rl-key10 | 0.153 | 0.992 | 0.038 | 0.000 | 0.025 | 0.206 |
| upstream/sqlite/p1 | 1 | rl-queue50 | 0.886 | 1.000 | 0.004 | 0.000 | 0.003 | 0.206 |
| upstream/sqlite/p1 | 2 | rl-both | 0.913 | 1.000 | 0.004 | 0.000 | 0.010 | 0.280 |
| upstream/sqlite/p1 | 2 | rl-fair | 0.121 | 0.959 | 0.185 | 0.000 | 0.051 | 1.213 |
| upstream/sqlite/p1 | 2 | rl-key10 | 0.152 | 0.988 | 0.050 | 0.000 | 0.014 | 0.210 |
| upstream/sqlite/p1 | 2 | rl-queue50 | 0.885 | 1.000 | 0.004 | 0.000 | 0.001 | 0.206 |
| ours/mem/p1 | 0 | rl-both | 0.908 | 0.999 | 0.004 | 0.000 | 0.012 | 0.280 |
| ours/mem/p1 | 0 | rl-fair | 0.148 | 0.958 | 0.153 | 0.000 | 0.052 | 1.205 |
| ours/mem/p1 | 0 | rl-key10 | 0.118 | 0.991 | 0.054 | 0.000 | 0.025 | 0.218 |
| ours/mem/p1 | 0 | rl-queue50 | 0.881 | 1.000 | 0.004 | 0.000 | 0.001 | 0.206 |
| ours/mem/p1 | 1 | rl-both | 0.917 | 0.999 | 0.004 | 0.000 | 0.011 | 0.280 |
| ours/mem/p1 | 1 | rl-fair | 0.139 | 0.950 | 0.165 | 0.000 | 0.052 | 1.200 |
| ours/mem/p1 | 1 | rl-key10 | 0.112 | 0.991 | 0.038 | 0.000 | 0.021 | 0.218 |
| ours/mem/p1 | 1 | rl-queue50 | 0.831 | 0.999 | 0.004 | 0.000 | 0.001 | 0.206 |
| ours/mem/p1 | 2 | rl-both | 0.919 | 0.999 | 0.004 | 0.000 | 0.011 | 0.280 |
| ours/mem/p1 | 2 | rl-fair | 0.133 | 0.957 | 0.194 | 0.000 | 0.051 | 1.211 |
| ours/mem/p1 | 2 | rl-key10 | 0.132 | 0.991 | 0.050 | 0.000 | 0.012 | 0.224 |
| ours/mem/p1 | 2 | rl-queue50 | 0.848 | 0.999 | 0.008 | 0.000 | 0.001 | 0.202 |
| ours/sqlite/p1 | 0 | rl-both | 0.916 | 0.999 | 0.004 | 0.000 | 0.011 | 0.278 |
| ours/sqlite/p1 | 0 | rl-fair | 0.115 | 0.896 | 0.369 | 0.000 | 0.053 | 1.184 |
| ours/sqlite/p1 | 0 | rl-key10 | 0.102 | 0.990 | 0.048 | 0.000 | 0.025 | 0.224 |
| ours/sqlite/p1 | 0 | rl-queue50 | 0.871 | 1.000 | 0.004 | 0.000 | 0.001 | 0.206 |
| ours/sqlite/p1 | 1 | rl-both | 0.920 | 0.999 | 0.004 | 0.000 | 0.009 | 0.282 |
| ours/sqlite/p1 | 1 | rl-fair | 0.122 | 0.973 | 0.227 | 0.000 | 0.050 | 1.203 |
| ours/sqlite/p1 | 1 | rl-key10 | 0.078 | 0.991 | 0.052 | 0.000 | 0.018 | 0.220 |
| ours/sqlite/p1 | 1 | rl-queue50 | 0.864 | 1.000 | 0.004 | 0.000 | 0.001 | 0.206 |
| ours/sqlite/p1 | 2 | rl-both | 0.917 | 1.000 | 0.004 | 0.000 | 0.012 | 0.280 |
| ours/sqlite/p1 | 2 | rl-fair | 0.122 | 0.973 | 0.224 | 0.000 | 0.051 | 1.221 |
| ours/sqlite/p1 | 2 | rl-key10 | 0.138 | 0.991 | 0.050 | 0.000 | 0.018 | 0.226 |
| ours/sqlite/p1 | 2 | rl-queue50 | 0.879 | 1.000 | 0.004 | 0.000 | 0.001 | 0.206 |
| upstream/mem/p1 | 0 | rl-both | 0.922 | 0.999 | 0.004 | 0.000 | 0.012 | 0.280 |
| upstream/mem/p1 | 0 | rl-fair | 0.137 | 0.952 | 0.170 | 0.000 | 0.051 | 1.205 |
| upstream/mem/p1 | 0 | rl-key10 | 0.153 | 0.989 | 0.052 | 0.000 | 0.027 | 0.210 |
| upstream/mem/p1 | 0 | rl-queue50 | 0.880 | 1.000 | 0.004 | 0.000 | 0.001 | 0.206 |
| upstream/sqlite/p4 | 0 | rl-both | 0.008 | 0.638 | 0.150 | 0.000 | 0.003 | 0.282 |
| upstream/sqlite/p4 | 0 | rl-fair | 0.013 | 0.797 | 0.625 | 0.000 | 0.012 | 1.158 |
| upstream/sqlite/p4 | 0 | rl-key10 | 0.007 | 0.751 | 0.098 | 0.000 | 0.005 | 0.230 |
| upstream/sqlite/p4 | 0 | rl-queue50 | 0.011 | 0.902 | 0.100 | 0.009 | 0.002 | 0.172 |
| upstream/sqlite/p4 | 1 | rl-both | 0.006 | 0.678 | 0.124 | 0.000 | 0.007 | 0.254 |
| upstream/sqlite/p4 | 1 | rl-fair | 0.017 | 0.845 | 0.475 | 0.000 | 0.012 | 1.214 |
| upstream/sqlite/p4 | 1 | rl-key10 | 0.004 | 0.765 | 0.092 | 0.000 | 0.003 | 0.242 |
| upstream/sqlite/p4 | 1 | rl-queue50 | 0.013 | 0.901 | 0.086 | 0.008 | 0.003 | 0.182 |
| upstream/sqlite/p4 | 2 | rl-both | 0.006 | 0.652 | 0.202 | 0.000 | 0.004 | 0.286 |
| upstream/sqlite/p4 | 2 | rl-fair | 0.008 | 0.816 | 0.617 | 0.000 | 0.008 | 1.180 |
| upstream/sqlite/p4 | 2 | rl-key10 | 0.004 | 0.728 | 0.122 | 0.000 | 0.003 | 0.248 |
| upstream/sqlite/p4 | 2 | rl-queue50 | 0.009 | 0.889 | 0.130 | 0.010 | 0.001 | 0.172 |

Mean over queues and runs (excluding the reference itself):

| impl/store/partitions | exact | tau | L1 | queue-runs |
|---|---|---|---|---|
| ours/mem/p1 | 0.507 | 0.986 | 0.057 | 12 |
| ours/sqlite/p1 | 0.504 | 0.984 | 0.083 | 12 |
| upstream/mem/p1 | 0.523 | 0.985 | 0.057 | 4 |
| upstream/sqlite/p1 | 0.520 | 0.985 | 0.068 | 8 |
| upstream/sqlite/p4 | 0.009 | 0.780 | 0.235 | 12 |

Throughput in the limited window [limited_start, limited_end] (deliveries/s; configured limit in brackets):

| impl/store/partitions | run | rl-queue50 [50] | rl-key10 [8 keys x 10 = 80] max-key | rl-both [min(100, 10x20)] max-key | rl-fair [100] |
|---|---|---|---|---|---|
| upstream/sqlite/p1 | 0 | 55.0 | 88.4 (11.1) | 100.0 (11.3) | 110.0 |
| upstream/sqlite/p1 | 1 | 55.0 | 88.4 (11.1) | 100.0 (11.3) | 110.0 |
| upstream/sqlite/p1 | 2 | 55.0 | 87.7 (11.0) | 100.0 (11.3) | 110.0 |
| ours/mem/p1 | 0 | 55.1 | 88.0 (11.0) | 100.0 (11.3) | 110.0 |
| ours/mem/p1 | 1 | 55.1 | 88.8 (11.1) | 100.0 (11.3) | 110.1 |
| ours/mem/p1 | 2 | 55.0 | 88.0 (11.0) | 100.0 (11.3) | 110.0 |
| ours/sqlite/p1 | 0 | 55.1 | 88.0 (11.0) | 100.0 (11.3) | 110.0 |
| ours/sqlite/p1 | 1 | 55.0 | 88.0 (11.0) | 100.0 (11.3) | 110.0 |
| ours/sqlite/p1 | 2 | 55.1 | 88.5 (11.1) | 100.0 (11.3) | 110.0 |
| upstream/mem/p1 | 0 | 55.1 | 87.7 (11.0) | 100.0 (11.3) | 110.0 |
| upstream/sqlite/p4 | 0 | 52.4 | 76.9 (9.8) | 91.0 (10.1) | 89.3 |
| upstream/sqlite/p4 | 1 | 49.7 | 74.2 (9.5) | 89.2 (9.6) | 91.8 |
| upstream/sqlite/p4 | 2 | 49.6 | 74.0 (9.4) | 86.2 (10.1) | 93.9 |
