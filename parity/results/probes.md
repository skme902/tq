## Ablations (ours/mem, each row one feature removed; compared with the full upstream det-fair run)

### no-overrides
| ours/mem/p1 | 0 | det-override | 0.065 | 0.755 | 0.612 | 0.000 | 0.003 | 0.864 |
| ours/mem/p1 | 0 | det-pri | 1.000 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| ours/mem/p1 | 0 | det-taskw | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.854 |

### no-priorities
| ours/mem/p1 | 0 | det-override | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.892 |
| ours/mem/p1 | 0 | det-pri | 0.000 | 0.278 | 0.000 | 0.497 | 1.000 | 0.000 |
| ours/mem/p1 | 0 | det-taskw | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.854 |

### no-taskweights
| ours/mem/p1 | 0 | det-override | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.892 |
| ours/mem/p1 | 0 | det-pri | 1.000 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| ours/mem/p1 | 0 | det-taskw | 0.009 | 0.918 | 0.701 | 0.000 | 0.000 | 0.870 |

## Held leases (ours only: each lease acked after -hold-ms; upstream acks at start)
Order must still match upstream det-fair (leases do not hold back reads), with 0 lost and 0 dup.

### det-fair, hold 2000 ms
| ours/mem/p1 | 0 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 928.2 | 1469.6 | 928.2/1469.6 | -/- | true | 1.5 |
| ours/sqlite/p1 | 0 | 4500 | 0 | 4500 | 0 | 0 | 0.0 | 1121.1 | 2023.6 | 1121.1/2023.6 | -/- | true | 2.1 |
| ours/mem/p1 | 0 | det-override | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.892 |
| ours/mem/p1 | 0 | det-pri | 1.000 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| ours/mem/p1 | 0 | det-taskw | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.854 |
| ours/sqlite/p1 | 0 | det-override | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.892 |
| ours/sqlite/p1 | 0 | det-pri | 1.000 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| ours/sqlite/p1 | 0 | det-taskw | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.854 |
| ours/mem/p1 | 1.000 | 1.000 | 0.000 | 3 |
| ours/sqlite/p1 | 1.000 | 1.000 | 0.000 | 3 |

### churn, hold 500 ms (delivery counts only; churn order is not deterministic)
| ours/mem/p1 | 0 | 5686 | 0 | 5686 | 0 | 0 | 84.9 | 0.0 | 1245.1 | 382.5/1618.7 | 0.0/0.1 | true | 15.0 |
| ours/sqlite/p1 | 0 | 5686 | 0 | 5686 | 0 | 0 | 84.9 | 0.0 | 1244.6 | 383.1/1618.7 | 0.0/0.1 | true | 15.0 |

## System-limit probes (4000 backlog, 4 pollers)

| trace | delivered | complete | drain s | in first 1s | rate after 1s |
|---|---|---|---|---|---|
| probe-never.ours-mem | 4000 | True | 0.38 | 4000 | nan |
| probe-never.ours-sqlite | 4000 | True | 0.49 | 4000 | nan |
| probe-never.up | 4000 | True | 0.43 | 4000 | nan |
| probe-set5000unset.ours-mem | 4000 | True | 0.35 | 4000 | nan |
| probe-set5000unset.ours-sqlite | 4000 | True | 0.39 | 4000 | nan |
| probe-set5000unset.up | 4000 | True | 0.44 | 4000 | nan |
| probe-set50unset.ours-mem | 4000 | True | 3.00 | 2000 | 1001 |
| probe-set50unset.ours-sqlite | 4000 | True | 3.00 | 1999 | 1001 |
| probe-set50unset.up | 4000 | True | 3.00 | 1998 | 1001 |

## Config-change lag (4 pollers at 50 rps; limit unset or raised at +2 s; lag = first 10 deliveries within 50 ms)

| trace | lag ms |
|---|---|
| lag-raise5000.ours-mem | 21 |
| lag-raise5000.ours-sqlite | 19 |
| lag-raise5000.up | 80 |
| lag-unset.ours-mem | 20 |
| lag-unset.ours-sqlite | 20 |
| lag-unset.up | 62 |
