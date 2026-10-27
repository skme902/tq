## Ablations (ours/mem, each row one feature removed; compared with the full upstream det-fair run)

### no-overrides
| ours/mem/p1 | 0 | det-override | 0.065 | 0.755 | 0.612 | 0.000 | 0.003 | 0.864 |
| ours/mem/p1 | 0 | det-pri | 0.998 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| ours/mem/p1 | 0 | det-taskw | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.854 |

### no-priorities
| ours/mem/p1 | 0 | det-override | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.892 |
| ours/mem/p1 | 0 | det-pri | 0.000 | 0.278 | 0.000 | 0.497 | 1.000 | 0.000 |
| ours/mem/p1 | 0 | det-taskw | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.854 |

### no-taskweights
| ours/mem/p1 | 0 | det-override | 1.000 | 1.000 | 0.000 | 0.000 | 0.001 | 0.892 |
| ours/mem/p1 | 0 | det-pri | 0.998 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| ours/mem/p1 | 0 | det-taskw | 0.009 | 0.918 | 0.701 | 0.000 | 0.000 | 0.870 |

## System-limit probes (4000 backlog, 4 pollers)

| trace | delivered | complete | drain s | in first 1s | rate after 1s |
|---|---|---|---|---|---|
| probe-never.ours-mem | 4000 | True | 0.32 | 4000 | nan |
| probe-never.ours-sqlite | 4000 | True | 0.36 | 4000 | nan |
| probe-never.up | 4000 | True | 0.40 | 4000 | nan |
| probe-set5000unset.ours-mem | 4000 | True | 0.32 | 4000 | nan |
| probe-set5000unset.ours-sqlite | 4000 | True | 0.37 | 4000 | nan |
| probe-set5000unset.up | 4000 | True | 0.41 | 4000 | nan |
| probe-set50unset.ours-mem | 4000 | True | 3.00 | 2000 | 1000 |
| probe-set50unset.ours-sqlite | 4000 | True | 3.00 | 2000 | 1000 |
| probe-set50unset.up | 4000 | True | 3.00 | 2000 | 1000 |

## Config-change lag (4 pollers at 50 rps; limit unset or raised at +2 s; lag = first 10 deliveries within 50 ms)

| trace | lag ms |
|---|---|
| lag-raise5000.ours-mem | 20 |
| lag-raise5000.ours-sqlite | 0 |
| lag-raise5000.up | 62 |
| lag-unset.ours-mem | 0 |
| lag-unset.ours-sqlite | 0 |
| lag-unset.up | 63 |
