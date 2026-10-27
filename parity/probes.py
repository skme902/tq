#!/usr/bin/env python3
"""Generates the vetting scripts for run_probes.sh.

scripts-ablate/: det-fair with one feature removed (weight overrides, per-task weights, priorities), to show
  the exact-order match on det-fair depends on each feature and is not vacuous.
scripts-probe/: single-queue system-rate-limit probes (4000 backlog, 4 pollers, queue loaded first):
  never set / set 5000 then unset / set 50 then unset.
"""
import json, copy, os
os.makedirs('scripts-ablate', exist_ok=True); os.makedirs('scripts-probe', exist_ok=True)
base = json.load(open('scripts/det-fair.json'))
def ablate(name, keep):
    s = copy.deepcopy(base); s['events'] = [e for e in (keep(e) for e in s['events']) if e is not None]
    json.dump(s, open(f'scripts-ablate/{name}.json', 'w'))
def no_overrides(e): return None if e.get('op') == 'weights' else e
def strip(field, q):
    def f(e):
        if e.get('op') == 'add' and e.get('q') == q: e.pop(field, None)
        return e
    return f
ablate('no-overrides', no_overrides); ablate('no-taskweights', strip('w', 'det-taskw')); ablate('no-priorities', strip('pri', 'det-pri'))

def probe(name, mode):
    ev = [{'at_ms': 0, 'op': 'add', 'q': 'q1', 'id': i, 'key': 'k%d' % (i % 8)} for i in range(1, 4001)]
    ev.append({'at_ms': 0, 'op': 'barrier', 'settle_ms': 300})
    if mode != 'never':
        ev += [{'at_ms': 0, 'op': 'queue_rps', 'q': 'q1', 'rps': 5000 if mode == 'set5000unset' else 50},
               {'at_ms': 0, 'op': 'barrier', 'settle_ms': 300}, {'at_ms': 0, 'op': 'queue_rps', 'q': 'q1'}]
    ev += [{'at_ms': 0, 'op': 'barrier', 'settle_ms': 1500}, {'at_ms': 0, 'op': 'mark', 'key': 'poll_start'}]
    ev += [{'at_ms': 0, 'op': 'start', 'q': 'q1', 'poller': 'p%d' % p} for p in range(4)]
    ev.append({'at_ms': 0, 'op': 'await_all'})
    json.dump({'name': name, 'seed': 1, 'namespace': 'parity-ns', 'queues': ['q1'], 'long_poll_ms': 1000,
               'deterministic': False, 'drain_timeout_ms': 60000, 'events': ev}, open(f'scripts-probe/{name}.json', 'w'))
for m in ['never', 'set5000unset', 'set50unset']: probe('probe-' + m, m)

# Config-change lag: pollers active at 50 rps; at 2 s the limit is unset or raised to 5000.
def lag(name, final_rps):
    ev = [{'at_ms': 0, 'op': 'add', 'q': 'q1', 'id': i, 'key': 'k%d' % (i % 8)} for i in range(1, 3001)]
    ev += [{'at_ms': 0, 'op': 'queue_rps', 'q': 'q1', 'rps': 50}, {'at_ms': 0, 'op': 'barrier', 'settle_ms': 800},
           {'at_ms': 0, 'op': 'mark', 'key': 'poll_start'}]
    ev += [{'at_ms': 0, 'op': 'start', 'q': 'q1', 'poller': 'p%d' % p} for p in range(4)]
    e = {'at_ms': 2000, 'op': 'queue_rps', 'q': 'q1'}
    if final_rps: e['rps'] = final_rps
    ev += [{'at_ms': 2000, 'op': 'mark', 'key': 'change'}, e, {'at_ms': 2000, 'op': 'await_all'}]
    json.dump({'name': name, 'seed': 1, 'namespace': 'parity-ns', 'queues': ['q1'], 'long_poll_ms': 1000,
               'deterministic': False, 'drain_timeout_ms': 60000, 'events': ev}, open(f'scripts-probe/{name}.json', 'w'))
lag('lag-unset', None); lag('lag-raise5000', 5000)
