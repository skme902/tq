#!/usr/bin/env python3
"""raw BASE NEW: compare the deterministic cells of every '| ours/' row of probes.md with the committed BASE.
pinned PARITYDIR PINNEDREF BASE: rescore this run's ablation and held-lease det-fair traces against the pinned
upstream reference with cmd/compare and compare those rows with BASE. Timing cells are masked."""
import sys,subprocess,glob,os
def rows(text):
    return [[c.strip() for c in l.strip().strip('|').split('|')] for l in text.splitlines() if l.startswith('| ours/')]
def mask(r):  # delivery rows: name|run|added|..|..|..|..|timings...|complete|time -> keep counts and complete
    return r[:7]+[r[12]] if len(r)==14 else r
def cmp(kind,base,new):
    b=[mask(r) for r in base]; n=[mask(r) for r in new]
    mm=[(i,x,y) for i,(x,y) in enumerate(zip(b,n)) if x!=y]
    k=len(mm)+abs(len(b)-len(n))
    print(f"{'probe ours' if kind=='raw' else 'pinned-ref'} rows {len(b)} {len(n)} mismatches {k}")
    for i,x,y in mm[:12]: print('  row',i,'base',' | '.join(x),'\n         new ',' | '.join(y))
    if k: sys.exit(1)
if sys.argv[1]=='raw':
    cmp('raw',rows(open(sys.argv[2]).read()),rows(open(sys.argv[3]).read()))
else:
    p,ref,base=sys.argv[2],sys.argv[3],sys.argv[4]
    if not os.path.exists(ref): print('pinned-ref MISSING',ref); sys.exit(1)
    def run(*traces): return subprocess.run(['go','run','./cmd/compare','-scripts','scripts',ref,*traces],cwd=p,capture_output=True,text=True,check=True).stdout
    out=''
    for s in sorted(glob.glob(p+'/scripts-ablate/*.json')):
        n=os.path.basename(s)[:-5]; out+='\n'.join(l for l in run(f'traces/probe/ablate-{n}.json').splitlines() if l.startswith('| ours/mem/p1 | 0 | det-'))+'\n'
    out+='\n'.join(l for l in run(*sorted(glob.glob(p+'/traces/probe/hold-det-fair.ours-*.json'))).splitlines() if l.startswith('| ours/'))+'\n'
    b=rows(open(base).read()); nb=len(rows(out)); cmp('pinned',b[:nb],rows(out))
