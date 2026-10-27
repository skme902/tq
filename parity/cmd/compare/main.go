// Command compare reads parity traces and prints per-run metrics and pairwise comparisons
// against a reference trace (the first upstream run of each script) as markdown.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/skme902/tq/parity/driver"
)

func load(p string) *driver.Trace {
	b, err := os.ReadFile(p)
	if err != nil {
		log.Fatal(err)
	}
	var t driver.Trace
	if err := json.Unmarshal(b, &t); err != nil {
		log.Fatal(err)
	}
	return &t
}

func label(t *driver.Trace) string {
	return fmt.Sprintf("%s/%s/p%d", t.Impl, t.Store, t.Partitions)
}

type stats struct {
	added, addErr, delivered, unique, lost, dup, sync int
	p50, p99, p50Sync, p99Sync, p50Back, p99Back      float64 // ms
}

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	sort.Float64s(xs)
	i := int(math.Ceil(p*float64(len(xs)))) - 1
	return xs[max(0, min(i, len(xs)-1))]
}

func basic(t *driver.Trace) stats {
	var s stats
	addAt := map[int64]driver.AddRec{}
	for _, a := range t.Adds {
		if a.Err != "" {
			s.addErr++
			continue
		}
		s.added++
		addAt[a.ID] = a
		if a.Sync {
			s.sync++
		}
	}
	seen := map[int64]bool{}
	var all, sy, bk []float64
	for _, d := range t.Deliveries {
		s.delivered++
		if seen[d.ID] {
			s.dup++
			continue
		}
		seen[d.ID] = true
		if a, ok := addAt[d.ID]; ok {
			l := float64(d.AtNS-a.AtNS) / 1e6
			all = append(all, l)
			if a.Sync {
				sy = append(sy, l)
			} else {
				bk = append(bk, l)
			}
		}
	}
	s.unique = len(seen)
	for id := range addAt {
		if !seen[id] {
			s.lost++
		}
	}
	s.p50, s.p99 = pct(all, .5), pct(all, .99)
	s.p50Sync, s.p99Sync = pct(sy, .5), pct(sy, .99)
	s.p50Back, s.p99Back = pct(bk, .5), pct(bk, .99)
	return s
}

func seqByQueue(t *driver.Trace) map[string][]int64 {
	m := map[string][]int64{}
	seen := map[int64]bool{}
	for _, d := range t.Deliveries {
		if !seen[d.ID] {
			seen[d.ID] = true
			m[d.Queue] = append(m[d.Queue], d.ID)
		}
	}
	return m
}

// exact returns the fraction of positions where a and b deliver the same task.
func exact(a, b []int64) float64 {
	n := max(len(a), len(b))
	if n == 0 {
		return math.NaN()
	}
	m := 0
	for i := range min(len(a), len(b)) {
		if a[i] == b[i] {
			m++
		}
	}
	return float64(m) / float64(n)
}

// kendall returns Kendall's tau between the delivery ranks of the tasks common to a and b.
func kendall(a, b []int64) float64 {
	rb := map[int64]int{}
	for i, id := range b {
		rb[id] = i
	}
	var xs []int
	for _, id := range a {
		if r, ok := rb[id]; ok {
			xs = append(xs, r)
		}
	}
	n := len(xs)
	if n < 2 {
		return math.NaN()
	}
	// count inversions with merge sort: O(n log n)
	inv := mergeCount(xs)
	pairs := float64(n) * float64(n-1) / 2
	return 1 - 2*float64(inv)/pairs
}

func mergeCount(xs []int) int64 {
	if len(xs) < 2 {
		return 0
	}
	mid := len(xs) / 2
	l := append([]int(nil), xs[:mid]...)
	r := append([]int(nil), xs[mid:]...)
	inv := mergeCount(l) + mergeCount(r)
	i, j, k := 0, 0, 0
	for i < len(l) && j < len(r) {
		if l[i] <= r[j] {
			xs[k] = l[i]
			i++
		} else {
			xs[k] = r[j]
			j++
			inv += int64(len(l) - i)
		}
		k++
	}
	for ; i < len(l); i++ {
		xs[k] = l[i]
		k++
	}
	for ; j < len(r); j++ {
		xs[k] = r[j]
		k++
	}
	return inv
}

// windowL1 splits both delivery sequences of a queue into windows of w deliveries and
// returns the mean L1 distance between per-fairness-key share vectors (0 = identical
// shares, 2 = disjoint).
func windowL1(a, b []int64, keyOf map[int64]string, w int) float64 {
	n := min(len(a), len(b)) / w
	if n == 0 {
		return math.NaN()
	}
	tot := 0.0
	for i := range n {
		ca, cb := map[string]float64{}, map[string]float64{}
		for _, id := range a[i*w : (i+1)*w] {
			ca[keyOf[id]]++
		}
		for _, id := range b[i*w : (i+1)*w] {
			cb[keyOf[id]]++
		}
		d := 0.0
		for k, v := range ca {
			d += math.Abs(v - cb[k])
		}
		for k, v := range cb {
			if _, ok := ca[k]; !ok {
				d += v
			}
		}
		tot += d / float64(w)
	}
	return tot / float64(n)
}

// wrongorder is upstream tests/priority_fairness_test.go's "wrongorderness": the fraction
// of adjacent-or-not pairs delivered with a lower priority before a higher one, normalized
// to [0,1] (0 = perfect priority order).
func wrongorder(seq []int64, priOf map[int64]int32) float64 {
	var ps []int
	for _, id := range seq {
		p := priOf[id]
		if p == 0 {
			p = 3
		}
		ps = append(ps, int(p))
	}
	n := len(ps)
	if n < 2 {
		return math.NaN()
	}
	// inversions where a larger pri value (lower priority) precedes a smaller one
	inv := mergeCount(append([]int(nil), ps...))
	// normalize by the maximum possible inversions given the multiset
	cnt := map[int]int{}
	for _, p := range ps {
		cnt[p]++
	}
	maxInv := float64(n) * float64(n-1) / 2
	for _, c := range cnt {
		maxInv -= float64(c) * float64(c-1) / 2
	}
	if maxInv == 0 {
		return 0
	}
	return float64(inv) / maxInv
}

type meta struct {
	keyOf map[int64]string
	priOf map[int64]int32
	order map[string][]int64 // add order per queue (FIFO baseline)
}

func scriptMeta(p string) (*driver.Script, *meta) {
	s, err := driver.LoadScript(p)
	if err != nil {
		log.Fatal(err)
	}
	m := &meta{keyOf: map[int64]string{}, priOf: map[int64]int32{}, order: map[string][]int64{}}
	for _, e := range s.Events {
		if e.Op == "add" {
			m.keyOf[e.ID] = e.Key
			m.priOf[e.ID] = e.Pri
			m.order[e.Queue] = append(m.order[e.Queue], e.ID)
		}
	}
	return s, m
}

func f(v float64) string {
	if math.IsNaN(v) {
		return "-"
	}
	return fmt.Sprintf("%.3f", v)
}

func ms(v float64) string {
	if math.IsNaN(v) {
		return "-"
	}
	return fmt.Sprintf("%.1f", v)
}

// rateIn returns deliveries per second for queue q (and per key) within [from,to].
func rateIn(t *driver.Trace, q string, from, to int64, keyOf map[int64]string) (float64, map[string]float64) {
	n := 0
	per := map[string]float64{}
	for _, d := range t.Deliveries {
		if d.Queue == q && d.AtNS >= from && d.AtNS < to {
			n++
			per[keyOf[d.ID]]++
		}
	}
	sec := float64(to-from) / 1e9
	for k := range per {
		per[k] /= sec
	}
	return float64(n) / sec, per
}

func main() {
	scripts := flag.String("scripts", "scripts", "script dir")
	window := flag.Int("window", 100, "share window size (deliveries)")
	flag.Parse()
	byScript := map[string][]*driver.Trace{}
	for _, p := range flag.Args() {
		t := load(p)
		byScript[t.Script] = append(byScript[t.Script], t)
	}
	names := make([]string, 0, len(byScript))
	for n := range byScript {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		trs := byScript[name]
		sort.SliceStable(trs, func(i, j int) bool {
			if label(trs[i]) != label(trs[j]) {
				// upstream/sqlite/p1 first (reference)
				ri, rj := label(trs[i]) == "upstream/sqlite/p1", label(trs[j]) == "upstream/sqlite/p1"
				if ri != rj {
					return ri
				}
				return label(trs[i]) < label(trs[j])
			}
			return trs[i].Run < trs[j].Run
		})
		s, m := scriptMeta(filepath.Join(*scripts, name+".json"))
		ref := trs[0]
		refSeq := seqByQueue(ref)
		fmt.Printf("\n### %s (reference: %s run %d)\n\n", name, label(ref), ref.Run)
		fmt.Println("| impl/store/partitions | run | added | add err | delivered | lost | dup | sync % | lat p50 ms | lat p99 ms | backlog p50/p99 ms | sync p50/p99 ms | complete | elapsed s |")
		fmt.Println("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
		for _, t := range trs {
			b := basic(t)
			fmt.Printf("| %s | %d | %d | %d | %d | %d | %d | %.1f | %s | %s | %s/%s | %s/%s | %v | %.1f |\n",
				label(t), t.Run, b.added, b.addErr, b.unique, b.lost, b.dup, 100*float64(b.sync)/float64(max(1, b.added)),
				ms(b.p50), ms(b.p99), ms(b.p50Back), ms(b.p99Back), ms(b.p50Sync), ms(b.p99Sync), t.Complete, float64(t.ElapsedMS)/1000)
		}
		queues := append([]string(nil), s.Queues...)
		sort.Strings(queues)
		fmt.Printf("\nOrder vs reference, per queue (exact = positional match rate; tau = Kendall rank correlation; L1 = mean per-key share distance over %d-delivery windows, 0 identical, 2 disjoint; wrong = priority wrongorderness):\n\n", *window)
		fmt.Println("| impl/store/partitions | run | queue | exact | tau | L1 | wrong | FIFO exact | FIFO L1 |")
		fmt.Println("|---|---|---|---|---|---|---|---|---|")
		agg := map[string][4]float64{} // label -> sums of exact, tau, L1, count
		for _, t := range trs {
			seq := seqByQueue(t)
			for _, q := range queues {
				a, r := seq[q], refSeq[q]
				ex, tau, l1 := exact(a, r), kendall(a, r), windowL1(a, r, m.keyOf, *window)
				if len(queues) <= 6 {
					fmt.Printf("| %s | %d | %s | %s | %s | %s | %s | %s | %s |\n", label(t), t.Run, q, f(ex), f(tau), f(l1),
						f(wrongorder(a, m.priOf)), f(exact(a, m.order[q])), f(windowL1(a, m.order[q], m.keyOf, *window)))
				}
				if t == ref {
					continue
				}
				x := agg[label(t)]
				if !math.IsNaN(l1) {
					x[0] += ex
					x[1] += tau
					x[2] += l1
					x[3]++
				}
				agg[label(t)] = x
			}
		}
		if len(queues) > 6 {
			fmt.Printf("(%d queues; per-queue rows omitted)\n", len(queues))
		}
		fmt.Print("\nMean over queues and runs (excluding the reference itself):\n\n")
		fmt.Println("| impl/store/partitions | exact | tau | L1 | queue-runs |")
		fmt.Println("|---|---|---|---|---|")
		ls := make([]string, 0, len(agg))
		for l := range agg {
			ls = append(ls, l)
		}
		sort.Strings(ls)
		for _, l := range ls {
			x := agg[l]
			if x[3] == 0 {
				continue
			}
			fmt.Printf("| %s | %s | %s | %s | %d |\n", l, f(x[0]/x[3]), f(x[1]/x[3]), f(x[2]/x[3]), int(x[3]))
		}
		if strings.HasPrefix(name, "ratelimit") {
			fmt.Print("\nThroughput in the limited window [limited_start, limited_end] (deliveries/s; configured limit in brackets):\n\n")
			fmt.Println("| impl/store/partitions | run | rl-queue50 [50] | rl-key10 [8 keys x 10 = 80] max-key | rl-both [min(100, 10x20)] max-key | rl-fair [100] |")
			fmt.Println("|---|---|---|---|---|---|")
			for _, t := range trs {
				from, to := t.Marks["limited_start"], t.Marks["limited_end"]
				r1, _ := rateIn(t, "rl-queue50", from, to, m.keyOf)
				r2, p2 := rateIn(t, "rl-key10", from, to, m.keyOf)
				r3, p3 := rateIn(t, "rl-both", from, to, m.keyOf)
				r4, _ := rateIn(t, "rl-fair", from, to, m.keyOf)
				fmt.Printf("| %s | %d | %.1f | %.1f (%.1f) | %.1f (%.1f) | %.1f |\n", label(t), t.Run, r1, r2, maxv(p2), r3, maxv(p3), r4)
			}
		}
	}
}

func maxv(m map[string]float64) float64 {
	x := 0.0
	for _, v := range m {
		x = max(x, v)
	}
	return x
}
