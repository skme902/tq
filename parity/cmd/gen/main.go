// Command gen writes the seeded parity workload scripts to a directory.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"

	"github.com/skme902/tq/parity/driver"
)

type gen struct {
	s   *driver.Script
	rng *rand.Rand
	id  int64
}

func newGen(name string, seed uint64) *gen {
	return &gen{
		s:   &driver.Script{Name: name, Seed: int64(seed), Namespace: "parity-ns", LongPollMS: 1000, DrainTimeoutMS: 120000},
		rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
	}
}

func (g *gen) ev(e driver.Event) { g.s.Events = append(g.s.Events, e) }

func (g *gen) queue(q string) {
	for _, x := range g.s.Queues {
		if x == q {
			return
		}
	}
	g.s.Queues = append(g.s.Queues, q)
}

func (g *gen) add(at int64, q string, pri int32, key string, w float32) {
	g.queue(q)
	g.id++
	g.ev(driver.Event{AtMS: at, Op: "add", Queue: q, ID: g.id, Pri: pri, Key: key, Weight: w})
}

// zipf picks key index in [0,n) with P(i) ∝ 1/(i+1)^s.
type zipf struct{ cdf []float64 }

func newZipf(n int, s float64) *zipf {
	z := &zipf{cdf: make([]float64, n)}
	sum := 0.0
	for i := range n {
		sum += 1 / math.Pow(float64(i+1), s)
		z.cdf[i] = sum
	}
	for i := range z.cdf {
		z.cdf[i] /= sum
	}
	return z
}

func (z *zipf) pick(r *rand.Rand) int { return sort.SearchFloat64s(z.cdf, r.Float64()) }

func f32(v float32) *float32 { return &v }

var weightChoices = []float32{0, 1, 2, 5} // 0 = unset (default 1.0)

// randPri returns 0 (default) 30% of the time, else 1..5.
func randPri(r *rand.Rand) int32 {
	if r.IntN(10) < 3 {
		return 0
	}
	return int32(1 + r.IntN(5))
}

// detFair: backlog-only, single poller per queue, <=100 keys: dispatch order is deterministic.
func detFair(seed uint64) *driver.Script {
	g := newGen("det-fair", seed)
	g.s.Deterministic = true
	// queue 1: task-carried weights, 40 skewed keys, mixed priorities
	z := newZipf(40, 1.1)
	kw := make([]float32, 40)
	for i := range kw {
		kw[i] = weightChoices[g.rng.IntN(len(weightChoices))]
	}
	for range 2000 {
		k := z.pick(g.rng)
		g.add(0, "det-taskw", randPri(g.rng), fmt.Sprintf("k%02d", k), kw[k])
	}
	// queue 2: weights via config overrides only, default priority, 30 keys
	ov := map[string]float32{}
	for i := range 10 {
		ov[fmt.Sprintf("o%02d", i)] = float32(1 + i%4)
	}
	g.queue("det-override")
	g.ev(driver.Event{Op: "weights", Queue: "det-override", Weights: ov})
	g.ev(driver.Event{Op: "barrier", SettleMS: 300})
	z2 := newZipf(30, 0.8)
	for range 1500 {
		g.add(0, "det-override", 0, fmt.Sprintf("o%02d", z2.pick(g.rng)), 0)
	}
	// queue 3: priorities only (no fairness key)
	for range 1000 {
		g.add(0, "det-pri", randPri(g.rng), "", 0)
	}
	g.ev(driver.Event{Op: "barrier", SettleMS: 500})
	g.ev(driver.Event{Op: "mark", Key: "poll_start"})
	for _, q := range g.s.Queues {
		g.ev(driver.Event{Op: "start", Queue: q, Poller: "p0"})
	}
	g.ev(driver.Event{Op: "await_all"})
	return g.s
}

// manyKeys: >100 keys (count-min sketch regime), backlog then one poller.
func manyKeys(seed uint64) *driver.Script {
	g := newGen("many-keys", seed)
	z := newZipf(300, 1.0)
	kw := make([]float32, 300)
	for i := range kw {
		kw[i] = weightChoices[g.rng.IntN(len(weightChoices))]
	}
	for range 6000 {
		k := z.pick(g.rng)
		g.add(0, "mk", 0, fmt.Sprintf("k%03d", k), kw[k])
	}
	g.ev(driver.Event{Op: "barrier", SettleMS: 500})
	g.ev(driver.Event{Op: "mark", Key: "poll_start"})
	g.ev(driver.Event{Op: "start", Queue: "mk", Poller: "p0"})
	g.ev(driver.Event{Op: "await_all"})
	return g.s
}

// rateLimit: queue and per-key limits with a pre-filled backlog and continuous arrivals.
func rateLimit(seed uint64) *driver.Script {
	g := newGen("ratelimit", seed)
	g.queue("rl-queue50")
	g.queue("rl-key10")
	g.queue("rl-both")
	g.queue("rl-fair")
	g.ev(driver.Event{Op: "queue_rps", Queue: "rl-queue50", RPS: f32(50)})
	g.ev(driver.Event{Op: "key_rps", Queue: "rl-key10", RPS: f32(10)})
	g.ev(driver.Event{Op: "queue_rps", Queue: "rl-both", RPS: f32(100)})
	g.ev(driver.Event{Op: "key_rps", Queue: "rl-both", RPS: f32(20)})
	g.ev(driver.Event{Op: "queue_rps", Queue: "rl-fair", RPS: f32(100)})
	g.ev(driver.Event{Op: "barrier", SettleMS: 500})
	for range 1000 {
		g.add(0, "rl-queue50", randPri(g.rng), fmt.Sprintf("k%d", g.rng.IntN(5)), 0)
	}
	for range 1000 {
		g.add(0, "rl-key10", 0, fmt.Sprintf("k%d", g.rng.IntN(8)), 0)
	}
	for range 1000 {
		g.add(0, "rl-both", 0, fmt.Sprintf("k%d", g.rng.IntN(10)), 0)
	}
	g.ev(driver.Event{Op: "barrier", SettleMS: 300})
	g.ev(driver.Event{Op: "mark", Key: "limited_start"})
	for _, q := range g.s.Queues {
		for p := range 4 {
			g.ev(driver.Event{Op: "start", Queue: q, Poller: fmt.Sprintf("p%d", p)})
		}
	}
	// rl-fair: 120 skewed keys arriving at 300/s against a 100/s limit for 10s.
	z := newZipf(120, 1.0)
	kw := make([]float32, 120)
	for i := range kw {
		kw[i] = weightChoices[g.rng.IntN(len(weightChoices))]
	}
	var adds []driver.Event
	for i := range 3000 {
		k := z.pick(g.rng)
		g.id++
		adds = append(adds, driver.Event{AtMS: int64(i) * 10 / 3, Op: "add", Queue: "rl-fair", ID: g.id, Key: fmt.Sprintf("f%03d", k), Weight: kw[k]})
	}
	g.s.Events = append(g.s.Events, adds...)
	g.ev(driver.Event{AtMS: 10000, Op: "mark", Key: "limited_end"})
	for _, q := range g.s.Queues {
		g.ev(driver.Event{AtMS: 10000, Op: "queue_rps", Queue: q, RPS: nil})
		g.ev(driver.Event{AtMS: 10000, Op: "key_rps", Queue: q, RPS: nil})
	}
	g.ev(driver.Event{AtMS: 10000, Op: "await_all"})
	return g.s
}

// churn: many queues created on the fly, Poisson arrivals, pollers joining/leaving,
// natural sync/backlog mix, priorities and skewed keys (>100 per queue for some).
func churn(seed uint64) *driver.Script {
	g := newGen("churn", seed)
	const nq, durMS = 30, 15000
	type qs struct {
		name  string
		birth int64
		z     *zipf
		kw    []float32
		rate  float64 // adds per second
	}
	var queues []qs
	for i := range nq {
		nk := 5 + g.rng.IntN(200)
		q := qs{name: fmt.Sprintf("c%02d", i), birth: int64(g.rng.IntN(durMS / 2)), z: newZipf(nk, 1.2), kw: make([]float32, nk), rate: 5 + g.rng.Float64()*25}
		for k := range q.kw {
			q.kw[k] = weightChoices[g.rng.IntN(len(weightChoices))]
		}
		queues = append(queues, q)
	}
	var evs []driver.Event
	for _, q := range queues {
		for t := float64(q.birth); t < durMS; t += g.rng.ExpFloat64() * 1000 / q.rate {
			k := q.z.pick(g.rng)
			g.id++
			evs = append(evs, driver.Event{AtMS: int64(t), Op: "add", Queue: q.name, ID: g.id, Pri: randPri(g.rng), Key: fmt.Sprintf("k%03d", k), Weight: q.kw[k]})
		}
		// pollers p0..p2 toggle on/off with exponential holding times (mean 1.5s)
		for p := range 3 {
			on := g.rng.IntN(2) == 0
			for t := float64(q.birth); t < durMS; t += g.rng.ExpFloat64() * 1500 {
				op := "stop"
				if on {
					op = "start"
				}
				evs = append(evs, driver.Event{AtMS: int64(t), Op: op, Queue: q.name, Poller: fmt.Sprintf("p%d", p)})
				on = !on
			}
		}
	}
	sort.SliceStable(evs, func(i, j int) bool { return evs[i].AtMS < evs[j].AtMS })
	for _, e := range evs {
		g.queue(e.Queue)
	}
	g.s.Events = append(g.s.Events, evs...)
	g.ev(driver.Event{AtMS: durMS, Op: "mark", Key: "load_end"})
	for _, q := range queues {
		g.ev(driver.Event{AtMS: durMS, Op: "start", Queue: q.name, Poller: "drain"})
	}
	g.ev(driver.Event{AtMS: durMS, Op: "await_all"})
	return g.s
}

func main() {
	out := flag.String("out", "scripts", "output dir")
	seed := flag.Uint64("seed", 20261027, "seed")
	flag.Parse()
	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}
	for _, s := range []*driver.Script{detFair(*seed), manyKeys(*seed + 1), rateLimit(*seed + 2), churn(*seed + 3)} {
		b, err := json.MarshalIndent(s, "", " ")
		if err != nil {
			log.Fatal(err)
		}
		p := filepath.Join(*out, s.Name+".json")
		if err := os.WriteFile(p, b, 0o644); err != nil {
			log.Fatal(err)
		}
		adds := 0
		for _, e := range s.Events {
			if e.Op == "add" {
				adds++
			}
		}
		fmt.Printf("%s: %d queues, %d events, %d adds\n", p, len(s.Queues), len(s.Events), adds)
	}
}
