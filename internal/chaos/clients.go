package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Behavior is how a simulated client misbehaves.
type Behavior string

// Client behaviors. All of them reuse their idempotency key on retries, as a
// correct client must; the system must turn every retry into the same workflow.
const (
	Polite      Behavior = "polite"      // one request, retried until it succeeds
	Impatient   Behavior = "impatient"   // gives up after 50–300ms and retries
	DoubleClick Behavior = "doubleclick" // two concurrent submissions
	Reformatted Behavior = "reformatted" // retries with reordered, re-indented JSON
	Malformed   Behavior = "malformed"   // invalid body: expects 4xx, never retries
)

// ClientConfig configures RunClients.
type ClientConfig struct {
	// BaseURLs are the API replicas. Every request (first try, retry, double
	// click) goes to the next one in turn, so one key's requests usually
	// reach several replicas.
	BaseURLs    []string
	Token       string
	Workflows   int
	Concurrency int // used when Spread is 0
	Seed        int64
	// Spread, if set, makes arrivals open-loop: workflows arrive at seeded
	// random times over this window, whether or not earlier ones finished, so
	// work is in flight for the whole campaign.
	Spread time.Duration
	// Outstanding, if set, holds the number of workflows a client is still
	// working on (submitting or retrying) at any moment.
	Outstanding *atomic.Int64

	replicas *replicas // set by RunClients
}

// ClientResult is what the clients observed.
type ClientResult struct {
	Submitted  int               `json:"submitted"`
	Accepted   map[string]string `json:"accepted"`   // idempotency key → workflow_id
	Rejected   int               `json:"rejected"`   // malformed requests refused, as expected
	Conflicts  []string          `json:"conflicts"`  // keys that saw two different workflow IDs
	Unfinished []string          `json:"unfinished"` // keys not accepted before the context ended
	Unexpected []string          `json:"unexpected"` // responses no correct server should give
	Retries    int64             `json:"retries"`    // attempts that hit an error, 5xx, 409 or 429
	Behaviors  map[Behavior]int  `json:"behaviors"`
	PerReplica map[string]int    `json:"per_replica"` // base URL → requests it answered (any status)
}

// replicas hands out base URLs in turn and counts the answers each gave.
type replicas struct {
	urls     []string
	next     atomic.Uint64
	mu       sync.Mutex
	answered map[string]int
}

func (r *replicas) pick() string {
	return r.urls[(r.next.Add(1)-1)%uint64(len(r.urls))]
}

func (r *replicas) answer(url string) {
	r.mu.Lock()
	r.answered[url]++
	r.mu.Unlock()
}

type job struct {
	key      string
	behavior Behavior
	body     map[string]any
	seed     uint64
}

// PlanBehaviors returns the behavior mix RunClients will use for (seed, n).
func PlanBehaviors(seed int64, n int) map[Behavior]int {
	mix := map[Behavior]int{}
	for _, j := range planJobs(seed, n) {
		mix[j.behavior]++
	}
	return mix
}

func planJobs(seed int64, n int) []job {
	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed)+1)) //nolint:gosec // reproducible traffic, not security
	jobs := make([]job, n)
	for i := range jobs {
		customer, merchant := fmt.Sprintf("cus_%d", i), fmt.Sprintf("mer_%d", i)
		if rng.IntN(100) < 5 {
			customer = fmt.Sprintf("cus_decline_%d", i)
		}
		if rng.IntN(100) < 10 {
			merchant = fmt.Sprintf("mer_invalid_%d", i)
		}
		jobs[i] = job{
			key:      fmt.Sprintf("chaos-%d-%d", seed, i),
			behavior: pickBehavior(rng),
			body: map[string]any{
				"amount_cents": 1000 + rng.Int64N(49000),
				"currency":     "EUR",
				"customer_id":  customer,
				"merchant_id":  merchant,
			},
			seed: rng.Uint64(),
		}
	}
	return jobs
}

func pickBehavior(rng *rand.Rand) Behavior {
	switch n := rng.IntN(100); {
	case n < 40:
		return Polite
	case n < 65:
		return Impatient
	case n < 80:
		return DoubleClick
	case n < 90:
		return Reformatted
	default:
		return Malformed
	}
}

// RunClients submits cfg.Workflows workflows with a seeded mix of behaviors and
// returns what was observed. It returns when every client is done or ctx ends.
func RunClients(ctx context.Context, cfg ClientConfig) ClientResult {
	jobs := planJobs(cfg.Seed, cfg.Workflows)
	if len(cfg.BaseURLs) == 0 {
		return ClientResult{Submitted: len(jobs), Accepted: map[string]string{}, Behaviors: map[Behavior]int{},
			Unexpected: []string{"no API replica to send to (empty BaseURLs)"}}
	}
	cfg.replicas = &replicas{urls: cfg.BaseURLs, answered: map[string]int{}}
	res := ClientResult{Submitted: len(jobs), Accepted: map[string]string{}, Behaviors: map[Behavior]int{}}
	var mu sync.Mutex
	var retries atomic.Int64

	run := func(j job) outcome {
		if cfg.Outstanding != nil {
			cfg.Outstanding.Add(1)
			defer cfg.Outstanding.Add(-1)
		}
		return runJob(ctx, cfg, j, &retries)
	}
	record := func(j job, o outcome) {
		mu.Lock()
		defer mu.Unlock()
		res.Behaviors[j.behavior]++
		switch {
		case o.unexpected != "":
			res.Unexpected = append(res.Unexpected, j.key+": "+o.unexpected)
		case o.conflict:
			res.Conflicts = append(res.Conflicts, j.key)
		case o.rejected:
			res.Rejected++
		case o.workflowID != "":
			res.Accepted[j.key] = o.workflowID
		default:
			res.Unfinished = append(res.Unfinished, j.key)
		}
	}

	var wg sync.WaitGroup
	if cfg.Spread > 0 {
		// Open loop: each workflow starts at its own arrival time.
		start := time.Now()
		for i, at := range arrivals(cfg.Seed, len(jobs), cfg.Spread) {
			select {
			case <-ctx.Done():
			case <-time.After(time.Until(start.Add(at))):
			}
			j := jobs[i]
			wg.Go(func() { record(j, run(j)) })
		}
	} else {
		work := make(chan job)
		for range max(cfg.Concurrency, 1) {
			wg.Go(func() {
				for j := range work {
					record(j, run(j))
				}
			})
		}
		for _, j := range jobs {
			work <- j
		}
		close(work)
	}
	wg.Wait()
	res.Retries = retries.Load()
	cfg.replicas.mu.Lock()
	res.PerReplica = maps.Clone(cfg.replicas.answered)
	cfg.replicas.mu.Unlock()
	return res
}

// arrivals returns n sorted arrival offsets uniformly spread over window — the
// arrival times of a Poisson process conditioned on n arrivals.
func arrivals(seed int64, n int, window time.Duration) []time.Duration {
	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed)+2)) //nolint:gosec // reproducible traffic
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = time.Duration(rng.Int64N(int64(window)))
	}
	slices.Sort(out)
	return out
}

type outcome struct {
	workflowID string
	rejected   bool
	conflict   bool
	unexpected string
}

func runJob(ctx context.Context, cfg ClientConfig, j job, retries *atomic.Int64) outcome {
	rng := rand.New(rand.NewPCG(j.seed, j.seed^0x9e3779b97f4a7c15)) //nolint:gosec // reproducible traffic
	compact, _ := json.Marshal(j.body)

	switch j.behavior {
	case Malformed:
		return submitUntilAnswered(ctx, cfg, retries, j.key, []byte(`{"amount_cents":`), nil)

	case Impatient:
		timeout := func() time.Duration { return time.Duration(50+rng.IntN(250)) * time.Millisecond }
		return submitUntilAnswered(ctx, cfg, retries, j.key, compact, timeout)

	case DoubleClick:
		var a, b outcome
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); a = submitUntilAnswered(ctx, cfg, retries, j.key, compact, nil) }()
		go func() { defer wg.Done(); b = submitUntilAnswered(ctx, cfg, retries, j.key, compact, nil) }()
		wg.Wait()
		return merge(a, b)

	case Reformatted:
		first := submitUntilAnswered(ctx, cfg, retries, j.key, compact, nil)
		if first.workflowID == "" {
			return first
		}
		pretty, _ := json.MarshalIndent(reordered(j.body), "", "    ")
		return merge(first, submitUntilAnswered(ctx, cfg, retries, j.key, pretty, nil))

	default: // Polite
		return submitUntilAnswered(ctx, cfg, retries, j.key, compact, nil)
	}
}

// reordered returns the body with keys in reverse order (encoding/json sorts
// map keys, so use an ordered slice of fields).
func reordered(body map[string]any) json.RawMessage {
	var buf bytes.Buffer
	buf.WriteString("{")
	keys := []string{"merchant_id", "customer_id", "currency", "amount_cents"}
	for i, k := range keys {
		v, _ := json.Marshal(body[k])
		fmt.Fprintf(&buf, "%q:%s", k, v)
		if i < len(keys)-1 {
			buf.WriteString(",")
		}
	}
	buf.WriteString("}")
	return buf.Bytes()
}

func merge(a, b outcome) outcome {
	switch {
	case a.unexpected != "":
		return a
	case b.unexpected != "":
		return b
	case a.workflowID != "" && b.workflowID != "" && a.workflowID != b.workflowID:
		return outcome{conflict: true}
	case a.workflowID != "":
		return a
	default:
		return b
	}
}

// submitUntilAnswered retries with the same key until the server gives a final
// answer (2xx, or 4xx for a malformed body) or ctx ends. timeout, if set,
// returns a fresh per-attempt client timeout.
func submitUntilAnswered(ctx context.Context, cfg ClientConfig, retries *atomic.Int64, key string, body []byte, timeout func() time.Duration) outcome {
	backoff := 50 * time.Millisecond
	for ctx.Err() == nil {
		d := 5 * time.Second
		if timeout != nil {
			d = timeout()
		}
		status, respBody, err := post(ctx, cfg, key, body, d)
		switch {
		case err != nil, status >= 500, status == http.StatusConflict, status == http.StatusTooManyRequests:
			// Transport error, server error, or a duplicate still in flight: retry.
		case status == http.StatusCreated || status == http.StatusOK:
			var c struct {
				WorkflowID string `json:"workflow_id"`
			}
			if json.Unmarshal(respBody, &c) != nil || c.WorkflowID == "" {
				return outcome{unexpected: fmt.Sprintf("%d without workflow_id: %s", status, respBody)}
			}
			return outcome{workflowID: c.WorkflowID}
		case status == http.StatusBadRequest && bytes.HasPrefix(body, []byte(`{"amount_cents":`)) && !json.Valid(body):
			return outcome{rejected: true}
		default:
			return outcome{unexpected: fmt.Sprintf("status %d: %s", status, respBody)}
		}
		retries.Add(1)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, time.Second)
	}
	return outcome{}
}

func post(ctx context.Context, cfg ClientConfig, key string, body []byte, timeout time.Duration) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	url := cfg.replicas.pick()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/workflows", bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Token)
	req.Header.Set("Idempotency-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	cfg.replicas.answer(url)
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, b, err
}
