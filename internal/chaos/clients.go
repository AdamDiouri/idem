package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"sync"
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
	BaseURL     string
	Token       string
	Workflows   int
	Concurrency int
	Seed        int64
}

// ClientResult is what the clients observed.
type ClientResult struct {
	Submitted  int               `json:"submitted"`
	Accepted   map[string]string `json:"accepted"`   // idempotency key → workflow_id
	Rejected   int               `json:"rejected"`   // malformed requests refused, as expected
	Conflicts  []string          `json:"conflicts"`  // keys that saw two different workflow IDs
	Unfinished []string          `json:"unfinished"` // keys not accepted before the context ended
	Unexpected []string          `json:"unexpected"` // responses no correct server should give
	Behaviors  map[Behavior]int  `json:"behaviors"`
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
	res := ClientResult{Submitted: len(jobs), Accepted: map[string]string{}, Behaviors: map[Behavior]int{}}
	var mu sync.Mutex

	work := make(chan job)
	var wg sync.WaitGroup
	for range max(cfg.Concurrency, 1) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range work {
				o := runJob(ctx, cfg, j)
				mu.Lock()
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
				mu.Unlock()
			}
		}()
	}
	for _, j := range jobs {
		work <- j
	}
	close(work)
	wg.Wait()
	return res
}

type outcome struct {
	workflowID string
	rejected   bool
	conflict   bool
	unexpected string
}

func runJob(ctx context.Context, cfg ClientConfig, j job) outcome {
	rng := rand.New(rand.NewPCG(j.seed, j.seed^0x9e3779b97f4a7c15)) //nolint:gosec // reproducible traffic
	compact, _ := json.Marshal(j.body)

	switch j.behavior {
	case Malformed:
		return submitUntilAnswered(ctx, cfg, j.key, []byte(`{"amount_cents":`), nil)

	case Impatient:
		timeout := func() time.Duration { return time.Duration(50+rng.IntN(250)) * time.Millisecond }
		return submitUntilAnswered(ctx, cfg, j.key, compact, timeout)

	case DoubleClick:
		var a, b outcome
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); a = submitUntilAnswered(ctx, cfg, j.key, compact, nil) }()
		go func() { defer wg.Done(); b = submitUntilAnswered(ctx, cfg, j.key, compact, nil) }()
		wg.Wait()
		return merge(a, b)

	case Reformatted:
		first := submitUntilAnswered(ctx, cfg, j.key, compact, nil)
		if first.workflowID == "" {
			return first
		}
		pretty, _ := json.MarshalIndent(reordered(j.body), "", "    ")
		return merge(first, submitUntilAnswered(ctx, cfg, j.key, pretty, nil))

	default: // Polite
		return submitUntilAnswered(ctx, cfg, j.key, compact, nil)
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
func submitUntilAnswered(ctx context.Context, cfg ClientConfig, key string, body []byte, timeout func() time.Duration) outcome {
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.BaseURL+"/workflows", bytes.NewReader(body))
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
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, b, err
}
