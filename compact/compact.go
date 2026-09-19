// Package compact is the reference compaction Transform for agentturn:
// when a transcript grows past a token budget, the older part is folded
// into a compaction item by the model's own compaction endpoint and the
// recent part is kept verbatim.
//
//	c := compact.New(model, compact.WithBudget(120_000))
//	cfg.Transform = c.Transform
//
// A Transform runs before every model call and shapes that call only;
// the loop's working transcript is never replaced. The transform
// therefore remembers what it compacted: as long as the transcript still
// begins with the prefix it folded, the next call reuses the compaction
// and only folds again when the kept part outgrows the budget. A front
// that wants to persist the compaction reads it from [Transform.Last].
//
// Local summarisation is a second implementation behind the same hook,
// not this package.
package compact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// Compactor is the compaction side of an openresponses.Adapter.
type Compactor interface {
	Compact(ctx context.Context, req openresponses.CompactRequest) (*openresponses.CompactResponse, error)
}

// DefaultBudget is the token budget when none is set.
const DefaultBudget = 100_000

// DefaultKeepLast is the number of recent items never compacted when
// none is set.
const DefaultKeepLast = 4

// Option configures a Transform.
type Option func(*Transform)

// WithBudget sets the estimated token count above which the transcript
// is compacted.
func WithBudget(tokens int) Option { return func(t *Transform) { t.budget = tokens } }

// WithEstimator replaces the token estimator. The default divides the
// JSON size of the items by four.
func WithEstimator(fn func(openresponses.Items) int) Option {
	return func(t *Transform) { t.estimate = fn }
}

// WithModel sets the model named on compaction requests.
func WithModel(name string) Option { return func(t *Transform) { t.model = name } }

// WithKeepLast sets how many recent items are always kept verbatim. The
// cut never separates a function call from its output.
func WithKeepLast(n int) Option { return func(t *Transform) { t.keepLast = n } }

// WithFilter sets the filter applied to the items sent for compaction,
// so app-only items never reach the server. The default is
// agentturn.DefaultFilter; pass the same function as Config.Filter.
func WithFilter(fn func(agentturn.Transcript) openresponses.Items) Option {
	return func(t *Transform) { t.filter = fn }
}

// Transform compacts transcripts. Its Transform method is the value for
// agentturn.Config.Transform. It is safe for concurrent use, but it
// remembers one compacted prefix, so share one per conversation.
type Transform struct {
	compactor Compactor
	budget    int
	keepLast  int
	model     string
	estimate  func(openresponses.Items) int
	filter    func(agentturn.Transcript) openresponses.Items

	mu sync.Mutex
	// prefixLen items of the transcript are represented by output.
	prefixLen  int
	prefixHash string
	output     openresponses.Items
	last       *openresponses.Compaction
}

// New builds a Transform over c.
func New(c Compactor, opts ...Option) *Transform {
	t := &Transform{
		compactor: c,
		budget:    DefaultBudget,
		keepLast:  DefaultKeepLast,
		estimate:  Estimate,
		filter:    agentturn.DefaultFilter,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Estimate is the default token estimator: the JSON size of the items
// divided by four.
func Estimate(items openresponses.Items) int {
	data, err := json.Marshal(items)
	if err != nil {
		return 0
	}
	return len(data) / 4
}

// Last returns the most recent compaction item produced, or nil.
func (t *Transform) Last() *openresponses.Compaction {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.last
}

// Transform returns the transcript to send for this call: unchanged
// when it fits the budget, otherwise the compaction of its older part
// followed by the recent items.
func (t *Transform) Transform(ctx context.Context, items agentturn.Transcript) (agentturn.Transcript, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	view, base := t.view(items)
	if t.estimate(view) <= t.budget {
		return view, nil
	}
	split := t.split(items)
	if split <= base {
		return view, nil
	}
	input := append(openresponses.Items(nil), t.output...)
	if base == 0 {
		input = nil
	}
	input = append(input, t.filter(items[base:split])...)
	if len(input) == 0 {
		return view, nil
	}
	resp, err := t.compactor.Compact(ctx, openresponses.CompactRequest{Model: t.model, Input: input})
	if err != nil {
		return nil, fmt.Errorf("compact: %w", err)
	}
	if resp == nil || len(resp.Output) == 0 {
		return nil, fmt.Errorf("compact: empty compaction response")
	}
	t.prefixLen = split
	t.prefixHash = hash(items[:split])
	t.output = append(openresponses.Items(nil), resp.Output...)
	for _, item := range resp.Output {
		if c, ok := item.(*openresponses.Compaction); ok {
			t.last = c
			break
		}
	}
	return t.join(items, split), nil
}

// view returns the transcript with the remembered compaction applied,
// and how many items it covers; zero when the memory no longer matches.
func (t *Transform) view(items agentturn.Transcript) (agentturn.Transcript, int) {
	if t.prefixLen > 0 && len(items) >= t.prefixLen && hash(items[:t.prefixLen]) == t.prefixHash {
		return t.join(items, t.prefixLen), t.prefixLen
	}
	t.prefixLen, t.prefixHash, t.output = 0, "", nil
	return items, 0
}

// join returns the remembered output followed by items from n.
func (t *Transform) join(items agentturn.Transcript, n int) agentturn.Transcript {
	out := make(agentturn.Transcript, 0, len(t.output)+len(items)-n)
	out = append(out, t.output...)
	return append(out, items[n:]...)
}

// split returns the index where the kept tail starts: keepLast items
// from the end, moved earlier so the tail never opens with a function
// call output whose call would be left behind.
func (t *Transform) split(items agentturn.Transcript) int {
	split := len(items) - t.keepLast
	if split < 0 {
		split = 0
	}
	for split > 0 {
		if _, ok := items[split].(*openresponses.FunctionCallOutput); !ok {
			break
		}
		split--
	}
	return split
}

func hash(items agentturn.Transcript) string {
	data, err := json.Marshal(items)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
