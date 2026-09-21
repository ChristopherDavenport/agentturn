// Package compact is the reference compaction Transform for agentturn:
// when a transcript grows past a token budget, the older part is folded
// into a shorter form and the recent part is kept verbatim.
//
// Two folds are provided behind the same Transform. [New] uses the
// model's own compaction endpoint and splices the returned compaction
// item in; [NewLocal] asks an ordinary model call for a summary and
// splices that in as a message, for the many servers that do not
// implement compaction.
//
//	c := compact.New(model, compact.WithBudget(120_000))
//	cfg.Transform = c.Transform
//
//	l := compact.NewLocal(model, compact.WithBudget(120_000), compact.WithModel("gpt-5-mini"))
//	cfg.Transform = l.Transform
//
// A Transform runs before every model call and shapes that call only;
// the loop's working transcript is never replaced. The transform
// therefore remembers what it compacted: as long as the transcript still
// begins with the prefix it folded, the next call reuses the result and
// only folds again when the kept part outgrows the budget. When it
// folds again, the previous result is folded with the new prefix, so
// what an earlier fold kept is summarised again rather than lost. A
// front that wants to persist the result registers [WithOnFold], which
// reports every fold, successful or not, with the index at which the
// transcript was split; [Transform.Last] returns the latest summary.
// [WithPin] keeps chosen items of the folded prefix verbatim after the
// summary, for context a harness injected that must not become
// whatever the summary made of it.
package compact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

// DefaultSummaryPrompt is the instruction [NewLocal] sends with the
// items to fold when [WithSummaryPrompt] is not given.
const DefaultSummaryPrompt = `Summarize the conversation above so that it can continue without the original messages. Keep every fact, decision, constraint, open question and identifier (file names, IDs, URLs, numbers) that later turns may need, and the outcome of every tool call. If the conversation opens with an earlier summary, fold its content into yours rather than repeating it. Write in the third person, in prose or short lists, with no preamble.`

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

// WithModel sets the model named on compaction or summary requests.
func WithModel(name string) Option { return func(t *Transform) { t.model = name } }

// WithKeepLast sets how many recent items are always kept verbatim. The
// cut never separates a function call from its output.
func WithKeepLast(n int) Option { return func(t *Transform) { t.keepLast = n } }

// WithPin keeps the items fn reports through a fold: whatever part of
// the folded prefix they were in, they follow the summary in the
// request, in their order, and the fold that summarised them
// summarised them too, so nothing is lost if the pin is later dropped.
// It is for context a harness injected and must not lose to a summary:
// a stream rule's reminder, a policy notice, an instruction the user
// gave once. [WithKeepLast] keeps a window at the end; this keeps a
// member of the part that is folded.
//
// A pinned item is one the request carries that the stored path does
// not rebuild, because a compaction entry names where the kept tail
// starts and nothing else. A session recorder therefore records the
// calls after such a fold without a request hash, naming the pinned
// items in the compaction entry, rather than recording a hash that
// would not verify.
func WithPin(fn func(openresponses.Item) bool) Option {
	return func(t *Transform) { t.pin = fn }
}

// WithFilter sets the filter applied to the items sent to be folded,
// so app-only items never reach the server. The default is
// agentturn.DefaultFilter; pass the same function as Config.Filter.
func WithFilter(fn func(agentturn.Transcript) agentturn.Transcript) Option {
	return func(t *Transform) { t.filter = fn }
}

// WithSummaryPrompt replaces [DefaultSummaryPrompt] for [NewLocal]. It
// is sent as the last user message of the summary request, after the
// items to fold. [New] ignores it.
func WithSummaryPrompt(prompt string) Option { return func(t *Transform) { t.prompt = prompt } }

// Fold describes one compaction attempt on the transcript passed to
// [Transform.Transform].
type Fold struct {
	// Split is the index into that transcript at which the kept tail
	// starts: the items before it were folded, and the items from it on
	// were sent verbatim after Output.
	Split int
	// Output stands in for the folded items on the request: the
	// compaction endpoint's output for [New], the summary message for
	// [NewLocal]. nil when the fold failed. The request the transform
	// returns is Output, then Pinned, then the items from Split on, so
	// the two members name different things and a reader that wants
	// what was sent joins them in that order.
	Output openresponses.Items
	// Summary is the item a recorder writes as the compaction summary:
	// the compaction item from [New], the summary message from
	// [NewLocal]. nil when the fold failed.
	Summary openresponses.Item
	// TokensBefore is the estimate that triggered the fold.
	TokensBefore int
	// Usage is what the fold's model call reported, when it did.
	Usage *openresponses.Usage
	// ResponseID is the ID of the response the fold's model call
	// produced, from either endpoint, so a recorder can tie the fold to
	// a call the server made and a replay can recognise the fold's call
	// among the run's.
	ResponseID string
	// Pinned are the items of the folded prefix that [WithPin] kept, in
	// their order. They follow Output on the request and are not part
	// of it.
	Pinned openresponses.Items
	// Request is the request [NewLocal] sent for the fold: the items
	// being folded and the summary prompt. Its input is no path's
	// context, so a hash of it never rebuilds from a stored path; a
	// recorder that keeps it must mark it as the fold's own call. nil
	// for [New], whose compaction request is not a Request.
	Request *openresponses.Request
	// Err is set when the fold failed; Transform returns it. A fold cut
	// off by an abort carries the context error.
	Err error
}

// WithOnFold sets a function called after every fold attempt, from the
// goroutine that called Transform and before Transform returns, with
// the fold that was applied or the failure. An error it returns fails
// the Transform and so the turn, the way a subscriber's error fails a
// run, so a recorder that could not write the fold stops the run
// rather than letting the record drift from the request. A fold
// discarded because another caller folded the same transcript meanwhile
// is not reported. A recorder registers here; see agentturn/session.
func WithOnFold(fn func(context.Context, Fold) error) Option {
	return func(t *Transform) { t.onFold = fn }
}

// WithSummaryItem sets how [NewLocal] turns the summary text into the
// item that stands in for the folded prefix. The default is a user
// message opening with "Summary of the conversation so far:", which
// every server accepts on input. A server that accepts compaction
// items on input could be given one here instead. [New] ignores it.
func WithSummaryItem(fn func(summary string) openresponses.Item) Option {
	return func(t *Transform) { t.summaryItem = fn }
}

// Transform compacts transcripts. Its Transform method is the value for
// agentturn.Config.Transform. It is safe for concurrent use and does
// not hold its lock across the model call, but it remembers one
// compacted prefix, so share one per conversation.
type Transform struct {
	fold        func(ctx context.Context, input openresponses.Items) (folded, error)
	onFold      func(context.Context, Fold) error
	budget      int
	keepLast    int
	model       string
	estimate    func(openresponses.Items) int
	filter      func(agentturn.Transcript) agentturn.Transcript
	prompt      string
	summaryItem func(string) openresponses.Item
	pin         func(openresponses.Item) bool

	mu sync.Mutex
	// prefixLen items of the transcript are represented by output. An
	// empty prefixHash means no memory.
	prefixLen  int
	prefixHash string
	output     openresponses.Items
	last       openresponses.Item
}

// New builds a Transform that folds through c's compaction endpoint.
// The result is the endpoint's output, a compaction item the same
// server expands on the next call.
func New(c Compactor, opts ...Option) *Transform {
	t := newTransform(opts)
	t.fold = func(ctx context.Context, input openresponses.Items) (folded, error) {
		resp, err := c.Compact(ctx, openresponses.CompactRequest{Model: t.model, Input: input})
		if err != nil {
			return folded{}, fmt.Errorf("compact: %w", err)
		}
		if resp == nil || len(resp.Output) == 0 {
			return folded{}, errors.New("compact: empty compaction response")
		}
		f := folded{output: append(openresponses.Items(nil), resp.Output...), usage: resp.Usage, responseID: resp.ID}
		for _, item := range f.output {
			if c, ok := item.(*openresponses.Compaction); ok {
				f.summary = c
				break
			}
		}
		return f, nil
	}
	return t
}

// folded is what a fold produced: the items that stand in for the
// prefix, the one among them a recorder keeps as the summary, the
// usage of the call that made them, and the call itself.
type folded struct {
	output     openresponses.Items
	summary    openresponses.Item
	usage      *openresponses.Usage
	responseID string
	request    *openresponses.Request
}

// NewLocal builds a Transform that folds by asking model for a summary
// with an ordinary call: the items to fold, then the summary prompt as
// a user message, with no tools. The result is one message carrying
// the summary (see [WithSummaryItem]). It works against any server,
// including those that answer 404 to the compaction endpoint. The
// model is named by [WithModel]; leave it empty to let the server
// pick its default.
func NewLocal(model openresponses.Streamer, opts ...Option) *Transform {
	t := newTransform(opts)
	t.fold = func(ctx context.Context, input openresponses.Items) (folded, error) {
		store := false
		req := openresponses.Request{
			Model: t.model,
			Input: append(append(openresponses.Items(nil), input...), openresponses.UserText(t.prompt)),
			Store: &store,
		}
		resp, err := openresponses.CollectStream(ctx, model, req)
		if err != nil {
			return folded{}, fmt.Errorf("compact: summary: %w", err)
		}
		if resp.Status == openresponses.ResponseStatusFailed {
			if resp.Error != nil {
				return folded{}, fmt.Errorf("compact: summary: %w", resp.Error.Err(0))
			}
			return folded{}, errors.New("compact: summary response failed")
		}
		summary := strings.TrimSpace(resp.OutputText())
		if summary == "" {
			return folded{}, errors.New("compact: summary response has no text")
		}
		item := t.summaryItem(summary)
		return folded{output: openresponses.Items{item}, summary: item, usage: resp.Usage, responseID: resp.ID, request: &req}, nil
	}
	return t
}

func newTransform(opts []Option) *Transform {
	t := &Transform{
		budget:      DefaultBudget,
		keepLast:    DefaultKeepLast,
		estimate:    Estimate,
		filter:      agentturn.DefaultFilter,
		prompt:      DefaultSummaryPrompt,
		summaryItem: SummaryMessage,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// SummaryMessage is the default [WithSummaryItem]: a user message that
// opens with "Summary of the conversation so far:" and carries the
// summary.
func SummaryMessage(summary string) openresponses.Item {
	return openresponses.UserText("Summary of the conversation so far:\n\n" + summary)
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

// Last returns the item that most recently replaced a folded prefix,
// or nil: the compaction item from [New], the summary message from
// [NewLocal]. [WithOnFold] reports the same item with the split that
// produced it.
func (t *Transform) Last() openresponses.Item {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.last
}

// Transform returns the transcript to send for this call: unchanged
// when it fits the budget, otherwise the fold of its older part
// followed by the recent items.
func (t *Transform) Transform(ctx context.Context, items agentturn.Transcript) (agentturn.Transcript, error) {
	t.mu.Lock()
	view, base := t.view(items)
	tokens := t.estimate(view)
	if tokens <= t.budget {
		t.mu.Unlock()
		return view, nil
	}
	split := t.split(items)
	if split <= base {
		t.mu.Unlock()
		return view, nil
	}
	var input openresponses.Items
	if base > 0 {
		input = append(input, t.output...)
	}
	input = append(input, t.filter(items[base:split])...)
	if len(input) == 0 {
		t.mu.Unlock()
		return view, nil
	}
	prevLen, prevHash := t.prefixLen, t.prefixHash
	t.mu.Unlock()

	// The model call runs unlocked so concurrent callers do not
	// serialise on the network.
	f, err := t.fold(ctx, input)
	if err != nil {
		if t.onFold != nil {
			if rerr := t.onFold(ctx, Fold{Split: split, TokensBefore: tokens, Err: err}); rerr != nil {
				return nil, fmt.Errorf("compact: on-fold: %w", rerr)
			}
		}
		return nil, err
	}

	t.mu.Lock()
	if t.prefixLen != prevLen || t.prefixHash != prevHash {
		// Another caller folded meanwhile; its memory stands and this
		// call is answered from it.
		view, _ := t.view(items)
		t.mu.Unlock()
		return view, nil
	}
	pinned := t.pinned(items[:split])
	t.prefixLen = split
	t.prefixHash = hash(items[:split])
	t.output = append(append(openresponses.Items(nil), f.output...), pinned...)
	if f.summary != nil {
		t.last = f.summary
	}
	out := t.join(items, split)
	t.mu.Unlock()
	if t.onFold != nil {
		// The fold's own output and the pinned items, as locals: the
		// memory they were written to belongs to the lock that was just
		// released.
		if err := t.onFold(ctx, Fold{Split: split, Output: f.output, Summary: f.summary, Pinned: pinned, TokensBefore: tokens, Usage: f.usage, ResponseID: f.responseID, Request: f.request}); err != nil {
			return nil, fmt.Errorf("compact: on-fold: %w", err)
		}
	}
	return out, nil
}

// pinned returns the items of the folded prefix that the pin keeps, in
// their order, filtered as the request is so an app-only item never
// reaches the server.
func (t *Transform) pinned(prefix agentturn.Transcript) openresponses.Items {
	if t.pin == nil {
		return nil
	}
	var out openresponses.Items
	for _, item := range t.filter(prefix) {
		if t.pin(item) {
			out = append(out, item)
		}
	}
	return out
}

// view returns the transcript with the remembered fold applied, and how
// many items it covers; zero when the memory no longer matches.
func (t *Transform) view(items agentturn.Transcript) (agentturn.Transcript, int) {
	if t.prefixHash != "" && t.prefixLen > 0 && len(items) >= t.prefixLen && hash(items[:t.prefixLen]) == t.prefixHash {
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

// hash identifies a prefix; an empty result means no memory, never a
// match.
func hash(items agentturn.Transcript) string {
	data, err := json.Marshal(items)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
