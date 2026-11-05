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
// whatever the summary made of it. [WithRequest] edits the summary
// request [NewLocal] sends, so it can carry the reasoning setting the
// agent's own requests do.
package compact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/gif"  // GIF headers, for an image's visual cost
	_ "image/jpeg" // JPEG headers
	_ "image/png"  // PNG headers
	"math"
	"strings"
	"sync"

	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

type foldKey struct{}

// MarkFold returns ctx marked as the context of a model call a fold
// makes for itself, such as [NewLocal]'s summary. The mark rides beside
// the request, not on it, so neither the provider nor a request hash
// sees it.
func MarkFold(ctx context.Context) context.Context {
	return context.WithValue(ctx, foldKey{}, true)
}

// IsFold reports whether ctx is the context of a model call a fold
// makes for itself, as [MarkFold] marks it: a model, a streamer
// wrapping one or a replay tells a fold's call from a turn's by this,
// when the call arrives, rather than by the request's shape.
func IsFold(ctx context.Context) bool {
	v, _ := ctx.Value(foldKey{}).(bool)
	return v
}

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
// is compacted. The budget covers the whole request: the items the
// transform would send and the settings the request carries beside
// them, its instructions and its tool definitions, which the loop puts
// on the transform's context (agentturn.RequestFromContext) and
// [EstimateSettings] weighs. So a budget set as a share of the model's
// context window is a share of what the window holds. A transform
// called with no settings on its context, or given [WithEstimator]
// without [WithSettingsEstimator], weighs the items alone.
func WithBudget(tokens int) Option { return func(t *Transform) { t.budget = tokens } }

// WithEstimator replaces the token estimator for items. The default is
// [Estimate]. An estimator of its own counts in its own units, so the
// settings are weighed only when [WithSettingsEstimator] gives an
// estimator for them in the same units.
func WithEstimator(fn func(openresponses.Items) int) Option {
	return func(t *Transform) {
		t.estimate = fn
		if !t.settingsSet {
			t.settings = nil
		}
	}
}

// WithSettingsEstimator replaces the estimator of the settings a
// request carries beside its input, the instructions and the tool
// definitions, which the transform adds to the estimate of the items
// before it compares the sum with the budget. The default is
// [EstimateSettings] while the item estimator is [Estimate]. nil weighs
// the items alone.
func WithSettingsEstimator(fn func(openresponses.Request) int) Option {
	return func(t *Transform) { t.settings, t.settingsSet = fn, true }
}

// WithModel sets the model named on compaction or summary requests.
func WithModel(name string) Option { return func(t *Transform) { t.model = name } }

// WithKeepLast sets how many recent items are always kept verbatim. The
// cut never separates a function call from its output.
func WithKeepLast(n int) Option { return func(t *Transform) { t.keepLast = n } }

// WithMinFold sets the estimated token count below which the part of
// the transcript a fold would replace is left as it is: the call goes
// over budget, nothing is asked and nothing is reported to [WithOnFold],
// as for a call within the budget. The part weighed is what the fold
// would send to be folded, the previous fold's output included. It is
// for a transcript over budget because of its kept tail, such as a
// large tool output among the last [WithKeepLast] items, whose prefix
// is too small for any summary to shrink.
//
// For [NewLocal] the default is the larger of an eighth of the budget
// and twice the estimate of the summary item [WithSummaryItem] makes
// from no text, the summary's wrapper and as much again for its text,
// both as the other options leave them. A prefix that small saves
// little even when summarised in a word, which is not worth a summary
// call, and a few short items are likely to be refused as no smaller
// than their summary after a second call, which is a failed fold. For
// [New] the default is zero, since the size of a compaction item is
// the server's. Zero folds whatever is over budget.
func WithMinFold(tokens int) Option { return func(t *Transform) { t.minFold = tokens } }

// WithFailedFold seeds the transform with a failed fold it backs off
// from, as though it had made it: the fold reported with [Fold.Split]
// split, [Fold.PrefixHash] prefixHash and [Fold.TokensBefore] tokens.
// A process that restarts, or resumes a conversation in another, passes
// the last such fold its record holds, so it does not ask again for a
// summary that already failed; agentturn/session reads it from the
// record. A transcript that does not begin with that prefix ignores
// it, and an empty prefixHash or a negative split seeds nothing. The
// next fold the transform backs off from replaces it, as in one
// process.
func WithFailedFold(split int, prefixHash string, tokens int) Option {
	return func(t *Transform) {
		if prefixHash != "" && split >= 0 {
			t.failLen, t.failHash, t.failTokens = split, prefixHash, tokens
		}
	}
}

// WithBackOff turns the back-off after a failed fold off when enabled
// is false: a prefix whose fold failed with an unfolded send is asked
// about again on the next call over budget, as before v0.0.13, and
// [WithFailedFold] seeds nothing. The failed fold is still reported to
// [WithOnFold] with its prefix hash. It is for replaying a recording
// made across restarts before v0.0.15, when the back-off lived in the
// transform's memory alone and a host that restarted asked again, or by
// a host that resumed without agentturn/session's CompactOptions: a
// replay in one process would otherwise back off where the recording
// asked. The default is on.
func WithBackOff(enabled bool) Option { return func(t *Transform) { t.noBackOff = !enabled } }

// WithPin keeps the items fn reports through a fold: whatever part of
// the folded prefix they were in, they follow the summary in the
// request, in their order, and the fold that summarised them
// summarised them too, so nothing is lost if the pin is later dropped.
// It is for context a harness injected and must not lose to a summary:
// a stream rule's reminder, a policy notice, an instruction the user
// gave once. [WithKeepLast] keeps a window at the end; this keeps a
// member of the part that is folded.
//
// A pinned item is on the request and not in the transcript, so a
// session recorder names it in the compaction entry, whose pinned
// member the context algorithm places after the summary. The calls
// after such a fold therefore keep their request hashes, and a session
// resumed from the record still carries the pinned items.
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

// WithRequest sets a function that edits the request [NewLocal] sends
// for a summary before it is sent, after the transform has set its
// model, input and store. It is how the summary is asked the way the
// agent asks everything else: a thinking model left at its server's
// default reasoning may think through the whole fold and end with a
// function call copied from the transcript, so a caller whose own
// requests set Reasoning sets the same here, and may set
// MaxOutputTokens, Temperature or any other field. The call's context
// is marked with [MarkFold], so a model or a replay tells it from a
// turn's by [IsFold] whatever the request holds. The transform sets
// MaxOutputTokens to half the
// budget, at most [DefaultSummaryMaxOutputTokens], before fn runs, so
// a summary that runs away is cut by the server; fn may change or
// clear it. fn runs once per attempt on a
// fresh request; the edited request is the one reported as
// [Fold.Request]. [New] ignores it.
func WithRequest(fn func(*openresponses.Request)) Option {
	return func(t *Transform) { t.request = fn }
}

// Fold describes one compaction attempt on the transcript passed to
// [Transform.Transform].
type Fold struct {
	// Split is the index into that transcript at which the kept tail
	// starts: the items before it were folded, and the items from it on
	// were sent verbatim after Output.
	Split int
	// First is the item at Split, the first one kept, nil when the fold
	// kept none or failed. A recorder that names the entry holding it finds it by
	// this item rather than by Split, which counts items of the
	// transcript the transform was given: after another transform in a
	// chain, not the agent's.
	First openresponses.Item
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
	// Usage is what the fold's model calls reported, when they did. For
	// a fold that asked more than once, it is the sum over every call,
	// whether the fold then succeeded or failed: what the fold cost,
	// not what its last call did.
	Usage *openresponses.Usage
	// ResponseID is the ID of the response the fold's model call
	// produced, from either endpoint, so a recorder can tie the fold to
	// a call the server made and a replay can recognise the fold's call
	// among the run's. For a fold that asked more than once, it is the
	// last call's.
	ResponseID string
	// OutputTypes are the item types of the last call's output, in
	// order, such as [reasoning function_call] for a summary that ended
	// in a call: enough to tell a model that answered with something
	// other than text from a stream that ended early. nil when the call
	// produced no response.
	OutputTypes []string
	// Attempts is the number of model calls the fold made: one, or two
	// for a [NewLocal] fold that asked again.
	Attempts int
	// Pinned are the items of the folded prefix that [WithPin] kept, in
	// their order. They follow Output on the request and are not part
	// of it.
	Pinned openresponses.Items
	// Request is the request [NewLocal] sent for the fold's last
	// attempt, as [WithRequest] left it: the items being folded and the
	// summary prompt. A failed fold carries it too. Its input is no
	// path's context, so a hash of it never rebuilds from a stored
	// path; a recorder that keeps it must mark it as the fold's own
	// call. nil for [New], whose compaction request is not a Request.
	Request *openresponses.Request
	// Forced says the fold was asked for, by [Transform.FoldNext] or by
	// the loop after the server refused a request as too long, rather
	// than made because the estimate passed the budget.
	Forced bool
	// Requested says a host or a person asked for the fold, through
	// [Transform.FoldNext]: it is false for a fold the budget or the
	// loop's retry after a context-length refusal forced. A recorder
	// writes agentsession RFC 0001's requested from it, never from
	// Forced, so an overflow's fold does not read as one the person
	// asked for. Instructions are the ones FoldNext gave for it, "" for
	// none, and only ever set beside Requested.
	Requested    bool
	Instructions string
	// PrefixHash is the [PrefixHash] of the transcript's first Split
	// items when the fold failed and the transform backs off from that
	// prefix, as [Transform.Transform] describes, and empty otherwise:
	// with Split and TokensBefore, what [WithFailedFold] takes to back
	// off in another process.
	PrefixHash string
	// Err is set when the fold failed; Transform returns it, save for
	// [ErrSummaryTooLarge], [ErrSummaryIncomplete] and
	// [ErrSummaryNoText], after which Transform sends the transcript
	// unfolded and returns no error. A fold cut off by an abort carries
	// the context error. A failed fold still reports what its calls
	// did: Usage, ResponseID, OutputTypes, Attempts and Request, as far
	// as the calls got.
	Err error
}

// WithOnFold adds a function called after every fold attempt, from the
// goroutine that called Transform and before Transform returns, with
// the fold that was applied or the failure. Each WithOnFold adds one
// more, and they are called in the order they were given, so a
// recorder and a product's own counter both hear every fold. An error
// one returns ends the calling: the ones after it are not called for
// that fold. An error it returns fails
// the Transform and so the turn, the way a subscriber's error fails a
// run, so a recorder that could not write the fold stops the run
// rather than letting the record drift from the request. A fold
// discarded because another caller folded the same transcript meanwhile
// is not reported. A recorder registers here; see agentturn/session.
func WithOnFold(fn func(context.Context, Fold) error) Option {
	return func(t *Transform) {
		if fn != nil {
			t.onFold = append(t.onFold, fn)
		}
	}
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
// compacted prefix and one failed fold, so share one per conversation.
// Both memories are keyed by a hash of the prefix they cover, so a
// Transform shared between conversations never sends one the other's
// fold or skips a fold because the other's failed; it only folds more
// often than one per conversation would, as each forgets the other's.
type Transform struct {
	fold        func(ctx context.Context, input openresponses.Items, instructions string) (folded, error)
	onFold      []func(context.Context, Fold) error
	budget      int
	keepLast    int
	minFold     int
	model       string
	estimate    func(openresponses.Items) int
	settings    func(openresponses.Request) int
	settingsSet bool
	filter      func(agentturn.Transcript) agentturn.Transcript
	prompt      string
	summaryItem func(string) openresponses.Item
	pin         func(openresponses.Item) bool
	request     func(*openresponses.Request)

	mu sync.Mutex
	// next says FoldNext asked the next call to fold whatever its
	// budget, with nextInstructions for that fold alone.
	next             bool
	nextInstructions string
	// prefixLen items of the transcript are represented by output. An
	// empty prefixHash means no memory.
	prefixLen  int
	prefixHash string
	// prefixGen is the quarantine's generation the memory was last
	// checked against: a quarantine that reaches the folded prefix
	// drops it, so the next fold is made without the item.
	prefixGen int
	output    openresponses.Items
	last      openresponses.Item
	// The last fold that failed with an unfolded send: failLen items of
	// the transcript hashing to failHash, at failTokens. An empty
	// failHash means none.
	failLen    int
	failHash   string
	failTokens int
	// noBackOff says WithBackOff(false) was given: a failed fold is
	// remembered and reported but never skips the next.
	noBackOff bool
}

// New builds a Transform that folds through c's compaction endpoint.
// The result is the endpoint's output, a compaction item the same
// server expands on the next call.
func New(c Compactor, opts ...Option) *Transform {
	t := newTransform(opts)
	if t.minFold < 0 {
		t.minFold = 0
	}
	t.fold = func(ctx context.Context, input openresponses.Items, _ string) (folded, error) {
		resp, err := c.Compact(MarkFold(ctx), openresponses.CompactRequest{Model: t.model, Input: input})
		if err != nil {
			return folded{attempts: 1}, fmt.Errorf("compact: %w", err)
		}
		if resp == nil || len(resp.Output) == 0 {
			f := folded{attempts: 1}
			if resp != nil {
				f.usage, f.responseID = resp.Usage, resp.ID
			}
			return f, errors.New("compact: empty compaction response")
		}
		f := folded{output: append(openresponses.Items(nil), resp.Output...), usage: resp.Usage, responseID: resp.ID, outputTypes: itemTypes(resp.Output), attempts: 1}
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
// usage of the call that made them, and the call itself. A fold that
// failed keeps the call's members, so the failure can be reported with
// what the call answered.
type folded struct {
	output      openresponses.Items
	summary     openresponses.Item
	usage       *openresponses.Usage
	responseID  string
	request     *openresponses.Request
	outputTypes []string
	attempts    int
}

// itemTypes returns the type of each item, in order.
func itemTypes(items openresponses.Items) []string {
	out := make([]string, len(items))
	for i, item := range items {
		out[i] = item.ItemType()
	}
	return out
}

// NewLocal builds a Transform that folds by asking model for a summary
// with an ordinary call: the items to fold, then the summary prompt as
// a user message, with no tools. The reasoning items among the items
// to fold are left out of the request: their content is encrypted for
// the model that produced them, which a summariser cannot read, and a
// provider refuses one another model produced. The result is one
// message carrying the summary (see [WithSummaryItem]). It works
// against any server, including those that answer 404 to the
// compaction endpoint. The model is named by [WithModel]; leave it
// empty to let the server pick its default. [WithRequest] edits the
// rest of the request.
//
// The summary request's MaxOutputTokens is half the budget, at most
// [DefaultSummaryMaxOutputTokens], unless [WithRequest] sets it
// otherwise. A summary response with no text, such as one that ends in
// a function call or in reasoning alone, a response the server ended
// incomplete, such as one cut at MaxOutputTokens, and a summary whose
// text is estimated at no fewer tokens than the items it folds are
// model errors rather than server ones, so the summary is asked once
// more. A second such answer is never applied, since there is nothing
// to apply, a summary cut short has lost part of what it folds and one
// too large would grow the request the fold exists to shrink: the fold
// is reported failed with [ErrSummaryNoText], with
// [ErrSummaryIncomplete] and the server's reason, or with
// [ErrSummaryTooLarge], the transcript is sent unfolded, and the
// transform backs off from that prefix as [Transform.Transform]
// describes. The summary's size is that of its text,
// the estimate of its item less that of an item with no text, so the
// wrapper [WithSummaryItem] puts around every summary does not count
// against it. The fold, failed or not, reports the last call's response
// ID, output types and request, the usage of every call summed, and
// the number of attempts.
func NewLocal(model openresponses.Streamer, opts ...Option) *Transform {
	t := newTransform(opts)
	if t.minFold < 0 {
		t.minFold = max(2*t.estimate(openresponses.Items{t.summaryItem("")}), t.budget/8)
	}
	t.fold = func(ctx context.Context, input openresponses.Items, instructions string) (folded, error) {
		f, err := t.summarize(ctx, model, input, instructions)
		f.attempts = 1
		if unfolded(err) {
			first := f.usage
			f, err = t.summarize(ctx, model, input, instructions)
			f.attempts = 2
			f.usage = addUsage(first, f.usage)
		}
		return f, err
	}
	return t
}

// addUsage returns the sum of a and b, nil when both are.
func addUsage(a, b *openresponses.Usage) *openresponses.Usage {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	return &openresponses.Usage{
		InputTokens:         a.InputTokens + b.InputTokens,
		OutputTokens:        a.OutputTokens + b.OutputTokens,
		TotalTokens:         a.TotalTokens + b.TotalTokens,
		InputTokensDetails:  openresponses.InputTokensDetails{CachedTokens: a.InputTokensDetails.CachedTokens + b.InputTokensDetails.CachedTokens},
		OutputTokensDetails: openresponses.OutputTokensDetails{ReasoningTokens: a.OutputTokensDetails.ReasoningTokens + b.OutputTokensDetails.ReasoningTokens},
	}
}

// ErrSummaryNoText is the error of a [NewLocal] fold whose summary
// response had no text twice, such as one that ended in a function
// call or in reasoning alone each time. Like [ErrSummaryTooLarge], such
// a fold is reported to [WithOnFold] with it and the transcript is sent
// unfolded: Transform returns no error, so a model that will not
// summarise a prefix does not fail every turn of the conversation.
var ErrSummaryNoText = errors.New("compact: summary response has no text")

// ErrSummaryIncomplete is the error of a [NewLocal] fold whose summary
// response the server ended incomplete twice, such as one cut at
// MaxOutputTokens each time: a summary cut short has lost what it had
// not yet said, so it is never applied. It is wrapped with the server's
// reason when it gave one. Like [ErrSummaryTooLarge], such a fold is
// reported to [WithOnFold] with it and the transcript is sent unfolded:
// Transform returns no error.
var ErrSummaryIncomplete = errors.New("compact: summary response is incomplete")

// DefaultSummaryMaxOutputTokens caps the MaxOutputTokens [NewLocal]
// sets on a summary request: half the budget, and no more than this.
const DefaultSummaryMaxOutputTokens = 8192

// ErrSummaryTooLarge is the error of a [NewLocal] fold whose summary,
// asked twice, was each time estimated at no fewer tokens than the
// items it was to replace. Such a fold is reported to [WithOnFold]
// with this error, wrapped with the two estimates, and the transcript
// is sent unfolded: Transform returns no error, since a summary that
// would not shrink the request leaves the request as it was rather
// than failing the turn.
//
// After any of the three errors the transform backs off: see
// [Transform.Transform].
var ErrSummaryTooLarge = errors.New("compact: summary is larger than what it folds")

// unfolded reports whether err is the error of a summary that was asked
// for and not applied: one [NewLocal] asks again and, after a second,
// sends the transcript unfolded and backs off from.
func unfolded(err error) bool {
	return errors.Is(err, ErrSummaryNoText) || errors.Is(err, ErrSummaryIncomplete) || errors.Is(err, ErrSummaryTooLarge)
}

// summarize asks model once for a summary of input, with instructions,
// when there are any, after the summary prompt.
func (t *Transform) summarize(ctx context.Context, model openresponses.Streamer, input openresponses.Items, instructions string) (folded, error) {
	// The summary is the fold's own call, and says so to the model.
	ctx = MarkFold(ctx)
	store := false
	prompt := t.prompt
	if instructions != "" {
		prompt += "\n\n" + instructions
	}
	req := openresponses.Request{
		Model: t.model,
		Input: append(withoutReasoning(input), openresponses.UserText(prompt)),
		Store: &store,
	}
	if limit := min(t.budget/2, DefaultSummaryMaxOutputTokens); limit > 0 {
		req.MaxOutputTokens = &limit
	}
	if t.request != nil {
		t.request(&req)
	}
	f := folded{request: &req}
	resp, err := openresponses.CollectStream(ctx, model, req)
	if resp != nil {
		f.usage, f.responseID, f.outputTypes = resp.Usage, resp.ID, itemTypes(resp.Output)
	}
	if err != nil {
		return f, fmt.Errorf("compact: summary: %w", err)
	}
	if resp.Status == openresponses.ResponseStatusFailed {
		if resp.Error != nil {
			return f, fmt.Errorf("compact: summary: %w", resp.Error.Err(0))
		}
		return f, errors.New("compact: summary response failed")
	}
	if resp.Status == openresponses.ResponseStatusIncomplete {
		if resp.IncompleteDetails != nil && resp.IncompleteDetails.Reason != "" {
			return f, fmt.Errorf("%w: %s", ErrSummaryIncomplete, resp.IncompleteDetails.Reason)
		}
		return f, ErrSummaryIncomplete
	}
	summary := strings.TrimSpace(resp.OutputText())
	if summary == "" {
		return f, ErrSummaryNoText
	}
	item := t.summaryItem(summary)
	// The summary's own text is weighed, not the wrapper every summary
	// item carries: a prefix of a few short items would otherwise be
	// smaller than any summary of it.
	got := t.estimate(openresponses.Items{item}) - t.estimate(openresponses.Items{t.summaryItem("")})
	if folds := t.estimate(input); got >= folds {
		return f, fmt.Errorf("%w: %d tokens for %d", ErrSummaryTooLarge, got, folds)
	}
	f.output, f.summary = openresponses.Items{item}, item
	return f, nil
}

// withoutReasoning returns a copy of items without its reasoning
// items. Their content is encrypted for the model that produced them,
// so a summariser reads nothing in them, and a provider refuses one
// another model produced, which the summary model often is.
func withoutReasoning(items openresponses.Items) openresponses.Items {
	out := make(openresponses.Items, 0, len(items)+1)
	for _, item := range items {
		if _, ok := item.(*openresponses.ReasoningItem); !ok {
			out = append(out, item)
		}
	}
	return out
}

func newTransform(opts []Option) *Transform {
	t := &Transform{
		budget:      DefaultBudget,
		keepLast:    DefaultKeepLast,
		minFold:     -1,
		estimate:    Estimate,
		settings:    EstimateSettings,
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
// divided by four, but for images, which a provider charges by their
// visual cost rather than by their encoded size. Each input_image part,
// and each input_file part whose data is an image, wherever it is, in
// a message or a function call output, counts [ImageTokens] for its
// dimensions when its data URL holds a PNG, JPEG or GIF whose header
// gives them, and [DefaultImageTokens] otherwise; its encoded data
// counts nothing more. So a screenshot weighs what the model is charged
// for it, not a hundred thousand tokens of base64.
func Estimate(items openresponses.Items) int {
	data, err := json.Marshal(items)
	if err != nil {
		return 0
	}
	if !bytes.Contains(data, []byte(`"input_image"`)) && !bytes.Contains(data, []byte(`"input_file"`)) {
		return len(data) / 4
	}
	var tree any
	if err := json.Unmarshal(data, &tree); err != nil {
		return len(data) / 4
	}
	images := weighImages(tree)
	rest, err := json.Marshal(tree)
	if err != nil {
		return len(data) / 4
	}
	return len(rest)/4 + images
}

// EstimateSettings is the default estimator of the settings a request
// carries beside its input: the JSON size of its instructions and its
// tool definitions divided by four, as [Estimate] weighs items.
func EstimateSettings(req openresponses.Request) int {
	data, err := json.Marshal(struct {
		Instructions string              `json:"instructions,omitempty"`
		Tools        openresponses.Tools `json:"tools,omitempty"`
	}{req.Instructions, req.Tools})
	if err != nil {
		return 0
	}
	return len(data) / 4
}

// DefaultImageTokens is what [Estimate] counts for an image whose
// dimensions it cannot read: one referred to by URL or file ID, or of a
// format whose header it does not parse. It is about what the largest
// image a provider takes without scaling costs.
const DefaultImageTokens = 1600

// ImageTokens is the visual cost [Estimate] gives an image of width by
// height pixels: its area over 750, after scaling it down to fit
// within 1568 pixels on its long edge and about 1.15 megapixels, as
// the providers that charge by area scale it, and never more than
// [DefaultImageTokens].
func ImageTokens(width, height int) int {
	if width <= 0 || height <= 0 {
		return DefaultImageTokens
	}
	w, h := float64(width), float64(height)
	if long := max(w, h); long > 1568 {
		w, h = w*1568/long, h*1568/long
	}
	if area := w * h; area > 1_150_000 {
		scale := math.Sqrt(1_150_000 / area)
		w, h = w*scale, h*scale
	}
	return min(int(math.Ceil(w*h/750)), DefaultImageTokens)
}

// weighImages walks a decoded JSON tree, removes the encoded data of
// every image part it finds and returns their visual cost.
func weighImages(v any) int {
	total := 0
	switch n := v.(type) {
	case []any:
		for _, e := range n {
			total += weighImages(e)
		}
	case map[string]any:
		switch n["type"] {
		case "input_image":
			url, _ := n["image_url"].(string)
			delete(n, "image_url")
			return imageTokens(url)
		case "input_file":
			if data, _ := n["file_data"].(string); strings.HasPrefix(data, "data:image/") {
				delete(n, "file_data")
				return imageTokens(data)
			}
		}
		for _, e := range n {
			total += weighImages(e)
		}
	}
	return total
}

// imageTokens is the visual cost of the image a URL names: by the
// dimensions in its header when it is a base64 data URL of a format
// image.DecodeConfig reads, [DefaultImageTokens] otherwise.
func imageTokens(url string) int {
	rest, ok := strings.CutPrefix(url, "data:")
	if !ok {
		return DefaultImageTokens
	}
	meta, payload, ok := strings.Cut(rest, ",")
	if !ok || !strings.HasSuffix(meta, ";base64") {
		return DefaultImageTokens
	}
	cfg, _, err := image.DecodeConfig(base64.NewDecoder(base64.StdEncoding, strings.NewReader(payload)))
	if err != nil {
		return DefaultImageTokens
	}
	return ImageTokens(cfg.Width, cfg.Height)
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

// FoldNext asks the next call to [Transform.Transform] to fold whatever
// its budget, with instructions, when not empty, for that fold alone:
// [NewLocal] sends them after the summary prompt, and [New], whose
// compaction endpoint takes no prompt, ignores them. The fold is the
// one a call over budget makes, kept tail and [WithPin] included, and
// is reported to [WithOnFold] as any other, so a recorder writes it.
// [WithMinFold] still guards it: a prefix too small to fold is left as
// it is, and so is a transcript whose older part the remembered fold
// already covers. The back-off after a failed fold does not hold a
// fold asked for. The request is taken by the next call whether or not
// it folds; a later FoldNext before that call replaces its
// instructions. It is how a host compacts on request, such as a
// /compact command, between runs or during one.
func (t *Transform) FoldNext(instructions string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.next, t.nextInstructions = true, instructions
}

// Transform returns the transcript to send for this call: unchanged
// when it fits the budget or when its older part is smaller than
// [WithMinFold], otherwise the fold of its older part followed by the
// recent items. A call after [Transform.FoldNext], or one whose context
// the loop marked with agentturn.ContextWithFoldRequest, as it does to
// rebuild a request the server refused as too long, folds whatever the
// budget.
//
// A fold that failed with [ErrSummaryTooLarge], [ErrSummaryIncomplete]
// or [ErrSummaryNoText] sends the transcript unfolded, and the
// transform remembers it: the length and hash of the prefix it would
// have folded, and the estimate that triggered it. While a later transcript still begins with that
// prefix, it is not folded again until the part to fold has grown by
// at least [WithKeepLast] items (at least one) or the estimate by at
// least a quarter of the budget: the same prefix would most likely
// fail the same way, and asking on every turn would cost two summary
// calls and a failed fold each time. Such a call goes over budget,
// folds nothing and reports nothing to [WithOnFold]. A transcript that
// does not begin with the failed prefix, such as another
// conversation's or one rewound before it, folds as usual.
// [WithBackOff] turns the back-off off.
func (t *Transform) Transform(ctx context.Context, items agentturn.Transcript) (agentturn.Transcript, error) {
	settings := 0
	if req, ok := agentturn.RequestFromContext(ctx); ok && t.settings != nil {
		// The instructions and tools share the window with the items.
		settings = t.settings(req)
	}
	quarantined := agentturn.QuarantineFromContext(ctx)
	t.mu.Lock()
	// A fold FoldNext asked for is requested; one only the loop's
	// retry after an overflow asked for is forced and not requested.
	requested, instructions := t.next, t.nextInstructions
	forced := requested || agentturn.FoldRequested(ctx)
	t.next, t.nextInstructions = false, ""
	view, base := t.view(items, quarantined)
	tokens := t.estimate(view) + settings
	if tokens <= t.budget && !forced {
		t.mu.Unlock()
		return view, nil
	}
	split := t.split(items)
	if split <= base || (!forced && t.backOff(items, split, tokens)) {
		t.mu.Unlock()
		return view, nil
	}
	var input openresponses.Items
	if base > 0 {
		input = append(input, t.output...)
	}
	// What the quarantine leaves out of a request is left out of what
	// the fold sends its model too (#281).
	input = append(input, quarantined.Apply(t.filter(items[base:split]))...)
	if len(input) == 0 || t.estimate(input) < t.minFold {
		t.mu.Unlock()
		return view, nil
	}
	prevLen, prevHash := t.prefixLen, t.prefixHash
	t.mu.Unlock()

	// The model call runs unlocked so concurrent callers do not
	// serialise on the network.
	f, err := t.fold(ctx, input, instructions)
	if err != nil {
		failed := Fold{Split: split, TokensBefore: tokens, Usage: f.usage, ResponseID: f.responseID, OutputTypes: f.outputTypes, Attempts: f.attempts, Request: f.request, Forced: forced, Requested: requested, Instructions: instructions, Err: err}
		if unfolded(err) {
			failed.PrefixHash = PrefixHash(items[:split])
		}
		if rerr := t.report(ctx, failed); rerr != nil {
			return nil, rerr
		}
		if !unfolded(err) {
			return nil, err
		}
		// There was no summary, or it would have grown the request or
		// lost part of what it folds; the request goes as it was, and
		// this prefix is not asked about again until it has grown.
		if failed.PrefixHash != "" {
			t.mu.Lock()
			t.failLen, t.failHash, t.failTokens = split, failed.PrefixHash, tokens
			t.mu.Unlock()
		}
		return view, nil
	}

	t.mu.Lock()
	if t.prefixLen != prevLen || t.prefixHash != prevHash {
		// Another caller folded meanwhile; its memory stands and this
		// call is answered from it.
		view, _ := t.view(items, quarantined)
		t.mu.Unlock()
		return view, nil
	}
	pinned := t.pinned(items[:split])
	var first openresponses.Item
	if split < len(items) {
		first = items[split]
	}
	t.prefixLen = split
	t.prefixGen = quarantined.Generation()
	t.prefixHash = PrefixHash(items[:split])
	t.output = append(append(openresponses.Items(nil), f.output...), pinned...)
	if f.summary != nil {
		t.last = f.summary
	}
	out := t.join(items, split)
	t.mu.Unlock()
	// The fold's own output and the pinned items, as locals: the memory
	// they were written to belongs to the lock that was just released.
	if err := t.report(ctx, Fold{Split: split, First: first, Output: f.output, Summary: f.summary, Pinned: pinned, TokensBefore: tokens, Usage: f.usage, ResponseID: f.responseID, OutputTypes: f.outputTypes, Attempts: f.attempts, Request: f.request, Forced: forced, Requested: requested, Instructions: instructions}); err != nil {
		return nil, err
	}
	return out, nil
}

// report calls each [WithOnFold] function with f in order, stopping at
// the first error.
func (t *Transform) report(ctx context.Context, f Fold) error {
	for _, fn := range t.onFold {
		if err := fn(ctx, f); err != nil {
			return fmt.Errorf("compact: on-fold: %w", err)
		}
	}
	return nil
}

// backOff reports whether a fold of items at split is skipped because
// a fold of the same transcript failed with an unfolded send and the
// transcript has not grown enough since. Called with the lock held.
func (t *Transform) backOff(items agentturn.Transcript, split, tokens int) bool {
	if t.noBackOff || t.failHash == "" || len(items) < t.failLen {
		return false
	}
	if split >= t.failLen+max(t.keepLast, 1) || tokens >= t.failTokens+max(t.budget/4, 1) {
		return false
	}
	return PrefixHash(items[:t.failLen]) == t.failHash
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
func (t *Transform) view(items agentturn.Transcript, quarantined agentturn.QuarantineView) (agentturn.Transcript, int) {
	if t.prefixHash != "" && t.prefixLen > 0 && len(items) >= t.prefixLen && PrefixHash(items[:t.prefixLen]) == t.prefixHash && !t.quarantinedSince(items[:t.prefixLen], quarantined) {
		return t.join(items, t.prefixLen), t.prefixLen
	}
	t.prefixLen, t.prefixHash, t.output = 0, "", nil
	return items, 0
}

// quarantinedSince reports whether the quarantine has grown since the
// memory was last checked to reach an item of prefix, the part the
// remembered fold stands for, whose summary may then carry what the
// quarantine leaves out. A growth that reaches none is taken as checked.
// Called with the lock held.
func (t *Transform) quarantinedSince(prefix agentturn.Transcript, quarantined agentturn.QuarantineView) bool {
	gen := quarantined.Generation()
	if gen == t.prefixGen {
		return false
	}
	for _, item := range prefix {
		if quarantined.Has(item) {
			return true
		}
	}
	t.prefixGen = gen
	return false
}

// join returns the remembered output followed by items from n.
func (t *Transform) join(items agentturn.Transcript, n int) agentturn.Transcript {
	out := make(agentturn.Transcript, 0, len(t.output)+len(items)-n)
	out = append(out, t.output...)
	return append(out, items[n:]...)
}

// split returns the index where the kept tail starts: keepLast items
// from the end, moved earlier so the tail holds no function call output
// whose call would be left behind, wherever in the tail the output is.
// An item between a call and its output, such as an extension item an
// adapter emits after the call, does not separate them.
func (t *Transform) split(items agentturn.Transcript) int {
	split := max(len(items)-t.keepLast, 0)
	calls := map[string]int{}
	for i, item := range items[:split] {
		if c, ok := item.(*openresponses.FunctionCall); ok {
			if _, seen := calls[c.CallID]; !seen {
				calls[c.CallID] = i
			}
		}
	}
	// Walking back from the end, every output met is in the tail, the
	// ones a move of the split brought into it included.
	for i := len(items) - 1; i >= split; i-- {
		if o, ok := items[i].(*openresponses.FunctionCallOutput); ok {
			if at, ok := calls[o.CallID]; ok && at < split {
				split = at
			}
		}
	}
	return split
}

// PrefixHash identifies a prefix of a transcript, the way the transform
// recognises a transcript it folded or failed to fold: the SHA-256, in
// lowercase hex, of the JSON array encoding/json makes of the items,
// each as openresponses marshals it. It is empty, which never matches,
// when the items do not marshal. It is what [Fold.PrefixHash] reports
// and [WithFailedFold] takes, so a session record keeps it; a change to
// an item's encoding in a later openresponses only makes a remembered
// prefix not match, and the transcript is folded as usual.
func PrefixHash(items agentturn.Transcript) string {
	data, err := json.Marshal(items)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
