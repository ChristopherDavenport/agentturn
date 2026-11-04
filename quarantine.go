package agentturn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"

	"github.com/ChristopherDavenport/openresponses"
)

// QuarantinedOutput is the text a quarantined function call output is
// sent with in place of its own. One text serves every quarantined
// output, so a session recorder describes them all with the one
// placeholder its format's omit setting holds.
const QuarantinedOutput = "[This output was withheld: a guard quarantined it as untrusted.]"

// ErrNoQuarantine is the error of [Quarantine] called on a context that
// belongs to no run.
var ErrNoQuarantine = errors.New("agentturn: no run to quarantine items in")

// ErrNotQuarantinable is the error of a quarantine naming an item held
// by value in a type that cannot be compared, as an extension's item
// may be: a quarantine names an item by identity.
var ErrNotQuarantinable = errors.New("agentturn: item cannot be named for quarantine")

// quarantine holds the items quarantined for the rest of a
// conversation, by identity (RFC 0001 The request, step 3; #281). It is
// shared by an agent and its runs, so an item quarantined during a run
// stays quarantined for every later request.
type quarantine struct {
	mu    sync.Mutex
	items map[openresponses.Item]bool
	// gen counts the additions, so the loop can tell that a hook
	// quarantined something.
	gen int
}

// add quarantines items. An item already held, or a nil, adds nothing.
func (q *quarantine) add(items ...openresponses.Item) error {
	for _, item := range items {
		if item != nil && !reflect.TypeOf(item).Comparable() {
			return fmt.Errorf("%w: %s", ErrNotQuarantinable, item.ItemType())
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, item := range items {
		if item == nil || q.items[item] {
			continue
		}
		if q.items == nil {
			q.items = map[openresponses.Item]bool{}
		}
		q.items[item] = true
		q.gen++
	}
	return nil
}

// list returns the quarantined items, in no order.
func (q *quarantine) list() openresponses.Items {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(openresponses.Items, 0, len(q.items))
	for item := range q.items {
		out = append(out, item)
	}
	return out
}

// generation returns the number of additions so far.
func (q *quarantine) generation() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.gen
}

// apply returns items as a request carries them under the quarantine:
// a quarantined function call output with [QuarantinedOutput] in place
// of its output, since its call needs an answer; a quarantined function
// call left out with its outputs; any other quarantined item left out.
// items itself is returned when nothing is quarantined among them.
func (q *quarantine) apply(items openresponses.Items) openresponses.Items {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return items
	}
	held := func(item openresponses.Item) bool {
		return item != nil && reflect.TypeOf(item).Comparable() && q.items[item]
	}
	calls := map[string]bool{}
	for item := range q.items {
		if c, ok := item.(*openresponses.FunctionCall); ok {
			calls[c.CallID] = true
		}
	}
	// An output answers the last call before it with its call ID, so
	// with a provider that reuses call IDs a quarantined call takes its
	// own output and not another call's; an output whose call is not
	// among items goes with any quarantined call of its ID.
	last := map[string]openresponses.Item{}
	var out openresponses.Items
	changed := false
	for i, item := range items {
		sent := item
		out2, isOutput := item.(*openresponses.FunctionCallOutput)
		if c, ok := item.(*openresponses.FunctionCall); ok {
			last[c.CallID] = c
		}
		callHeld := false
		if isOutput {
			if c, ok := last[out2.CallID]; ok {
				callHeld = held(c)
			} else {
				callHeld = calls[out2.CallID]
			}
		}
		switch {
		case callHeld:
			sent = nil
		case held(item) && isOutput:
			placeholder := *out2
			placeholder.Output = openresponses.FunctionCallOutputData{Text: QuarantinedOutput}
			sent = &placeholder
		case held(item):
			sent = nil
		}
		if sent == item && !changed {
			continue
		}
		if !changed {
			out = append(openresponses.Items(nil), items[:i]...)
			changed = true
		}
		if sent != nil {
			out = append(out, sent)
		}
	}
	if !changed {
		return items
	}
	return out
}

// has reports whether item is quarantined itself, or is an output with
// the call ID of a quarantined function call. Given one item, it cannot
// tell which call before it an output answers, so with a provider that
// reuses call IDs it reports another call's output too, which apply,
// pairing an output with the call before it, sends.
func (q *quarantine) has(item openresponses.Item) bool {
	if item == nil {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if reflect.TypeOf(item).Comparable() && q.items[item] {
		return true
	}
	if out, ok := item.(*openresponses.FunctionCallOutput); ok {
		for held := range q.items {
			if c, ok := held.(*openresponses.FunctionCall); ok && c.CallID == out.CallID {
				return true
			}
		}
	}
	return false
}

type quarantineKey struct{}

// quarantineOn returns the quarantine of the run whose hook,
// transform or tool the context belongs to, else the [QuarantineSet]
// a host attached, or nil.
func quarantineOn(ctx context.Context) *quarantine {
	if q, ok := ctx.Value(quarantineKey{}).(*quarantine); ok && q != nil {
		return q
	}
	if s, ok := ctx.Value(quarantineSetKey{}).(*QuarantineSet); ok && s != nil {
		return &s.q
	}
	return nil
}

// QuarantineView is a read-only view of the quarantine a request is built
// under, as the transform's context carries it: a transform that sends
// a model of its own any part of the conversation, such as a fold's
// summary request, leaves out what the quarantine leaves out, and one
// that remembers what it built over the conversation builds it again
// when the quarantine reaches what it remembers (RFC 0001 The request,
// step 3; #281). The zero value quarantines nothing.
type QuarantineView struct{ q *quarantine }

// QuarantineFromContext returns the quarantine of the run whose hook,
// transform or tool ctx belongs to, or the zero view outside a run.
func QuarantineFromContext(ctx context.Context) QuarantineView {
	return QuarantineView{quarantineOn(ctx)}
}

// Has reports whether item is left out of a request or sent as a
// placeholder: quarantined itself, or an output with the call ID of a
// quarantined call. It is given the item alone, so it matches an
// output by its call ID: with a provider that reuses call IDs it also
// reports another call's output under the quarantined call's ID, which
// [QuarantineView.Apply], pairing each output with the call before it,
// sends. A transform that leaves out what Has reports leaves out too
// much there, never too little.
func (v QuarantineView) Has(item openresponses.Item) bool {
	return v.q != nil && v.q.has(item)
}

// Generation counts the items quarantined so far; it changes whenever
// the quarantine grows, so a transform keys what it remembers on it.
func (v QuarantineView) Generation() int {
	if v.q == nil {
		return 0
	}
	return v.q.generation()
}

// Apply returns items as a request carries them under the quarantine:
// a quarantined output with [QuarantinedOutput] in place of its own, a
// quarantined call left out with its outputs, any other quarantined
// item left out; items itself when nothing among them is quarantined.
func (v QuarantineView) Apply(items openresponses.Items) openresponses.Items {
	if v.q == nil {
		return items
	}
	return v.q.apply(items)
}

// QuarantineSet is a conversation's quarantine a host holds across
// runs: [Run] and [Continue] given it on their context with
// [ContextWithQuarantineSet], and an [Agent] given it with
// [WithQuarantineSet], build every request under it, and a hook or
// tool of theirs that quarantines adds to it, so the next run over the
// same conversation, another agent's leg of a handoff included, leaves
// the same items out. A host that serves a conversation over a wire,
// a front starting an agent per task or per request, keeps one per
// conversation and gives each agent the set [QuarantineSet.Bind]
// returns for the transcript it loaded, so a quarantine made in one
// task holds in the next (#281).
type QuarantineSet struct{ q quarantine }

// NewQuarantineSet returns a set quarantining items.
func NewQuarantineSet(items ...openresponses.Item) (*QuarantineSet, error) {
	s := &QuarantineSet{}
	if err := s.q.add(items...); err != nil {
		return nil, err
	}
	return s, nil
}

// Add quarantines items, by identity.
func (s *QuarantineSet) Add(items ...openresponses.Item) error { return s.q.add(items...) }

// Items returns the quarantined items, in no order.
func (s *QuarantineSet) Items() openresponses.Items { return s.q.list() }

// Has reports whether item is left out of a request or sent as a
// placeholder under the set, as [QuarantineView.Has] does, an output
// matched by its call ID alone.
func (s *QuarantineSet) Has(item openresponses.Item) bool { return s.q.has(item) }

// Apply returns items as a request carries them under the set, as
// [QuarantineView.Apply] does: for a front that builds a request itself
// rather than through the loop.
func (s *QuarantineSet) Apply(items openresponses.Items) openresponses.Items {
	return s.q.apply(items)
}

// Bind returns a new set quarantining each item of t that s holds,
// itself or a copy of it, equal in its JSON encoding but for the id
// and status members a relay or a store may set on one copy and not
// another: the set the next run over t builds under, for a host that
// keeps a conversation's quarantine across loads of its transcript
// that decode the items afresh, a store's, or the input a caller sends
// back with each request. An item equal to a quarantined one is
// quarantined whichever copy it is. What the run quarantines is added
// to the set Bind returned, not to s; [QuarantineSet.Merge] keeps it
// in s for the next load.
func (s *QuarantineSet) Bind(t Transcript) *QuarantineSet {
	out := &QuarantineSet{}
	held := s.q.list()
	if len(held) == 0 {
		return out
	}
	keys := map[string]bool{}
	ids := make(map[openresponses.Item]bool, len(held))
	for _, item := range held {
		ids[item] = true
		if key, ok := copyKey(item); ok {
			keys[key] = true
		}
	}
	var bound openresponses.Items
	for _, item := range t {
		if item == nil || !reflect.TypeOf(item).Comparable() {
			continue
		}
		if ids[item] {
			bound = append(bound, item)
			continue
		}
		if key, ok := copyKey(item); ok && keys[key] {
			bound = append(bound, item)
		}
	}
	_ = out.q.add(bound...)
	return out
}

// copyKey is what two copies of one item share: its JSON encoding with
// the id and status members left out, which a relay, a provider or a
// store may set on one copy and not another, and the object's members
// in order.
func copyKey(item openresponses.Item) (string, bool) {
	data, err := json.Marshal(item)
	if err != nil {
		return "", false
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil {
		return "", false
	}
	delete(members, "id")
	delete(members, "status")
	data, err = json.Marshal(members)
	if err != nil {
		return "", false
	}
	return string(data), true
}

// Merge adds to s each item o holds of which s holds no copy, as
// [QuarantineSet.Bind] matches one: what a run under the set [QuarantineSet.Bind]
// returned quarantined, kept, one copy of each, for the next load of
// the conversation.
func (s *QuarantineSet) Merge(o *QuarantineSet) {
	items := o.Items()
	known := s.Bind(items)
	for _, item := range items {
		if !known.Has(item) {
			_ = s.q.add(item)
		}
	}
}

type quarantineSetKey struct{}

// ContextWithQuarantineSet attaches s for [Run] and [Continue], which
// build their requests under it and add to it what their hooks and
// tools quarantine, and for a hook a front calls itself outside a run,
// whose [Quarantine] adds to s.
func ContextWithQuarantineSet(ctx context.Context, s *QuarantineSet) context.Context {
	return context.WithValue(ctx, quarantineSetKey{}, s)
}

// runQuarantine is the quarantine a run of the low-level loop builds
// under: the host's set on ctx, shared, or one of its own, either way
// with what the run ctx belongs to held when this run started.
func runQuarantine(ctx context.Context) *quarantine {
	q := &quarantine{}
	if s, ok := ctx.Value(quarantineSetKey{}).(*QuarantineSet); ok && s != nil {
		q = &s.q
	}
	return inheritQuarantine(ctx, q)
}

// inheritQuarantine adds to q what the quarantine of the run ctx
// belongs to holds, and returns q: a run a tool starts over its
// parent's items, as a child the loop-as-a-tool opens with the
// conversation so far does, leaves out what the parent quarantined.
// It is a copy taken as the child starts: a child that runs on, a
// detached or spawned one, does not see what the parent quarantines
// after that.
func inheritQuarantine(ctx context.Context, q *quarantine) *quarantine {
	if parent := quarantineOn(ctx); parent != nil && parent != q {
		_ = q.add(parent.list()...)
	}
	return q
}

// Quarantine quarantines items for the rest of the conversation of the
// run whose hook or tool ctx belongs to: from the next request on,
// everything a request is built from leaves each out, or carries a
// function call output with [QuarantinedOutput] in place of its own,
// and a function call left out takes its outputs with it. That is the
// request's input, what the transform sends a model of its own, such as
// compact's summary request, and the transcript a tool reads with
// [TranscriptFromContext]; a run a tool starts carries the quarantine
// as it stood when the run started. The transcript keeps every item as
// it was, and so does the record: a session recorder writes the
// quarantine as the format's omit setting, so every later response
// still verifies, and the record and what is exported from it, an ATIF
// trajectory or OTel spans, hold the item as it arrived, being the
// record of the conversation and not the model's input. An item is
// named by identity: the one a hook was given in the request or the
// transcript. A quarantine made in BeforeModelCall applies to the
// request that hook is given, as it returns, and when the transform had
// folded a quarantined item into that request the loop builds the
// request once more. It is how a guard that detects an injected
// instruction keeps it from the model without rewriting the
// append-only transcript (RFC 0001 The request, step 3; #281). It
// returns [ErrNoQuarantine] outside a run.
func Quarantine(ctx context.Context, items ...openresponses.Item) error {
	q := quarantineOn(ctx)
	if q == nil {
		return ErrNoQuarantine
	}
	return q.add(items...)
}

// Quarantine quarantines items of the agent's transcript for the rest
// of its conversation, as [Quarantine] does from inside a run: from the
// next request on, a run in flight's included, each is left out, or a
// function call output sent with [QuarantinedOutput]. The transcript
// keeps them. An item is named by identity, as [Agent.State] hands it.
// Made while no run is active, or after the run's end was delivered,
// it is reported to the subscribers as [QuarantineAdded], at once, or,
// from inside a delivery, right after the event being delivered, so a
// recorder records it before the agent runs again; while a run is
// active the run's next turn_start, or its run_end, carries it. The
// event is delivered under the values of the context the agent's last
// run was started with, so a subscriber that files it by the context,
// a recorder for a child the host started, finds that run's session.
// Called while an event is being delivered, from a subscriber or
// another goroutine, it only owes the event, which that delivery
// reports before it lets the barrier go, right after the event it is
// delivering; otherwise the event is delivered before Quarantine
// returns, and an error a subscriber returns for it is returned.
func (a *Agent) Quarantine(items ...openresponses.Item) error {
	a.mu.Lock()
	gen := a.quarantine.generation()
	if err := a.quarantine.add(items...); err != nil {
		a.mu.Unlock()
		return err
	}
	// A run's events carry what is quarantined while it runs, up to its
	// end; anything else is reported on its own.
	report := a.quarantine.generation() != gen && (!a.running || a.runEnded)
	if report {
		added := slices.DeleteFunc(slices.Clone(items), func(item openresponses.Item) bool { return item == nil })
		a.quarantineOwed = append(a.quarantineOwed, &QuarantineAdded{Items: added, Quarantined: a.quarantine.list()})
	}
	a.mu.Unlock()
	if !report {
		return nil
	}
	// The event is owed before the barrier is tried, so whoever holds
	// it, a delivery under way, the one calling this from a subscriber
	// among them, finds it when it lets the barrier go (unlockEmit).
	if !a.emitMu.TryLock() {
		return nil
	}
	err := a.flushQuarantine(a.eventContext())
	if uerr := a.unlockEmit(); err == nil {
		err = uerr
	}
	return err
}

// eventContext is the context an event that belongs to no run is
// delivered under: the values of the last run's context, lastCtx.
func (a *Agent) eventContext() context.Context {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lastCtx != nil {
		return a.lastCtx
	}
	return context.Background()
}

// unlockEmit lets the delivery barrier go and delivers the
// [QuarantineAdded] events owed since its holder last flushed them:
// one Agent.Quarantine owed while the barrier was held, which it could
// not take. It takes the barrier again only when it is free; when it is
// not, the holder runs this as it lets go, so no owed event is left
// behind. The caller holds a.emitMu.
func (a *Agent) unlockEmit() error {
	for {
		a.emitMu.Unlock()
		a.mu.Lock()
		owed := len(a.quarantineOwed) > 0
		a.mu.Unlock()
		if !owed || !a.emitMu.TryLock() {
			return nil
		}
		if err := a.flushQuarantine(a.eventContext()); err != nil {
			a.emitMu.Unlock()
			return err
		}
	}
}

// flushQuarantine delivers the [QuarantineAdded] events owed. The
// caller holds a.emitMu.
func (a *Agent) flushQuarantine(ctx context.Context) error {
	a.mu.Lock()
	owed := a.quarantineOwed
	a.quarantineOwed = nil
	a.mu.Unlock()
	for _, ev := range owed {
		if err := a.dispatch(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

// Quarantined returns the items quarantined in the agent's
// conversation, in no order.
func (a *Agent) Quarantined() openresponses.Items {
	return a.quarantine.list()
}

// WithQuarantineSet has the agent hold s as its conversation's
// quarantine, rather than one of its own: every run builds its
// requests under s, and [Agent.Quarantine], or a hook or tool of a run
// that quarantines, adds to it. It is how a host that starts an agent
// per task or per request over a conversation it keeps, as front/a2a
// and front/responses do, carries the quarantine from one to the next
// (RFC 0001 The request, step 3; #281). A nil s is ignored.
func WithQuarantineSet(s *QuarantineSet) Option {
	return func(a *Agent) {
		if s != nil {
			a.quarantine = &s.q
		}
	}
}
