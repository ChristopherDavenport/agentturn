package agentturn

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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
	var out openresponses.Items
	changed := false
	for i, item := range items {
		sent := item
		out2, isOutput := item.(*openresponses.FunctionCallOutput)
		switch {
		case isOutput && calls[out2.CallID]:
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

// has reports whether item is quarantined itself, or is the output of
// a quarantined function call, and so is left out of a request.
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

// quarantineFromContext returns the quarantine of the run whose hook,
// transform or tool the context belongs to, or nil.
func quarantineFromContext(ctx context.Context) *quarantine {
	q, _ := ctx.Value(quarantineKey{}).(*quarantine)
	return q
}

// Quarantined is a read-only view of the quarantine a request is built
// under, as the transform's context carries it: a transform that sends
// a model of its own any part of the conversation, such as a fold's
// summary request, leaves out what the quarantine leaves out, and one
// that remembers what it built over the conversation builds it again
// when the quarantine reaches what it remembers (RFC 0001 The request,
// step 3; #281). The zero value quarantines nothing.
type Quarantined struct{ q *quarantine }

// QuarantinedFromContext returns the quarantine of the run whose hook,
// transform or tool ctx belongs to, or the zero view outside a run.
func QuarantinedFromContext(ctx context.Context) Quarantined {
	return Quarantined{quarantineFromContext(ctx)}
}

// Has reports whether item is left out of a request or sent as a
// placeholder: quarantined itself, or the output of a quarantined call.
func (v Quarantined) Has(item openresponses.Item) bool {
	return v.q != nil && v.q.has(item)
}

// Generation counts the items quarantined so far; it changes whenever
// the quarantine grows, so a transform keys what it remembers on it.
func (v Quarantined) Generation() int {
	if v.q == nil {
		return 0
	}
	return v.q.generation()
}

// Apply returns items as a request carries them under the quarantine:
// a quarantined output with [QuarantinedOutput] in place of its own, a
// quarantined call left out with its outputs, any other quarantined
// item left out; items itself when nothing among them is quarantined.
func (v Quarantined) Apply(items openresponses.Items) openresponses.Items {
	if v.q == nil {
		return items
	}
	return v.q.apply(items)
}

// QuarantineSet is a conversation's quarantine a host holds across the
// runs of the low-level loop, as an [Agent] holds its own: [Run] and
// [Continue] given it on their context with [ContextWithQuarantineSet]
// build every request under it, and a hook or tool of theirs that
// quarantines adds to it, so the next run over the same conversation,
// another agent's leg of a handoff included, leaves the same items out.
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

type quarantineSetKey struct{}

// ContextWithQuarantineSet attaches s for [Run] and [Continue], which
// build their requests under it and add to it what their hooks and
// tools quarantine.
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
	if parent := quarantineFromContext(ctx); parent != nil && parent != q {
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
	q := quarantineFromContext(ctx)
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
func (a *Agent) Quarantine(items ...openresponses.Item) error {
	return a.quarantine.add(items...)
}

// Quarantined returns the items quarantined in the agent's
// conversation, in no order.
func (a *Agent) Quarantined() openresponses.Items {
	return a.quarantine.list()
}
