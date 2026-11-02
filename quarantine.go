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

type quarantineKey struct{}

// quarantineFromContext returns the quarantine of the run whose hook or
// tool the context belongs to, or nil.
func quarantineFromContext(ctx context.Context) *quarantine {
	q, _ := ctx.Value(quarantineKey{}).(*quarantine)
	return q
}

// inheritQuarantine adds to q what the quarantine of the run ctx
// belongs to holds, and returns q: a run a tool starts over its
// parent's items, as a child the loop-as-a-tool opens with the
// conversation so far does, leaves out what the parent quarantined.
func inheritQuarantine(ctx context.Context, q *quarantine) *quarantine {
	if parent := quarantineFromContext(ctx); parent != nil && parent != q {
		_ = q.add(parent.list()...)
	}
	return q
}

// Quarantine quarantines items for the rest of the conversation of the
// run whose hook or tool ctx belongs to: from the next request on,
// every request leaves each out, or carries a function call output with
// [QuarantinedOutput] in place of its own, and a function call left out
// takes its outputs with it. The transcript keeps every item as it was,
// and a session recorder writes the quarantine as the format's omit
// setting, so every later response still verifies. An item is named by
// identity: the one a hook was given in the request or a tool in the
// transcript. A quarantine made in BeforeModelCall applies to the
// request that hook is given, as it returns. It is how a guard that
// detects an injected instruction keeps it from the model without
// rewriting the append-only transcript (RFC 0001 The request, step 3;
// #281). It returns [ErrNoQuarantine] outside a run.
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
