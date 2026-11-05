package agentturn

import (
	"context"
	"maps"

	"github.com/ChristopherDavenport/openresponses"
)

// PersonMessages names the messages of a transcript a person wrote:
// the user messages the loop appended from a prompt, a steer, a
// follow-up or a delivery ([Agent.Deliver], [Agent.Accept]), a person's
// steer a view forwarded among them, that came in under a trigger of
// kind [TriggerHuman] and
// that the host did not compose, copy or hide. A message composed from
// instruction layers, one marked [Copied] or [Hidden], and one whose
// [Provenance] names a source of another kind are not the person's,
// as a session recorder does not write them as the person's (#303).
// The transform reads them from its context
// ([PersonMessagesFromContext]), so a fold keeps what the person asked
// for rather than a summary's paraphrase of it, as compact's reference
// fold does (RFC 0001 The request, step 2; #298).
//
// The loop fills it as it appends the person's input, so a
// configuration replaced between runs finds the earlier runs' messages
// named. A transcript the loop has not seen, rebuilt from a store or
// sent by a caller, names none until a host gives them, with
// [WithPersonMessages], [Agent.SetPersonMessages] or
// [ContextWithPersonMessages]; agentturn/session reads them from a
// record. Messages are held by identity: a copy of a message, such as
// one a transform made, is not the message it copied.
type PersonMessages map[*openresponses.Message]bool

// Has reports whether item is one of the messages m names.
func (m PersonMessages) Has(item openresponses.Item) bool {
	msg, ok := item.(*openresponses.Message)
	return ok && m[msg]
}

// merged returns a new set holding m's messages and more's.
func (m PersonMessages) merged(more PersonMessages) PersonMessages {
	out := make(PersonMessages, len(m)+len(more))
	for msg, ok := range m {
		if ok {
			out[msg] = true
		}
	}
	for msg, ok := range more {
		if ok {
			out[msg] = true
		}
	}
	return out
}

// retain returns the messages of m that t holds, or nil when none are
// left.
func (m PersonMessages) retain(t Transcript) PersonMessages {
	var out PersonMessages
	for _, item := range t {
		if m.Has(item) {
			if out == nil {
				out = PersonMessages{}
			}
			out[item.(*openresponses.Message)] = true
		}
	}
	return out
}

// add names item, which the loop appended as the run's input under
// trigger, when it is the person's own message: a user message under a
// trigger of kind [TriggerHuman], not copied, not hidden and composed
// from no instruction layers. trigger is the one the item's record
// names, its provenance's source when it has one. m must not be nil.
func (m PersonMessages) add(item openresponses.Item, trigger Trigger, prov *Provenance, hidden, copied bool) {
	msg, ok := item.(*openresponses.Message)
	if !ok || msg.Role != openresponses.RoleUser || trigger.Kind != TriggerHuman || hidden || copied {
		return
	}
	if prov != nil && len(prov.Layers) > 0 {
		return
	}
	m[msg] = true
}

type personsKey struct{}

// ContextWithPersonMessages attaches m, over any an outer context
// attached, for [Run] and [Continue], whose transcript the loop has not
// seen, and for an [Agent]'s run over what the agent already knows. The
// run reads a copy and adds the person's messages it appends. The loop
// attaches the run's own to the transform's context, and none to a
// hook's or a tool's beyond that: a run a tool starts over the parent's
// items is prompted with copies, which are not the person's.
func ContextWithPersonMessages(ctx context.Context, m PersonMessages) context.Context {
	if len(m) == 0 {
		return ctx
	}
	return context.WithValue(ctx, personsKey{}, personsFromContext(ctx).merged(m))
}

// PersonMessagesFromContext returns a copy of the messages
// [ContextWithPersonMessages] attached to ctx, nil for none: on a
// transform's context, the person's own messages among the transcript
// it is given (RFC 0001 The request, step 2).
func PersonMessagesFromContext(ctx context.Context) PersonMessages {
	m := personsFromContext(ctx)
	if len(m) == 0 {
		return nil
	}
	return maps.Clone(m)
}

// personsFromContext returns what [ContextWithPersonMessages] attached
// to ctx, or nil, not copied.
func personsFromContext(ctx context.Context) PersonMessages {
	m, _ := ctx.Value(personsKey{}).(PersonMessages)
	return m
}

// withoutPersons is ctx with no person's messages attached, for a hook
// or a tool.
func withoutPersons(ctx context.Context) context.Context {
	if personsFromContext(ctx) == nil {
		return ctx
	}
	return context.WithValue(ctx, personsKey{}, PersonMessages(nil))
}

// WithPersonMessages says which messages of the transcript
// [WithTranscript] gives a person wrote, from what the host knows of
// it, so a transform keeps them as it keeps those the agent's own runs
// appended. agentturn/session's AgentOptions gives them from a record.
func WithPersonMessages(m PersonMessages) Option {
	return func(a *Agent) { a.persons = a.persons.merged(m) }
}

// SetPersonMessages says which messages of the transcript a person
// wrote, as [WithPersonMessages] does for a new agent: after
// [Agent.SetTranscript] to a branch of a session, with what the session
// package reads there. What it says is added to what the agent knows.
// It returns [ErrRunning] while a run is active.
func (a *Agent) SetPersonMessages(m PersonMessages) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.running {
		return ErrRunning
	}
	a.persons = a.persons.merged(m)
	return nil
}
