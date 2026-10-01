package agentturn

import (
	"context"

	"github.com/ChristopherDavenport/openresponses"
)

// ReasoningModels says which [Config.ModelName] produced each reasoning
// item it holds. A reasoning item carries state only its own provider
// reads, a signature it issued among it, and another provider refuses
// it, so the loop leaves out of each request the reasoning items
// another model produced. The transcript keeps them, and so does a
// record of it.
//
// The loop fills it as models answer, so a configuration replaced
// between runs with [Agent.SetConfig] finds the previous model's items
// attributed. A transcript the loop has not seen, rebuilt from a store
// or sent by a caller, has no attribution until a host gives it, with
// [WithReasoningModels] or [ContextWithReasoningModels]. A reasoning
// item it does not hold is sent, as every one was before the loop
// attributed them. Items are held by identity: a copy of an item, such
// as one a transform made, is not the item it copied.
type ReasoningModels map[*openresponses.ReasoningItem]string

// Attribute records modelName as the producer of each reasoning item
// in items that m does not hold yet. An empty modelName, the model
// adapter's default, names no model and records nothing. m must not be
// nil.
func (m ReasoningModels) Attribute(modelName string, items openresponses.Items) {
	if modelName == "" {
		return
	}
	for _, item := range items {
		if r, ok := item.(*openresponses.ReasoningItem); ok {
			if _, known := m[r]; !known {
				m[r] = modelName
			}
		}
	}
}

// For returns t as modelName is to be sent it: without the reasoning
// items m attributes to another model. The model's own reasoning items
// stay, since a provider may require them back, as Anthropic does the
// thinking block of a turn that called a tool, and so do the ones m
// does not hold. An empty modelName drops nothing. t itself is returned
// when nothing is dropped.
func (m ReasoningModels) For(modelName string, t Transcript) Transcript {
	if modelName == "" || len(m) == 0 {
		return t
	}
	var out Transcript
	dropped := false
	for i, item := range t {
		if r, ok := item.(*openresponses.ReasoningItem); ok {
			if by, known := m[r]; known && by != modelName {
				if !dropped {
					out = append(Transcript(nil), t[:i]...)
					dropped = true
				}
				continue
			}
		}
		if dropped {
			out = append(out, item)
		}
	}
	if !dropped {
		return t
	}
	return out
}

// merged returns a new map holding m's entries with more's over them.
func (m ReasoningModels) merged(more ReasoningModels) ReasoningModels {
	out := make(ReasoningModels, len(m)+len(more))
	for r, name := range m {
		out[r] = name
	}
	for r, name := range more {
		out[r] = name
	}
	return out
}

// retain returns the entries of m whose item t holds, or nil when none
// are left.
func (m ReasoningModels) retain(t Transcript) ReasoningModels {
	var out ReasoningModels
	for _, item := range t {
		if r, ok := item.(*openresponses.ReasoningItem); ok {
			if name, known := m[r]; known {
				if out == nil {
					out = ReasoningModels{}
				}
				out[r] = name
			}
		}
	}
	return out
}

type reasoningKey struct{}

// ContextWithReasoningModels attaches m, over any an outer context
// attached, for [Run] and [Continue], whose transcript the loop has not
// seen, and for an [Agent]'s run over what the agent already knows. The
// run reads a copy and adds the reasoning its own model produces.
func ContextWithReasoningModels(ctx context.Context, m ReasoningModels) context.Context {
	if len(m) == 0 {
		return ctx
	}
	return context.WithValue(ctx, reasoningKey{}, reasoningFromContext(ctx).merged(m))
}

// reasoningFromContext returns what [ContextWithReasoningModels]
// attached to ctx, or nil.
func reasoningFromContext(ctx context.Context) ReasoningModels {
	m, _ := ctx.Value(reasoningKey{}).(ReasoningModels)
	return m
}

// WithReasoningModels says which model produced the reasoning items of
// the transcript [WithTranscript] gives, from what the host knows of
// it, so the loop leaves another model's out of each request as it
// does for items its own runs produced. A front that rebuilds a
// conversation from a store or from a caller's input attributes them
// to the configurations that had it.
func WithReasoningModels(m ReasoningModels) Option {
	return func(a *Agent) { a.reasoning = a.reasoning.merged(m) }
}

// attribute records the run's model as the producer of the reasoning
// items among items.
func (r *runner) attribute(items openresponses.Items) {
	if r.reasoning == nil {
		r.reasoning = ReasoningModels{}
	}
	r.reasoning.Attribute(r.cfg.ModelName, items)
}
