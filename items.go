package agentturn

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// markedItem marks an item the caller hands the loop: one the model
// reads and a renderer should not show ([Hidden]), one taken from
// another conversation ([Copied]), one the host composed from text the
// person did not write ([WithProvenance]), or any of them together. It
// is a wrapper the loop strips when it appends the item, so nothing
// downstream sees it: the transcript, the request and every type
// switch on an item work on the item itself, and only the item events
// carry the marks. It marshals
// as the item it wraps, so one that reaches a server or a store anyway
// is the item and not an envelope.
type markedItem struct {
	item   openresponses.Item
	hidden bool
	copied bool
	// prov is held by pointer so the wrapper stays comparable.
	prov *Provenance
}

// ItemType returns the wrapped item's type.
func (h markedItem) ItemType() string { return h.item.ItemType() }

// MarshalJSON encodes the wrapped item.
func (h markedItem) MarshalJSON() ([]byte, error) { return json.Marshal(h.item) }

// Hidden marks item as part of the model's context that a renderer
// should hide: a stream rule's interrupt report, an advisory, a notice
// a keyword added, a subagent's result. The model still sees it, the
// transcript holds it and a request carries it; only its item events
// say it is hidden, and a session recorder writes its entry with
// visible false, which is the format's word for exactly this. A hidden
// message is not what the person typed, which they see: a session
// recorder writes it without the trigger kind [TriggerHuman] (#303).
//
// Use it wherever the loop appends an item the caller supplied:
// [Agent.Prompt], [Agent.Steer], [Agent.FollowUp], the prompts of [Run]
// and the items [Config.BeforeTurn] returns. The loop unwraps it as it
// appends, so nothing downstream has to know the wrapper exists; a
// hidden item still in a queue, which [Agent.State] hands back, is
// wrapped, so a host that re-queues what it read keeps the mark and one
// that type-switches on it should call [Unhide] first.
//
// An item the filter in force hides from the model is a different
// thing: it is out of the model's context, a recorder writes it as a
// custom entry, and marking it hidden adds nothing.
func Hidden(item openresponses.Item) openresponses.Item {
	if item == nil {
		return nil
	}
	m, _ := item.(markedItem)
	if m.item == nil {
		m.item = item
	}
	m.hidden = true
	return m
}

// Unhide returns the item [Hidden], [Copied] or [WithProvenance]
// wrapped and whether it was hidden. An item never marked comes back unchanged.
func Unhide(item openresponses.Item) (openresponses.Item, bool) {
	if m, ok := item.(markedItem); ok {
		return m.item, m.hidden
	}
	return item, false
}

// Copied marks item as taken from another conversation rather than
// produced in this one: a child prompted with its parent's
// conversation, as tools/agent's WithCallConfig does. The model reads
// it as any item; the loop carries the mark unread on its item events
// ([ItemEnd.Copied]), and a session recorder writes the item's entry
// with agentsession RFC 0001's copied_from, naming where it came from,
// and writes no decision for a call so marked or its output, since the
// call is not this session's. Use it where [Hidden] may be used; the
// marks compose, and [Unhide] strips them all. A copy's trust is the
// trust of what it copies, and a [Provenance] trust on a copied item
// adds its labels to it; see [Provenance.Trust].
func Copied(item openresponses.Item) openresponses.Item {
	if item == nil {
		return nil
	}
	m, _ := item.(markedItem)
	if m.item == nil {
		m.item = item
	}
	m.copied = true
	return m
}

// IsCopied reports whether item carries the [Copied] mark, for a host
// or a recorder that takes an input before the loop does, as
// agentturn/session's Recorder.Queue does.
func IsCopied(item openresponses.Item) bool {
	_, _, copied := marks(item)
	return copied
}

// Provenance says where an item the host or a hook composed came from
// and whether it is trusted, for text the person did not write that is
// added to the conversation as a message: an instruction file a hook
// reached mid-session, a prompt template, an MCP server's prompt or
// resource, context a hook composed. Without it the record reads such
// text as the person's words. The loop carries it unread on the item's
// events ([ItemStart.Provenance], [ItemEnd.Provenance],
// [Queued.Provenance]), and a session recorder writes it on the item's
// entry in agentsession RFC 0001's members: Layers as
// instructions_part, Source as source and Trust as trust (RFC 0001 The
// record).
type Provenance struct {
	// Layers names the instruction layers a message was composed from,
	// in the order their texts appear in it: an instruction file wrapped
	// in a reminder, a template, a memory block. A message composed from
	// a layer is not an input of its run: a replay does not send it
	// again as the person's. Only a message may name layers.
	Layers []Layer
	// Source is what sent the item, in a trigger's terms: the MCP server
	// whose prompt it is, the hook that composed it. It stands over the
	// trigger the item is handed in under, for this item alone: the
	// item's queued report carries it as its trigger, and a recorder
	// writes it as the item's source, where the run's trigger would be.
	// Zero leaves the item the trigger it is handed in under, though a
	// session recorder writes one of kind [TriggerHuman] without its
	// kind as the source of a message composed from Layers: what the
	// host composed is not the person's words (#303). A host that
	// composed the message names its own kind here.
	Source Trigger
	// Trust is what the host knows of the content's trust; nil says
	// nothing. On an item also marked [Copied] a session recorder
	// merges its labels into the trust the copy carries from what it
	// copies, since agentsession's format 0.21 lets a copy's trust hold
	// more labels than its source's, never fewer (agentsession#279); an
	// empty list adds nothing there.
	Trust *Trust
}

// Layer names an instruction layer a message was composed from, as an
// instructions part names one (agentsession RFC 0001, config,
// Instructions as parts): Source is the layer, "agentsmd" for an
// instruction file, "agentmemory" for memory or one in the host's own
// terms such as "dax.prompts", and ID the part of it, the file's path
// for an instruction file. Source is required, and so is ID for an
// instruction file, which has a part per file.
type Layer struct {
	ID     string
	Source string
}

// Trust labels what an item's content holds, in the labels a tool's
// [agenttool.Flow] reads use: [agenttool.LabelUntrusted] for text an
// outside party wrote, a repository's or an MCP server's,
// [agenttool.LabelPrivate] for what must not leave the session. An
// empty Reads says the host judged the content and found neither. A
// recorder writes it as agentsession RFC 0001's trust.
type Trust struct {
	Reads []agenttool.Label
}

// ErrProvenance is returned by [WithProvenance] for a provenance the
// record cannot hold.
var ErrProvenance = errors.New("agentturn: provenance cannot be recorded")

// WithProvenance marks item with where it came from and whether it is
// trusted, for an item the host or a hook composes from text the person
// did not write; see [Provenance]. Use it where [Hidden] may be used:
// [Agent.Prompt], [Agent.Steer], [Agent.FollowUp], [Agent.Queue],
// [Agent.Deliver], the prompts of [Run] and the items
// [Config.BeforeTurn] returns. The marks compose, a second provenance
// replaces the first, and [Unhide] strips them all.
//
// It refuses, with [ErrProvenance], layers on an item that is not a
// message, a layer with no source, an instruction file's with no ID, a
// layer named twice, an empty or repeated trust label, a source
// [Trigger.Validate] refuses, and a source of kind [TriggerHuman]
// beside layers, since a message composed from layers is not the
// person's words. A provenance that says nothing returns
// item as it is, and a nil item nil.
func WithProvenance(item openresponses.Item, p Provenance) (openresponses.Item, error) {
	if item == nil {
		return nil, nil
	}
	if len(p.Layers) == 0 && p.Source.IsZero() && p.Trust == nil {
		return item, nil
	}
	m, _ := item.(markedItem)
	if m.item == nil {
		m.item = item
	}
	if err := p.validate(m.item); err != nil {
		return nil, err
	}
	m.prov = p.clone()
	return m, nil
}

// ProvenanceOf returns the provenance [WithProvenance] marked item
// with, and whether it has one.
func ProvenanceOf(item openresponses.Item) (Provenance, bool) {
	if m, ok := item.(markedItem); ok && m.prov != nil {
		return *m.prov.clone(), true
	}
	return Provenance{}, false
}

// validate checks p for an item, the format's rules for
// instructions_part and trust.
func (p Provenance) validate(item openresponses.Item) error {
	if len(p.Layers) > 0 {
		if _, ok := item.(*openresponses.Message); !ok {
			return fmt.Errorf("%w: a %s item names instruction layers; only a message may", ErrProvenance, item.ItemType())
		}
	}
	if len(p.Layers) > 0 && p.Source.Kind == TriggerHuman {
		return fmt.Errorf("%w: a message composed from instruction layers names a source of kind %q, which says a person wrote it", ErrProvenance, TriggerHuman)
	}
	for i, l := range p.Layers {
		switch {
		case l.Source == "":
			return fmt.Errorf("%w: layer %d has no source", ErrProvenance, i)
		case l.ID == "" && l.Source == "agentsmd":
			return fmt.Errorf("%w: instruction file layer %d has no id", ErrProvenance, i)
		}
		for _, o := range p.Layers[:i] {
			if o.ID == l.ID && (l.ID != "" || o.Source == l.Source) {
				return fmt.Errorf("%w: layer %d names %q again", ErrProvenance, i, l.ID+l.Source)
			}
		}
	}
	if p.Trust != nil {
		for i, l := range p.Trust.Reads {
			if l == "" || slices.Contains(p.Trust.Reads[:i], l) {
				return fmt.Errorf("%w: trust label %q is empty or repeated", ErrProvenance, l)
			}
		}
	}
	if err := p.Source.Validate(); err != nil {
		return fmt.Errorf("%w: source: %w", ErrProvenance, err)
	}
	return nil
}

// clone returns a copy of p holding lists of its own, so neither the
// caller nor a subscriber shares one with the mark.
func (p *Provenance) clone() *Provenance {
	if p == nil {
		return nil
	}
	out := &Provenance{Layers: slices.Clone(p.Layers), Source: p.Source}
	if p.Source.Extra != nil {
		out.Source.Extra = make(map[string]any, len(p.Source.Extra))
		for k, v := range p.Source.Extra {
			out.Source.Extra[k] = v
		}
	}
	if p.Trust != nil {
		out.Trust = &Trust{Reads: slices.Clone(p.Trust.Reads)}
		if out.Trust.Reads == nil {
			out.Trust.Reads = []agenttool.Label{}
		}
	}
	return out
}

// provenance returns a copy of the provenance item is marked with, nil
// for none, for an event to carry.
func provenance(item openresponses.Item) *Provenance {
	if m, ok := item.(markedItem); ok {
		return m.prov.clone()
	}
	return nil
}

// marks returns the item the marks wrap, and the marks.
func marks(item openresponses.Item) (base openresponses.Item, hidden, copied bool) {
	if m, ok := item.(markedItem); ok {
		return m.item, m.hidden, m.copied
	}
	return item, false, false
}

// unhideAll returns items with every hidden mark stripped, and whether
// any were, so a check that type-switches on the items sees them.
func unhideAll(items openresponses.Items) openresponses.Items {
	var out openresponses.Items
	for i, item := range items {
		// The marks are checked by type: an item type that cannot be
		// compared would panic an equality test.
		m, marked := item.(markedItem)
		if !marked {
			continue
		}
		base := m.item
		if out == nil {
			out = append(openresponses.Items(nil), items...)
		}
		out[i] = base
	}
	if out == nil {
		return items
	}
	return out
}
