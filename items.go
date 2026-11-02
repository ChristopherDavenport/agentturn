package agentturn

import (
	"encoding/json"

	"github.com/ChristopherDavenport/openresponses"
)

// markedItem marks an item the caller hands the loop: one the model
// reads and a renderer should not show ([Hidden]), one taken from
// another conversation ([Copied]), or both. It is a wrapper the loop
// strips when it appends the item, so nothing downstream sees it: the
// transcript, the request and every type switch on an item work on the
// item itself, and only the item events carry the marks. It marshals
// as the item it wraps, so one that reaches a server or a store anyway
// is the item and not an envelope.
type markedItem struct {
	item   openresponses.Item
	hidden bool
	copied bool
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
// visible false, which is the format's word for exactly this.
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

// Unhide returns the item [Hidden] or [Copied] wrapped and whether it
// was hidden. An item never marked comes back unchanged.
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
// two compose, and [Unhide] strips both.
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
		base, _, _ := marks(item)
		if base == item {
			continue
		}
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
