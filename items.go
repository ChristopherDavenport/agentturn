package agentturn

import (
	"encoding/json"

	"github.com/ChristopherDavenport/openresponses"
)

// hiddenItem marks an item the model reads and a renderer should not
// show. It is a wrapper the loop strips when it appends the item, so
// nothing downstream sees it: the transcript, the request and every
// type switch on an item work on the item itself. It marshals as the
// item it wraps, so one that reaches a server or a store anyway is the
// item and not an envelope.
type hiddenItem struct{ item openresponses.Item }

// ItemType returns the wrapped item's type.
func (h hiddenItem) ItemType() string { return h.item.ItemType() }

// MarshalJSON encodes the wrapped item.
func (h hiddenItem) MarshalJSON() ([]byte, error) { return json.Marshal(h.item) }

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
	if _, ok := item.(hiddenItem); ok {
		return item
	}
	return hiddenItem{item: item}
}

// Unhide returns the item [Hidden] wrapped and whether it was hidden.
// An item that was never hidden comes back unchanged.
func Unhide(item openresponses.Item) (openresponses.Item, bool) {
	if h, ok := item.(hiddenItem); ok {
		return h.item, true
	}
	return item, false
}

// unhideAll returns items with every hidden mark stripped, and whether
// any were, so a check that type-switches on the items sees them.
func unhideAll(items openresponses.Items) openresponses.Items {
	var out openresponses.Items
	for i, item := range items {
		base, hidden := Unhide(item)
		if !hidden {
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
