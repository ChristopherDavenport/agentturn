// Package outputslots finds, in an [openresponses.Accumulator], the item
// an output index names.
//
// A stream that follows the lifecycle keeps each item at its own output
// index, and the accumulator's Output holds an item at its index. A
// stream that reuses an index after closing the item there, as Ollama
// 0.23 does for parallel function calls, has every item kept: the
// accumulator appends the later item, and from then on a position in
// Output is no longer an output index. It keeps the map from one to the
// other to itself and reports only that a reuse happened, so a reader
// that must know which item an event names follows the map here, from
// the same two signals: the number of reuses it reports, and that an
// item it does not place at its index is appended.
package outputslots

import "github.com/ChristopherDavenport/openresponses"

// Slots maps the output indexes of one stream to positions in the
// accumulator's Output. The zero value is ready; a stream has one.
type Slots struct {
	at     map[int]int
	reused int
}

// Observe records the position the event's item took in acc, and must
// be called with every event of the stream after acc.Add has taken it.
// Only an output_item.added and an output_item.done place an item.
func (s *Slots) Observe(ev openresponses.StreamEvent, acc *openresponses.Accumulator) {
	var idx int
	switch e := ev.(type) {
	case *openresponses.OutputItemAddedEvent:
		idx = e.OutputIndex
	case *openresponses.OutputItemDoneEvent:
		idx = e.OutputIndex
	default:
		return
	}
	if idx < 0 {
		return
	}
	reused := len(acc.ReusedIndexes())
	pos, known := s.at[idx]
	switch {
	case reused > s.reused || !known && reused > 0:
		// The item was appended, behind everything accumulated.
		if cur := acc.Response(); cur != nil {
			pos = len(cur.Output) - 1
		}
	case !known:
		pos = idx
	}
	if s.at == nil {
		s.at = map[int]int{}
	}
	s.at[idx], s.reused = pos, reused
}

// Position returns the position in acc's Output of the item events at
// output index idx name, and false when the stream has opened none
// there.
func (s *Slots) Position(idx int, acc *openresponses.Accumulator) (int, bool) {
	cur := acc.Response()
	if cur == nil || idx < 0 {
		return 0, false
	}
	pos, known := s.at[idx]
	if !known {
		if s.reused > 0 {
			return 0, false
		}
		pos = idx
	}
	return pos, pos < len(cur.Output)
}

// Item returns the item events at output index idx name, or nil when
// the stream has opened none there.
func (s *Slots) Item(idx int, acc *openresponses.Accumulator) openresponses.Item {
	pos, ok := s.Position(idx, acc)
	if !ok {
		return nil
	}
	return acc.Response().Output[pos]
}
