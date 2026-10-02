package outputslots

import (
	"testing"

	"github.com/ChristopherDavenport/openresponses"
)

func call(id, callID, args string) *openresponses.FunctionCall {
	return &openresponses.FunctionCall{ID: id, CallID: callID, Name: "upper", Arguments: args}
}

func added(idx int, item openresponses.Item) openresponses.StreamEvent {
	return &openresponses.OutputItemAddedEvent{OutputIndex: idx, Item: item}
}

func done(idx int, item openresponses.Item) openresponses.StreamEvent {
	return &openresponses.OutputItemDoneEvent{OutputIndex: idx, Item: item}
}

func delta(idx int, d string) openresponses.StreamEvent {
	return &openresponses.FunctionCallArgumentsDeltaEvent{OutputIndex: idx, Delta: d}
}

// TestSlotsFollowTheAccumulator pins Item against what each event of a
// stream names, on streams that keep their indexes apart and on ones
// that reuse one: after every event that names an index, the item the
// slots give is the one the stream means, by the arguments the deltas
// built up on it.
func TestSlotsFollowTheAccumulator(t *testing.T) {
	type step struct {
		ev openresponses.StreamEvent
		// at is the index the event names; want the arguments the item
		// there holds once the event is taken, or "" for no item.
		at   int
		want string
	}
	cases := []struct {
		name  string
		steps []step
	}{
		{name: "own indexes", steps: []step{
			{added(0, call("a", "ca", "")), 0, ""},
			{delta(0, "x"), 0, "x"},
			{done(0, call("a", "ca", "x")), 0, "x"},
			{added(1, call("b", "cb", "")), 1, ""},
			{delta(1, "y"), 1, "y"},
			{done(1, call("b", "cb", "y")), 1, "y"},
			{delta(0, ""), 0, "x"},
		}},
		{name: "one index, item IDs", steps: []step{
			{added(0, call("a", "ca", "")), 0, ""},
			{delta(0, "x"), 0, "x"},
			{done(0, call("a", "ca", "x")), 0, "x"},
			{added(0, call("b", "cb", "")), 0, ""},
			{delta(0, "y"), 0, "y"},
			{done(0, call("b", "cb", "y")), 0, "y"},
			{added(0, call("c", "cc", "")), 0, ""},
			{delta(0, "z"), 0, "z"},
			{done(0, call("c", "cc", "z")), 0, "z"},
		}},
		{name: "one index, no item IDs", steps: []step{
			{added(0, call("", "ca", "")), 0, ""},
			{delta(0, "x"), 0, "x"},
			{done(0, call("", "ca", "x")), 0, "x"},
			{added(0, call("", "cb", "")), 0, ""},
			{delta(0, "y"), 0, "y"},
			{done(0, call("", "cb", "y")), 0, "y"},
		}},
		{name: "a reused index beside one that is not", steps: []step{
			{added(0, call("a", "ca", "")), 0, ""},
			{done(0, call("a", "ca", "x")), 0, "x"},
			{added(0, call("b", "cb", "")), 0, ""},
			{delta(0, "y"), 0, "y"},
			// A first use after a reuse lands behind what is kept.
			{added(1, call("c", "cc", "")), 1, ""},
			{delta(1, "z"), 1, "z"},
			{delta(0, "y"), 0, "yy"},
			{done(0, call("b", "cb", "yy")), 0, "yy"},
			{done(1, call("c", "cc", "zz")), 1, "zz"},
			{added(0, call("d", "cd", "")), 0, ""},
			{delta(0, "w"), 0, "w"},
			{delta(1, "!"), 1, "zz!"},
		}},
		{name: "done alone", steps: []step{
			{done(0, call("a", "ca", "x")), 0, "x"},
			{done(0, call("b", "cb", "y")), 0, "y"},
			{done(1, call("c", "cc", "z")), 1, "z"},
		}},
		{name: "an index that was never opened", steps: []step{
			{added(0, call("a", "ca", "")), 0, ""},
			{done(0, call("a", "ca", "x")), 0, "x"},
			{added(0, call("b", "cb", "")), 0, ""},
			{delta(3, "?"), 3, ""},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var acc openresponses.Accumulator
			var s Slots
			acc.Add(&openresponses.ResponseCreatedEvent{Response: &openresponses.Response{ID: "resp"}})
			for i, st := range tc.steps {
				acc.Add(st.ev)
				s.Observe(st.ev, &acc)
				got := ""
				if fc, ok := s.Item(st.at, &acc).(*openresponses.FunctionCall); ok {
					got = fc.Arguments
				}
				if got != st.want {
					t.Errorf("step %d (%s at %d): item holds %q, want %q; reused %v", i, st.ev.EventType(), st.at, got, st.want, acc.ReusedIndexes())
				}
			}
		})
	}
}
