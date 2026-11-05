package agent

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestWithRecordContinuesOnlyTheKeyedChild pins the tool's side of
// #297: under WithRecord the tool reports itself keyed, except a
// detached one; a call with no key, or one the loader finds nothing
// for, starts afresh marked a retry; a loader's error fails the call
// before anything runs; and a child still holding a call its policy
// held, with nobody to ask, fails input-required naming it without
// running.
func TestWithRecordContinuesOnlyTheKeyedChild(t *testing.T) {
	cfg := agentturn.Config{Name: "child", Model: &echo.Adapter{}}
	args := json.RawMessage(`{"input":"hi"}`)
	if r := agenttool.ReplayOf(context.Background(), New(cfg), args); r != agenttool.ReplayUnknown {
		t.Errorf("without WithRecord replay = %v, want unknown", r)
	}
	load := func(context.Context, agenttool.Call, agentturn.Config) (*Record, error) { return nil, nil }
	if r := agenttool.ReplayOf(context.Background(), New(cfg, WithRecord(load)), args); r != agenttool.ReplayKeyed {
		t.Errorf("with WithRecord replay = %v, want keyed", r)
	}
	detached := New(cfg, WithRecord(load), WithDetach(func(string, *agentturn.RunEnd) {}))
	if r := agenttool.ReplayOf(context.Background(), detached, args); r != agenttool.ReplayUnknown {
		t.Errorf("detached replay = %v, want unknown", r)
	}

	held := &openresponses.FunctionCall{CallID: "c1", Name: "lookup", Arguments: `{}`}
	for _, tc := range []struct {
		name    string
		key     string
		rec     *Record
		err     error
		loaded  bool
		retry   bool
		input   bool
		failed  bool
		started bool
	}{
		{name: "no key", retry: true, started: true},
		{name: "nothing recorded", key: "k", loaded: true, retry: true, started: true},
		{name: "a loader's error", key: "k", err: errors.New("disk gone"), loaded: true, failed: true},
		{name: "held, nobody to ask", key: "k", loaded: true, input: true,
			rec: &Record{RunID: "run_1", Pending: []agentturn.PendingCall{{Call: held, Reason: agentturn.PendingDeferred, Decision: &agentturn.ToolDecision{Action: agentturn.Defer}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loaded, started, retry := false, false, false
			tool := New(cfg, WithRecord(func(_ context.Context, call agenttool.Call, _ agentturn.Config) (*Record, error) {
				loaded = call.IdempotencyKey == tc.key
				return tc.rec, tc.err
			}), WithObserver(func(ctx context.Context, ev agentturn.Event) {
				if _, ok := ev.(*agentturn.RunStart); ok {
					started, retry = true, RetryFromContext(ctx)
				}
			}))
			res, err := tool.Execute(context.Background(), agenttool.Call{ID: "call_1", Args: args, IdempotencyKey: tc.key})
			if loaded != tc.loaded || started != tc.started || retry != tc.retry {
				t.Errorf("loaded %v, started %v, retry %v; want %v, %v, %v", loaded, started, retry, tc.loaded, tc.started, tc.retry)
			}
			if tc.failed != (err != nil && !errors.Is(err, agentturn.ErrInputRequired)) {
				t.Errorf("err = %v, want failed %v", err, tc.failed)
			}
			var waiting agentturn.InputRequired
			if tc.input != errors.As(err, &waiting) {
				t.Fatalf("err = %v, want input required %v", err, tc.input)
			}
			if tc.input {
				if calls := waiting.PendingCalls(); len(calls) != 1 || calls[0].ID != "c1" || calls[0].Callee != "run_1" {
					t.Errorf("waiting = %+v, want c1 on run_1", calls)
				}
				if info, _ := res.Details.(ChildInfo); info.RunID != "run_1" || info.Reason != agentturn.ReasonInputRequired {
					t.Errorf("details = %+v", res.Details)
				}
			}
		})
	}
}
