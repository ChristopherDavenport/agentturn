package session

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// TestACallThatReturnedBeforeACrashIsNotRunAgain pins RFC 0001's
// tool_end row and the replay rule (#273): a call of a parallel batch
// whose tool returns while another still runs has its output recorded
// as a result, durable before its tool_end returns, since the batch
// appends the outputs only once every call has settled. When the
// process dies in between, a resume reads the call as returned and
// answers it with that output, so its tool runs once. A batch of one
// call, whose output joins as soon as it settles, writes no result.
func TestACallThatReturnedBeforeACrashIsNotRunAgain(t *testing.T) {
	t.Run("a parallel batch cut by a crash", func(t *testing.T) {
		ctx := context.Background()
		store := &cutStore{Store: agentsession.NewMemoryStore()}
		rec, s, err := Start(ctx, store, agentsession.Header{})
		if err != nil {
			t.Fatal(err)
		}
		var fetches, sends atomic.Int32
		fetched := make(chan struct{})
		tools := []agenttool.Tool{
			agenttool.New("fetch_page", "", func(context.Context, echoArgs) (string, error) {
				fetches.Add(1)
				return "page", nil
			}, agenttool.WithReplay(func(context.Context, json.RawMessage) agenttool.Replay { return agenttool.ReplaySafe })),
			agenttool.New("send_email", "", func(context.Context, echoArgs) (string, error) {
				if sends.Add(1) == 1 {
					<-fetched
					store.cut.Store(true) // the process dies while send_email runs
				}
				return "sent", nil
			}),
		}
		cfg := agentturn.Config{Model: allCalls{}, ModelName: "m", MaxTurns: 1, Tools: tools}
		a := agentturn.New(cfg)
		unsub := rec.Attach(a)
		a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
			if e, ok := ev.(*agentturn.ToolEnd); ok && e.Name == "fetch_page" {
				close(fetched)
			}
			return nil
		})
		if _, err := a.Prompt(ctx, openresponses.UserText("go")); err == nil {
			t.Fatal("the run survived the crash")
		}
		unsub()
		store.cut.Store(false)

		rec, s, err = Resume(ctx, store, s.ID())
		if err != nil {
			t.Fatal(err)
		}
		pending, err := Pending(s)
		if err != nil {
			t.Fatal(err)
		}
		fetchID, sendID := "", ""
		for _, p := range pending {
			switch p.Call.Name {
			case "fetch_page":
				fetchID = p.Call.CallID
				if p.Reason != agentturn.PendingReason("returned") || p.MayHaveRun() || p.Ran == nil || p.Ran.Output.Text != "page" {
					t.Errorf("fetch_page pending = %+v, want it returned with its output", p)
				}
			case "send_email":
				sendID = p.Call.CallID
			}
		}
		answers, err := ReplayAnswers(ctx, s, tools)
		if err != nil {
			t.Fatal(err)
		}
		opts, err := AgentOptions(s)
		if err != nil {
			t.Fatal(err)
		}
		b := agentturn.New(cfg, opts...)
		defer rec.Attach(b)()
		// A returned call is owed its result and takes no approval,
		// however safe its tool says running it again is.
		if _, err := b.Resume(ctx, agentturn.Approve(fetchID), agentturn.OutcomeUnknown(sendID)); !errors.Is(err, agentturn.ErrCallAnswered) {
			t.Errorf("an approval of the returned call: err = %v, want ErrCallAnswered", err)
		}
		if _, err := b.Resume(ctx, answers...); err != nil {
			t.Fatal(err)
		}
		if n := fetches.Load(); n != 1 {
			t.Errorf("fetch_page ran %d times, want once", n)
		}
		if n := sends.Load(); n != 1 {
			t.Errorf("send_email ran %d times, want once", n)
		}
		c := callsOf(t, s)["fetch_page"]
		if c == nil || c.Output == nil || c.Result == nil {
			t.Fatalf("fetch_page = %+v, want its result and its output", c)
		}
		if out := c.Output.Item.(*openresponses.FunctionCallOutput); out.Output.Text != "page" {
			t.Errorf("fetch_page's output = %q, want the recorded one", out.Output.Text)
		}
		if len(c.Decisions) != 0 {
			t.Errorf("fetch_page carries %d decisions, want its output alone after the result", len(c.Decisions))
		}
		if err := s.VerifyRecords(s.Leaf()); err != nil {
			t.Errorf("verify: %v", err)
		}
	})
	t.Run("a batch of one call", func(t *testing.T) {
		ctx := context.Background()
		store := agentsession.NewMemoryStore()
		rec, s, err := Start(ctx, store, agentsession.Header{})
		if err != nil {
			t.Fatal(err)
		}
		tools := []agenttool.Tool{agenttool.New("list", "", func(context.Context, echoArgs) (string, error) { return "a b", nil })}
		a := agentturn.New(agentturn.Config{Model: allCalls{}, ModelName: "m", MaxTurns: 1, Tools: tools})
		defer rec.Attach(a)()
		if _, err := a.Prompt(ctx, openresponses.UserText("go")); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(entryTypes(s), "result") {
			t.Errorf("a lone call wrote a result: %q", entryTypes(s))
		}
	})
}

// TestTheRecorderRefusesToRunAReturnedCall pins the recorder half of
// #273's replay rule: an agent that does not know a call returned, here
// one seeded with the context's items alone, which reads every call as
// one that may have run, cannot get it run again past the record. The
// format forbids a dispatch or a proceed after a result, so the
// recorder refuses the hand-off and the tool does not run.
func TestTheRecorderRefusesToRunAReturnedCall(t *testing.T) {
	ctx := context.Background()
	store := &cutStore{Store: agentsession.NewMemoryStore()}
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	var fetches atomic.Int32
	fetched := make(chan struct{})
	safe := agenttool.WithReplay(func(context.Context, json.RawMessage) agenttool.Replay { return agenttool.ReplaySafe })
	tools := []agenttool.Tool{
		agenttool.New("fetch_page", "", func(context.Context, echoArgs) (string, error) {
			fetches.Add(1)
			return "page", nil
		}, safe),
		agenttool.New("send_email", "", func(context.Context, echoArgs) (string, error) {
			<-fetched
			store.cut.Store(true)
			return "sent", nil
		}, safe),
	}
	cfg := agentturn.Config{Model: allCalls{}, ModelName: "m", MaxTurns: 1, Tools: tools}
	a := agentturn.New(cfg)
	unsub := rec.Attach(a)
	a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		if e, ok := ev.(*agentturn.ToolEnd); ok && e.Name == "fetch_page" {
			close(fetched)
		}
		return nil
	})
	if _, err := a.Prompt(ctx, openresponses.UserText("go")); err == nil {
		t.Fatal("the run survived the crash")
	}
	unsub()
	store.cut.Store(false)

	rec, s, err = Resume(ctx, store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	cx, err := s.Context()
	if err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(cfg, agentturn.WithTranscript(cx.Items))
	defer rec.Attach(b)()
	fetchID := callsOf(t, s)["fetch_page"].ID()
	var answers []agentturn.Answer
	for _, p := range b.State().Pending {
		if p.Call.CallID == fetchID {
			answers = append(answers, agentturn.Approve(fetchID).WithRunAgain())
		} else {
			answers = append(answers, agentturn.OutcomeUnknown(p.Call.CallID))
		}
	}
	if _, err := b.Resume(ctx, answers...); !errors.Is(err, agentsession.ErrCallReturned) {
		t.Errorf("running the returned call again: err = %v, want the recorder's refusal", err)
	}
	if n := fetches.Load(); n != 1 {
		t.Errorf("fetch_page ran %d times, want once", n)
	}
	if err := s.VerifyRecords(s.Leaf()); err != nil {
		t.Errorf("verify: %v", err)
	}
}
