package session

import (
	"context"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// roots counts the entries with no parent.
func roots(s *agentsession.Session) int {
	n := 0
	for _, e := range s.Entries() {
		if e.Base().Parent == "" {
			n++
		}
	}
	return n
}

// callEntry returns the ID of the item entry holding the first function
// call, and the call.
func callEntry(t *testing.T, s *agentsession.Session) (string, *openresponses.FunctionCall) {
	t.Helper()
	for _, e := range s.Entries() {
		it, ok := e.(*agentsession.ItemEntry)
		if !ok {
			continue
		}
		if call, ok := it.Item.(*openresponses.FunctionCall); ok {
			return e.Base().ID, call
		}
	}
	t.Fatal("no function call on the path")
	return "", nil
}

// TestSiblingAnswerGetsItsDecision is the /tree case: the user goes
// back to the entry holding a call the agent asked about and answers it
// differently, so the branch holds the call with neither the dispatch
// nor the hold that are on the branch it left. The output the caller
// writes is a reject decision there as it is anywhere else, and the
// record check passes.
func TestSiblingAnswerGetsItsDecision(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	deferAll := func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
		return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "ask the user"}, nil
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{upper}, BeforeToolCall: deferAll})
	defer rec.Attach(a)()
	end, err := a.Prompt(context.Background(), openresponses.UserText("abc"))
	if err != nil || end.Reason != agentturn.ReasonInputRequired {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	entryID, call := callEntry(t, s)
	// Answer on the first branch, then go back to the call and answer
	// it differently, as a session tree lets a user do.
	if _, err := a.Resume(context.Background(), agentturn.Output(openresponses.NewFunctionCallOutput(call.CallID, "jsonl"))); err != nil {
		t.Fatal(err)
	}
	if err := rec.Rebase(s, entryID); err != nil {
		t.Fatal(err)
	}
	cx, err := s.Context()
	if err != nil {
		t.Fatal(err)
	}
	if err := a.SetTranscript(cx.Items); err != nil {
		t.Fatal(err)
	}
	pending := a.State().Pending
	if len(pending) != 1 || pending[0].Reason != agentturn.PendingUnknown {
		t.Fatalf("pending on the sibling branch = %+v", pending)
	}
	if _, err := a.Resume(context.Background(), agentturn.Output(openresponses.NewFunctionCallOutput(call.CallID, "sqlite"))); err != nil {
		t.Fatal(err)
	}
	verifyAll(t, s)
	if len(s.Leaves()) != 2 {
		t.Errorf("leaves = %v", s.Leaves())
	}
	c := callsOf(t, s)["upper"]
	if c == nil || c.Dispatch != nil || c.Output == nil {
		t.Fatalf("call on the sibling branch = %+v", c)
	}
	if len(c.Decisions) != 1 || c.Decisions[0].Verdict != agentsession.VerdictReject || c.Decisions[0].Reason != "sqlite" {
		t.Errorf("decisions = %+v", c.Decisions)
	}
	if got := c.State(s.Header()); got != agentsession.CallCompleted {
		t.Errorf("call state = %v", got)
	}
}

// messageThenCut completes an assistant message and then blocks until
// the stream is cut, as a provider does when a rule or a user stops a
// run between two output items.
type messageThenCut struct{}

func (messageThenCut) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	w, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if err := w.Text("half an answer"); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

// TestFailedResponseNamesTheResponseInFlight checks that a stream cut
// after an item completed is written as a failed response carrying the
// response ID the writer saw, so the context algorithm strips that item
// rather than reading it as the call's own input.
func TestFailedResponseNamesTheResponseInFlight(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: messageThenCut{}, ModelName: "m"})
	defer rec.Attach(a)()
	a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		if e, ok := ev.(*agentturn.ItemEnd); ok {
			if m, ok := e.Item.(*openresponses.Message); ok && m.Role == openresponses.RoleAssistant {
				a.Abort()
			}
		}
		return nil
	})
	end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
	if end == nil || end.Reason != agentturn.ReasonAborted {
		t.Fatalf("end = %+v", end)
	}
	var failed *agentsession.ResponseEntry
	for _, e := range s.Entries() {
		if r, ok := e.(*agentsession.ResponseEntry); ok && r.Status == openresponses.ResponseStatusFailed {
			failed = r
		}
	}
	if failed == nil {
		t.Fatal("no failed response entry")
	}
	if failed.ResponseID == "" {
		t.Error("the failed response names no response, so its own items read as its input")
	}
	if failed.RequestHash == "" {
		t.Error("the failed response carries no request hash")
	}
	// The item the cut call produced is stripped, so the rebuilt
	// request is the one that was sent.
	if n := verifyAll(t, s); n != 1 {
		t.Errorf("responses = %d", n)
	}
	cx, err := s.RequestContext(failed.Base().ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(cx.Items) != 1 {
		t.Errorf("rebuilt input = %d items, want the prompt alone", len(cx.Items))
	}
}

// TestRebaseToTheEmptyIDResets is a product's /clear: the model's
// context begins again and the file keeps everything. The new root
// opens with a full config and its responses carry hashes.
func TestRebaseToTheEmptyIDResets(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Instructions: "be brief"})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("one")); err != nil {
		t.Fatal(err)
	}
	if err := rec.Rebase(s, ""); err != nil {
		t.Fatal(err)
	}
	if err := a.SetTranscript(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Prompt(context.Background(), openresponses.UserText("new topic")); err != nil {
		t.Fatal(err)
	}
	if n := roots(s); n != 2 {
		t.Errorf("roots = %d", n)
	}
	if n := verifyAll(t, s); n != 2 || hashed(s) != 2 {
		t.Errorf("responses = %d hashed = %d", n, hashed(s))
	}
	// The new root opens with a full config, so the context algorithm
	// rebuilds it with the model and the instructions in force.
	path := s.Path(s.Leaf())
	if len(path) == 0 {
		t.Fatal("no path to the leaf")
	}
	var kinds []string
	for _, e := range path {
		kinds = append(kinds, e.EntryType())
	}
	if kinds[0] != agentsession.TypeRun || kinds[1] != agentsession.TypeConfig {
		t.Errorf("new root opens with %v", kinds)
	}
	cfg, ok := path[1].(*agentsession.ConfigEntry)
	if !ok || cfg.Model != "m" || cfg.Instructions == nil || *cfg.Instructions != "be brief" {
		t.Errorf("config on the new root = %+v", path[1])
	}
	cx, err := s.Context()
	if err != nil {
		t.Fatal(err)
	}
	if cx.Settings.Model != "m" || cx.Settings.Instructions != "be brief" {
		t.Errorf("settings at the new leaf = %+v", cx.Settings)
	}
	if len(cx.Items) != 2 {
		t.Errorf("items at the new leaf = %d, want the new topic alone", len(cx.Items))
	}
	if got := entryTypes(s); !strings.Contains(got, "config") {
		t.Errorf("entries = %s", got)
	}
}
