package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
	"github.com/a2aproject/a2a-go/a2asrv/eventqueue"
)

func userMessage(text string) *a2a.Message {
	return a2a.NewMessage(a2a.MessageRoleUser, a2a.TextPart{Text: text})
}

func sendTask(t *testing.T, h a2asrv.RequestHandler, msg *a2a.Message) *a2a.Task {
	t.Helper()
	res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: msg})
	if err != nil {
		t.Fatalf("OnSendMessage: %v", err)
	}
	task, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("result = %T, want *a2a.Task", res)
	}
	return task
}

func TestSendMessageCompletes(t *testing.T) {
	store := NewMemoryStore()
	exec := New(agentturn.Config{Name: "echo", Model: &echo.Adapter{}, ModelName: "m"}, WithConversationStore(store))
	h := a2asrv.NewHandler(exec)

	task := sendTask(t, h, userMessage("hello there"))
	if task.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("state = %s", task.Status.State)
	}
	if got := Text(task); got != "hello there" {
		t.Errorf("text = %q", got)
	}
	if len(task.Artifacts) != 1 || partsText(task.Artifacts[0].Parts) != "hello there" {
		t.Errorf("artifacts = %+v", task.Artifacts)
	}
	if task.Status.Message == nil || task.Status.Message.Role != a2a.MessageRoleAgent {
		t.Errorf("status message = %+v", task.Status.Message)
	}
	tr, _ := store.Load(context.Background(), task.ContextID)
	if len(tr) != 2 {
		t.Errorf("stored transcript has %d items, want 2", len(tr))
	}
	if store.Len() != 1 {
		t.Errorf("stored contexts = %d", store.Len())
	}
}

func TestStreamingCoalescesArtifactChunks(t *testing.T) {
	exec := New(agentturn.Config{Model: &echo.Adapter{}}, WithChunkSize(8))
	h := a2asrv.NewHandler(exec)
	text := "one two three four five six seven eight nine ten"

	var (
		chunks   []*a2a.TaskArtifactUpdateEvent
		statuses []a2a.TaskState
	)
	for ev, err := range h.OnSendMessageStream(context.Background(), &a2a.MessageSendParams{Message: userMessage(text)}) {
		if err != nil {
			t.Fatal(err)
		}
		switch e := ev.(type) {
		case *a2a.TaskArtifactUpdateEvent:
			chunks = append(chunks, e)
		case *a2a.TaskStatusUpdateEvent:
			statuses = append(statuses, e.Status.State)
		}
	}
	if len(chunks) < 3 {
		t.Fatalf("chunks = %d, want several", len(chunks))
	}
	var b strings.Builder
	for i, c := range chunks {
		if c.Append != (i > 0) {
			t.Errorf("chunk %d append = %v", i, c.Append)
		}
		if c.LastChunk != (i == len(chunks)-1) {
			t.Errorf("chunk %d lastChunk = %v", i, c.LastChunk)
		}
		if c.Artifact.ID != chunks[0].Artifact.ID {
			t.Errorf("chunk %d has a different artifact id", i)
		}
		b.WriteString(partsText(c.Artifact.Parts))
	}
	if b.String() != text {
		t.Errorf("concatenated = %q", b.String())
	}
	if len(statuses) < 2 || statuses[0] != a2a.TaskStateWorking || statuses[len(statuses)-1] != a2a.TaskStateCompleted {
		t.Errorf("statuses = %v", statuses)
	}
}

func TestContextContinuesConversation(t *testing.T) {
	store := NewMemoryStore()
	var inputs []int
	model := &recordingModel{Adapter: &echo.Adapter{}, onRequest: func(req openresponses.Request) { inputs = append(inputs, len(req.Input)) }}
	h := a2asrv.NewHandler(New(agentturn.Config{Model: model}, WithConversationStore(store)))

	first := sendTask(t, h, userMessage("first"))
	second := userMessage("second")
	second.ContextID = first.ContextID
	task := sendTask(t, h, second)
	if task.ContextID != first.ContextID || task.ID == first.ID {
		t.Fatalf("second task = %+v", task)
	}
	if Text(task) != "second" {
		t.Errorf("text = %q", Text(task))
	}
	tr, _ := store.Load(context.Background(), first.ContextID)
	if len(tr) != 4 {
		t.Errorf("transcript has %d items, want 4", len(tr))
	}
	if len(inputs) != 2 || inputs[0] != 1 || inputs[1] != 3 {
		t.Errorf("model saw inputs of length %v, want [1 3]", inputs)
	}
}

type recordingModel struct {
	*echo.Adapter
	onRequest func(openresponses.Request)
}

func (m *recordingModel) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.onRequest(req)
	return m.Adapter.CreateStream(ctx, req, sink)
}

func TestCancelMidRun(t *testing.T) {
	blocking := agenttool.New("wait", "blocks until cancelled", func(ctx context.Context, _ struct {
		Text string `json:"text"`
	}) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	exec := New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{blocking}})
	h := a2asrv.NewHandler(exec)

	events := make(chan a2a.Event, 64)
	go func() {
		defer close(events)
		for ev, err := range h.OnSendMessageStream(context.Background(), &a2a.MessageSendParams{Message: userMessage("go")}) {
			if err != nil {
				return
			}
			events <- ev
		}
	}()
	var taskID a2a.TaskID
	select {
	case ev := <-events:
		taskID = ev.TaskInfo().TaskID
	case <-time.After(5 * time.Second):
		t.Fatal("no first event")
	}
	// Give the run time to reach the blocking tool.
	deadline := time.Now().Add(2 * time.Second)
	for {
		exec.mu.Lock()
		_, active := exec.cancels[taskID]
		exec.mu.Unlock()
		if active || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	canceled, err := h.OnCancelTask(context.Background(), &a2a.TaskIDParams{ID: taskID})
	if err != nil {
		t.Fatalf("OnCancelTask: %v", err)
	}
	if canceled.Status.State != a2a.TaskStateCanceled {
		t.Errorf("cancel result state = %s", canceled.Status.State)
	}
	var last a2a.Event
	for ev := range events {
		last = ev
	}
	switch e := last.(type) {
	case *a2a.TaskStatusUpdateEvent:
		if e.Status.State != a2a.TaskStateCanceled {
			t.Errorf("last status = %s", e.Status.State)
		}
	case *a2a.Task:
		if e.Status.State != a2a.TaskStateCanceled {
			t.Errorf("last task state = %s", e.Status.State)
		}
	default:
		t.Errorf("last event = %T", last)
	}
	got, err := h.OnGetTask(context.Background(), &a2a.TaskQueryParams{ID: taskID})
	if err != nil || got.Status.State != a2a.TaskStateCanceled {
		t.Errorf("stored task = %+v, err = %v", got, err)
	}
}

func TestCallerToolRoundTrip(t *testing.T) {
	store := NewMemoryStore()
	h := a2asrv.NewHandler(New(agentturn.Config{Model: &echo.Adapter{}}, WithConversationStore(store)))

	msg := userMessage("find it")
	msg.SetMeta(MetaCallerTools, []any{map[string]any{
		"type": "function", "name": "lookup", "description": "caller side lookup",
		"parameters": map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}, "required": []any{"q"}},
	}})
	task := sendTask(t, h, msg)
	if task.Status.State != a2a.TaskStateInputRequired {
		t.Fatalf("state = %s (%s)", task.Status.State, Text(task))
	}
	if task.Status.Message == nil || len(task.Status.Message.Parts) != 1 {
		t.Fatalf("status message = %+v", task.Status.Message)
	}
	dp, ok := task.Status.Message.Parts[0].(a2a.DataPart)
	if !ok || dp.Data["type"] != "function_call" || dp.Data["name"] != "lookup" {
		t.Fatalf("part = %+v", task.Status.Message.Parts[0])
	}
	// The pending call IDs survive the trip a real caller makes: JSON
	// out and back.
	raw, err := json.Marshal(task.Status.Message.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	var meta map[string]any
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatal(err)
	}
	if ids, _ := meta[MetaPendingCalls].([]any); len(ids) != 1 || ids[0] != dp.Data["call_id"] {
		t.Errorf("pending calls meta = %v", meta[MetaPendingCalls])
	}
	args, _ := dp.Data["arguments"].(string)
	if args != `{"q":"find it"}` {
		t.Errorf("arguments = %q", args)
	}
	tr, _ := store.Load(context.Background(), task.ContextID)
	if len(tr) != 2 {
		t.Fatalf("pending transcript = %d items, want user + function_call", len(tr))
	}
	if _, ok := tr[1].(*openresponses.FunctionCall); !ok {
		t.Errorf("transcript[1] = %T", tr[1])
	}

	answer := a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.DataPart{Data: map[string]any{
		"type": "function_call_output", "call_id": dp.Data["call_id"], "output": "found",
	}})
	done := sendTask(t, h, answer)
	if done.ID != task.ID || done.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("resumed task = %+v", done)
	}
	if Text(done) != "Tool result: found" {
		t.Errorf("text = %q", Text(done))
	}
	tr, _ = store.Load(context.Background(), task.ContextID)
	if len(tr) != 4 {
		t.Errorf("final transcript = %d items, want 4", len(tr))
	}
}

func TestFailedRun(t *testing.T) {
	h := a2asrv.NewHandler(New(agentturn.Config{Model: failing{}}))
	task := sendTask(t, h, userMessage("x"))
	if task.Status.State != a2a.TaskStateFailed || !strings.Contains(Text(task), "down") {
		t.Errorf("task = %s %q", task.Status.State, Text(task))
	}
	// Bad metadata is rejected before the run as invalid params, which
	// the SDK reports to the caller as an error rather than a task.
	bad := a2a.NewMessage(a2a.MessageRoleUser, a2a.TextPart{Text: "x"})
	bad.SetMeta(MetaCallerTools, "not a list")
	_, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: bad})
	if !errors.Is(err, a2a.ErrInvalidParams) {
		t.Errorf("bad metadata err = %v", err)
	}
}

type failing struct{}

func (failing) CreateStream(context.Context, openresponses.Request, openresponses.EventSink) error {
	return openresponses.ServerError("down", "model down")
}

func TestAgentCard(t *testing.T) {
	cfg := agentturn.Config{Name: "Research Agent", Description: "Finds things.", Tools: []agenttool.Tool{
		agenttool.New("search", "Search the web", func(context.Context, agenttool.NoArgs) (string, error) { return "", nil }),
	}}
	card := AgentCard(context.Background(), cfg, "http://localhost/invoke", "2.0.0")
	if card.Name != "Research Agent" || card.Description != "Finds things." || card.URL != "http://localhost/invoke" || card.Version != "2.0.0" {
		t.Errorf("card = %+v", card)
	}
	if !card.Capabilities.Streaming || card.PreferredTransport != a2a.TransportProtocolJSONRPC {
		t.Errorf("capabilities = %+v", card.Capabilities)
	}
	if len(card.Skills) != 2 || card.Skills[0].ID != "research_agent" || card.Skills[1].ID != "search" || card.Skills[1].Description != "Search the web" {
		t.Errorf("skills = %+v", card.Skills)
	}
	if AgentCard(context.Background(), agentturn.Config{}, "", "").Skills[0].ID != "agent" {
		t.Error("unnamed agent should get a default skill")
	}
	// A provider-backed tool list is advertised too.
	provided := agentturn.Config{ToolProvider: func(context.Context) []agenttool.Tool { return cfg.Tools }}
	if skills := AgentCard(context.Background(), provided, "", "").Skills; len(skills) != 2 || skills[1].ID != "search" {
		t.Errorf("provider skills = %+v", skills)
	}
	if _, err := json.Marshal(card); err != nil {
		t.Error(err)
	}
}

func TestItemsFromMessage(t *testing.T) {
	cases := []struct {
		name  string
		parts []a2a.Part
		want  string
		err   bool
	}{
		{"text", []a2a.Part{a2a.TextPart{Text: "hi"}}, "user[input_text]", false},
		{"text and image", []a2a.Part{a2a.TextPart{Text: "hi"}, a2a.FilePart{File: a2a.FileBytes{FileMeta: a2a.FileMeta{MimeType: "image/png"}, Bytes: "AA=="}}}, "user[input_text input_image]", false},
		{"file uri", []a2a.Part{a2a.FilePart{File: a2a.FileURI{FileMeta: a2a.FileMeta{MimeType: "application/pdf", Name: "a.pdf"}, URI: "https://x/a.pdf"}}}, "user[input_file]", false},
		{"image uri", []a2a.Part{a2a.FilePart{File: a2a.FileURI{FileMeta: a2a.FileMeta{MimeType: "image/jpeg"}, URI: "https://x/a.jpg"}}}, "user[input_image]", false},
		{"item data part splits messages", []a2a.Part{a2a.TextPart{Text: "a"}, a2a.DataPart{Data: map[string]any{"type": "function_call_output", "call_id": "c", "output": "o"}}, a2a.TextPart{Text: "b"}}, "user[input_text] function_call_output user[input_text]", false},
		{"raw data part is text", []a2a.Part{a2a.DataPart{Data: map[string]any{"k": 1}}}, "user[input_text]", false},
		{"bad item", []a2a.Part{a2a.DataPart{Data: map[string]any{"type": "message", "content": 5}}}, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			items, err := ItemsFromMessage(&a2a.Message{Parts: tc.parts})
			if (err != nil) != tc.err {
				t.Fatalf("err = %v", err)
			}
			if err != nil {
				return
			}
			var parts []string
			for _, item := range items {
				if m, ok := item.(*openresponses.Message); ok {
					var kinds []string
					for _, c := range m.Content {
						kinds = append(kinds, c.ContentType())
					}
					parts = append(parts, string(m.Role)+"["+strings.Join(kinds, " ")+"]")
					continue
				}
				parts = append(parts, item.ItemType())
			}
			if got := strings.Join(parts, " "); got != tc.want {
				t.Errorf("items = %q, want %q", got, tc.want)
			}
		})
	}
	// Raw JSON in a data part without a type is preserved as text.
	items, _ := ItemsFromMessage(&a2a.Message{Parts: []a2a.Part{a2a.DataPart{Data: map[string]any{"k": 1}}}})
	if items[0].(*openresponses.Message).Text() != `{"k":1}` {
		t.Errorf("raw data = %q", items[0].(*openresponses.Message).Text())
	}
}

func TestPartsFromItem(t *testing.T) {
	parts, err := PartsFromItem(openresponses.AssistantText("hello"))
	if err != nil || len(parts) != 1 || parts[0].(a2a.TextPart).Text != "hello" {
		t.Errorf("parts = %+v, err = %v", parts, err)
	}
	parts, err = PartsFromItem(&openresponses.FunctionCall{CallID: "c", Name: "f", Arguments: "{}"})
	if err != nil || len(parts) != 1 || parts[0].(a2a.DataPart).Data["type"] != "function_call" {
		t.Errorf("parts = %+v, err = %v", parts, err)
	}
}

func TestStripHelpers(t *testing.T) {
	items := openresponses.Items{
		openresponses.UserText("u"),
		&openresponses.FunctionCall{CallID: "a", Name: "x"},
		openresponses.NewFunctionCallOutput("a", "ok"),
		&openresponses.FunctionCall{CallID: "b", Name: "y"},
		openresponses.NewFunctionCallOutput("b", "pending"),
		&openresponses.FunctionCall{CallID: "c", Name: "z"},
	}
	if got := stripUnanswered(items); len(got) != 5 {
		t.Errorf("stripUnanswered = %d items", len(got))
	}
	if got := unanswered(items); len(got) != 1 || got[0].CallID != "c" {
		t.Errorf("unanswered = %+v", got)
	}
	// The resume contract: exactly the pending outputs, nothing else.
	cases := []struct {
		name    string
		prompts openresponses.Items
		ok      bool
	}{
		{"answers the call", openresponses.Items{openresponses.NewFunctionCallOutput("c", "v")}, true},
		{"text while pending", openresponses.Items{openresponses.UserText("hi")}, false},
		{"answers the wrong call", openresponses.Items{openresponses.NewFunctionCallOutput("zz", "v")}, false},
		{"answers twice", openresponses.Items{openresponses.NewFunctionCallOutput("c", "v"), openresponses.NewFunctionCallOutput("c", "v")}, false},
		{"nothing pending", nil, true},
	}
	for _, tc := range cases {
		tr := items
		if tc.name == "nothing pending" {
			tr = items[:5]
		}
		if err := checkAnswers(tr, tc.prompts); (err == nil) != tc.ok {
			t.Errorf("%s: err = %v", tc.name, err)
		}
	}
	if _, err := ItemsFromMessage(&a2a.Message{}); err != nil || errors.Is(err, context.Canceled) {
		t.Errorf("empty message err = %v", err)
	}
}

// callsEveryTool is a model that calls every offered tool once, then
// answers with the tool outputs it was given.
type callsEveryTool struct{}

func (callsEveryTool) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if len(req.Input) > 0 {
		if _, ok := req.Input[len(req.Input)-1].(*openresponses.FunctionCallOutput); ok {
			var texts []string
			for _, item := range req.Input {
				if fco, ok := item.(*openresponses.FunctionCallOutput); ok {
					texts = append(texts, fco.Output.String())
				}
			}
			w, err := em.Message(openresponses.PhaseFinalAnswer)
			if err != nil {
				return err
			}
			if err := w.Text(strings.Join(texts, "+")); err != nil {
				return err
			}
			return em.Complete()
		}
	}
	for _, tl := range req.Tools {
		ft := tl.(*openresponses.FunctionTool)
		w, err := em.FunctionCall("", ft.Name)
		if err != nil {
			return err
		}
		if err := w.Arguments(`{"q":"x"}`); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
	}
	return em.Complete()
}

func TestMixedBatchRunsLocalToolsAndDefersCallerTools(t *testing.T) {
	store := NewMemoryStore()
	local := agenttool.New("local", "runs here", func(context.Context, struct {
		Q string `json:"q"`
	}) (string, error) {
		return "local-ran", nil
	})
	h := a2asrv.NewHandler(New(agentturn.Config{Model: callsEveryTool{}, Tools: []agenttool.Tool{local}},
		WithConversationStore(store),
		WithCallerTools(openresponses.NewFunctionTool("remote", "caller side", json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`)))))

	task := sendTask(t, h, userMessage("go"))
	if task.Status.State != a2a.TaskStateInputRequired {
		t.Fatalf("state = %s (%s)", task.Status.State, Text(task))
	}
	if len(task.Status.Message.Parts) != 1 {
		t.Fatalf("pending parts = %d, want only the caller's call", len(task.Status.Message.Parts))
	}
	dp := task.Status.Message.Parts[0].(a2a.DataPart)
	if dp.Data["name"] != "remote" {
		t.Errorf("pending call = %v", dp.Data)
	}
	// The local call ran and its output is stored; the remote one is
	// unanswered.
	tr, _ := store.Load(context.Background(), task.ContextID)
	var outputs []string
	for _, item := range tr {
		if fco, ok := item.(*openresponses.FunctionCallOutput); ok {
			outputs = append(outputs, fco.Output.String())
		}
	}
	if len(outputs) != 1 || outputs[0] != "local-ran" || len(unanswered(tr)) != 1 {
		t.Errorf("stored outputs = %v, unanswered = %d", outputs, len(unanswered(tr)))
	}
	// A plain message while the call is pending does not fail the task:
	// it stays input-required, with the pending call repeated after a
	// note saying what is expected.
	again := sendTask(t, h, a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.TextPart{Text: "never mind"}))
	if again.Status.State != a2a.TaskStateInputRequired || len(again.Status.Message.Parts) != 2 {
		t.Fatalf("after a bad follow-up: state=%s parts=%d", again.Status.State, len(again.Status.Message.Parts))
	}
	if note, ok := again.Status.Message.Parts[0].(a2a.TextPart); !ok || !strings.Contains(note.Text, "waiting on 1 tool call") {
		t.Errorf("note = %+v", again.Status.Message.Parts[0])
	}
	if ids, _ := again.Status.Message.Metadata[MetaPendingCalls].([]any); len(ids) != 1 || ids[0] != dp.Data["call_id"] {
		t.Errorf("pending meta = %v", again.Status.Message.Metadata[MetaPendingCalls])
	}
	answer := a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.DataPart{Data: map[string]any{
		"type": "function_call_output", "call_id": dp.Data["call_id"], "output": "remote-ran",
	}})
	done := sendTask(t, h, answer)
	if done.Status.State != a2a.TaskStateCompleted || Text(done) != "local-ran+remote-ran" {
		t.Errorf("resumed = %s %q", done.Status.State, Text(done))
	}
}

// failingQueue fails every write after the first n.
type failingQueue struct {
	eventqueue.Queue
	n      int
	writes int
	err    error
}

func (q *failingQueue) Write(ctx context.Context, ev a2a.Event) error {
	q.writes++
	if q.writes > q.n {
		return q.err
	}
	return q.Queue.Write(ctx, ev)
}

func TestFailedEventWriteIsReturnedNotCanceled(t *testing.T) {
	store := NewMemoryStore()
	exec := New(agentturn.Config{Model: &echo.Adapter{}}, WithConversationStore(store))
	inner, err := eventqueue.NewInMemoryManager().GetOrCreate(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	broken := errors.New("queue broken")
	// The working status goes through; the first artifact chunk fails.
	q := &failingQueue{Queue: inner, n: 1, err: broken}
	reqCtx := &a2asrv.RequestContext{Message: userMessage("hello there"), TaskID: "t1", ContextID: "c1"}
	if err := exec.Execute(context.Background(), reqCtx, q); !errors.Is(err, broken) {
		t.Fatalf("Execute err = %v, want the write failure", err)
	}
	// The run was aborted by the failure, and the stored conversation
	// is still a valid input.
	tr, _ := store.Load(context.Background(), "c1")
	if len(tr) == 0 || unanswered(tr) != nil {
		t.Errorf("stored transcript = %d items, unanswered = %v", len(tr), unanswered(tr))
	}
	// The zero Executor is usable: track initialises the registry.
	var zero Executor
	zero.track("x", func() {})()
}
