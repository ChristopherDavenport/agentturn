// Package session records a live agentturn run into an agentsession
// store. It is the subscriber the session format was designed around:
// every item the loop appends becomes an item entry as soon as its
// item_end is delivered, every model call becomes a response entry
// carrying the hash of the exact request sent, config deltas track the
// settings between calls, a fold by the compact transform becomes a
// compaction entry, and a child run made through tools/agent becomes a
// linked subsession written live.
//
// Because the [agentturn.Agent] delivers events as barriers, a
// [Recorder] returns from each event only after the store's Append has
// returned under its sync policy, so tool preflight for a turn cannot
// start before the assistant items that requested it are durable.
//
//	store, _ := jsonl.Open(root)
//	rec, s, _ := session.Start(ctx, store, agentsession.Header{CWD: cwd})
//	defer rec.Attach(agent)()
//
// [Start] creates the session and [Resume] reopens one; both return
// the recorder and the session, and the caller keeps the store, which
// [Recorder.Store] also returns, for Sync, Release and Close.
//
// The package is named session, not agentsession, because a consumer
// imports both side by side: agentsession to open a store, session to
// attach a run to it.
//
// # What is written
//
//   - run_start: when the recorder knows the agent's configuration
//     ([Recorder.Attach] and [WithConfig] give it one), a full config
//     entry before the first item, so a root starts with one as the
//     format recommends.
//   - turn_start: a config entry when the settings in force changed
//     since the last call (a full one first if none was written, deltas
//     after, a tool list change as tools_added and tools_removed), so
//     the stored path replays to the request's settings.
//   - item_end: an item entry; an item streamed by the model carries its
//     response ID. An item the configured filter would hide from the
//     model, an app-only extension item, is written as a custom entry
//     instead so the path rebuilds exactly the input that was sent.
//   - response_end: the response entry with status, usage, error and the
//     request hash, after the items it produced and before any tool
//     output of the turn.
//   - run_end while a call is still in flight, because the model failed
//     or the run was aborted before the stream produced a response: a
//     response entry with status failed, the error and the request
//     hash, so the record shows the call was made and why it ended.
//   - a fold reported through [Recorder.Fold]: a compaction entry whose
//     first_kept is the entry of the first item the transform kept, with
//     the summary and the settings in force; a fold that failed is a
//     custom entry in the agentturn:compaction_failed namespace carrying
//     the error, so an abort or a failure during the fold leaves a
//     trace.
//   - a child run observed through [Recorder.Observe]: a session of its
//     own, with parent_session set and the child's configuration,
//     items, responses and folds written as they happen, and a link
//     entry with rel subsession and the call ID in the parent when the
//     call's tool_end arrives. A child of a child nests the same way.
//   - tool_end with a tools/agent ChildInfo whose run was not observed:
//     a session holding only the child's items, and the link; wire
//     Observe to get the full record.
//
// # Child runs
//
// A tools/agent child runs inside a tool call, so its events reach the
// parent's recorder only through the tool's observer:
//
//	specialist := agent.New(childCfg, agent.WithObserver(rec.Observe))
//
// Observe finds the parent run through the run ID the loop attaches to
// every tool call's context, so the same function serves every level of
// nesting: a child's child is linked from the child's session.
//
// # Compaction
//
// The hash recorded on a response is computed from the request as sent,
// in the canonical form [Canonical] defines. When a Transform changes
// the input for a call, the stored path must record the change or
// Session.Verify reports a mismatch for that call. For the compact
// transform, [Recorder.Fold] writes the compaction entry:
//
//	c := compact.NewLocal(model, compact.WithOnFold(rec.Fold))
//	cfg.Transform = c.Transform
//
// The recorder keeps the entry ID of every item it wrote, aligned with
// the agent's working transcript, and names the entry at the fold's
// split as first_kept. [Resume] seeds that alignment from the context
// at the leaf, so an agent resumed from Context.Items records folds
// too; an agent seeded with a transcript the recorder did not write and
// did not resume from cannot, and Fold returns an error, which fails
// the turn rather than let the record drift.
package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/compact"
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
)

// FailedFoldNS is the namespace of the custom entry written for a fold
// that failed. Its data is a [FailedFold].
const FailedFoldNS = "agentturn:compaction_failed"

// FailedFold is the data of a [FailedFoldNS] custom entry.
type FailedFold struct {
	Error        string `json:"error"`
	TokensBefore int    `json:"tokens_before,omitempty"`
}

// Recorder is an agentturn subscriber that writes one session, and the
// sessions of the child runs it observes. It is safe to attach to one
// agent at a time; events of a run arrive from one goroutine, and
// Observe may be called from the goroutines of parallel child tools.
type Recorder struct {
	store    agentsession.Store
	filter   func(agentturn.Transcript) agentturn.Transcript
	harness  *agentsession.Harness
	children bool
	now      func() time.Time

	// mu serialises Handle, Observe and Fold with Attach and Resume.
	// Events of a run arrive from one goroutine, so holding it across
	// every Append is not contention; it is what keeps entries in event
	// order when the recorder is attached to a second agent or observes
	// parallel children.
	mu sync.Mutex
	// root is the session this recorder was made for.
	root *writer
	// runs maps a run ID to the writer of its session: the root's
	// current run and every observed child run until its link is
	// written.
	runs    map[string]*writer
	rootRun string
}

// writer is the state of one session being written.
type writer struct {
	rec *Recorder
	id  string
	cfg *agentturn.Config

	settings agentsession.Settings
	// wroteConfig is set once a config entry is on the path, written by
	// this writer or replayed by Resume, so settle writes deltas.
	wroteConfig bool
	pending     string    // request hash of the call in flight
	started     time.Time // when the call in flight was sent
	// items holds the ID of the entry contributing each item of the
	// agent's working transcript, in order, so a fold's split index
	// names the first kept entry.
	items []string
}

// Option configures a Recorder.
type Option func(*Recorder)

// WithFilter sets the filter that decides which items the model saw.
// Items it drops are written as custom entries rather than item
// entries. It should match the agent's Config.Filter; the default is
// agentturn.DefaultFilter.
func WithFilter(f func(agentturn.Transcript) agentturn.Transcript) Option {
	return func(r *Recorder) { r.filter = f }
}

// WithHarness names the writer in the header of child sessions.
func WithHarness(name, version string) Option {
	return func(r *Recorder) { r.harness = &agentsession.Harness{Name: name, Version: version} }
}

// WithConfig gives the recorder the agent's configuration so the first
// entry it writes on a fresh session is a full config, before any item.
// [Recorder.Attach] sets it from the agent.
func WithConfig(cfg agentturn.Config) Option {
	return func(r *Recorder) { r.root.cfg = &cfg }
}

// WithoutChildSessions disables recording tools/agent child runs as
// linked sessions: Observe ignores every event and a ChildInfo on a
// tool_end writes nothing.
func WithoutChildSessions() Option {
	return func(r *Recorder) { r.children = false }
}

// New returns a recorder writing to the session with the given ID in
// store. The session must exist; [Start] creates one and [Resume]
// reopens one with its settings replayed, which New does not do.
func New(store agentsession.Store, sessionID string, opts ...Option) *Recorder {
	r := &Recorder{
		store:    store,
		filter:   agentturn.DefaultFilter,
		children: true,
		now:      time.Now,
		runs:     map[string]*writer{},
	}
	r.root = &writer{rec: r, id: sessionID}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Start creates a session from h and returns a recorder for it. Child
// sessions name h.Harness as their writer unless [WithHarness] says
// otherwise. The caller keeps store for Sync, Release and Close.
func Start(ctx context.Context, store agentsession.Store, h agentsession.Header, opts ...Option) (*Recorder, *agentsession.Session, error) {
	s, err := store.Create(ctx, h)
	if err != nil {
		return nil, nil, fmt.Errorf("session: create: %w", err)
	}
	r := New(store, s.ID(), opts...)
	if r.harness == nil {
		r.harness = s.Header().Harness
	}
	return r, s, nil
}

// Resume opens the session with the given ID and returns a recorder
// that continues it at its leaf. The recorder starts from the settings
// in force there, so the first config entry it writes is the delta
// from them, or nothing when the agent's configuration matches, rather
// than a full copy on every resume, and from the items of the context
// there, so an agent seeded with Context.Items can record folds. Child
// sessions name the header's harness unless [WithHarness] says
// otherwise.
func Resume(ctx context.Context, store agentsession.Store, sessionID string, opts ...Option) (*Recorder, *agentsession.Session, error) {
	s, err := store.Open(ctx, sessionID)
	if err != nil {
		return nil, nil, fmt.Errorf("session: open: %w", err)
	}
	r := New(store, s.ID(), opts...)
	if r.harness == nil {
		r.harness = s.Header().Harness
	}
	if s.Leaf() != "" {
		cx, err := s.Context()
		if err != nil {
			return nil, nil, fmt.Errorf("session: context at leaf: %w", err)
		}
		r.root.settings = cx.Settings
		r.root.wroteConfig = hasConfig(cx.Entries)
		r.root.items = make([]string, len(cx.ItemEntries))
		for i, e := range cx.ItemEntries {
			r.root.items[i] = e.Base().ID
		}
	}
	return r, s, nil
}

// hasConfig reports whether a config entry is on the path.
func hasConfig(entries []agentsession.Entry) bool {
	for _, e := range entries {
		if _, ok := e.(*agentsession.ConfigEntry); ok {
			return true
		}
	}
	return false
}

// SessionID returns the ID of the session being written.
func (r *Recorder) SessionID() string { return r.root.id }

// Store returns the store the recorder writes to.
func (r *Recorder) Store() agentsession.Store { return r.store }

// Attach subscribes the recorder to a and returns the unsubscribe
// function.
func (r *Recorder) Attach(a *agentturn.Agent) (unsubscribe func()) {
	r.mu.Lock()
	if r.root.cfg == nil {
		cfg := a.Config()
		r.root.cfg = &cfg
	}
	r.mu.Unlock()
	return a.Subscribe(r.Handle)
}

// Handle records one event of the agent's run. It is the subscriber
// function; use it directly with the low-level loop:
//
//	for ev := range agentturn.Run(ctx, t, prompts, cfg) {
//		if err := rec.Handle(ctx, ev); err != nil { ... }
//	}
func (r *Recorder) Handle(ctx context.Context, ev agentturn.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := ev.(*agentturn.RunStart); ok {
		if r.rootRun != "" {
			delete(r.runs, r.rootRun)
		}
		r.rootRun = e.RunID
		r.runs[e.RunID] = r.root
	}
	return r.root.handle(ctx, ev)
}

// Observe records one event of a child run made through tools/agent;
// register it with agent.WithObserver. On the child's run_start it
// creates the child's session, with parent_session naming the session
// of the run that called the tool, the run ID on the context, or this
// recorder's session when the tool ran outside a loop it records, and
// writes the child's configuration, which tools/agent puts on the
// context, as its first entry. Every later event of the run is written
// to that session as [Recorder.Handle] writes the agent's. The link
// from the parent is written when the call's tool_end reaches the
// parent's writer.
func (r *Recorder) Observe(ctx context.Context, ev agentturn.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.children {
		return
	}
	// An observer cannot fail the child run, so a store failure ends
	// the record of that child where it is: later events find no writer
	// and the parent's tool_end falls back to the items.
	if e, ok := ev.(*agentturn.RunStart); ok {
		parent := r.root
		if p, ok := r.runs[agentturn.RunIDFromContext(ctx)]; ok {
			parent = p
		}
		w, err := r.newChild(ctx, parent.id)
		if err != nil {
			return
		}
		if cfg, ok := agent.ConfigFromContext(ctx); ok {
			w.cfg = &cfg
		}
		r.runs[e.RunID] = w
	}
	w, ok := r.runs[runID(ev)]
	if !ok {
		return
	}
	if err := w.handle(ctx, ev); err != nil {
		delete(r.runs, runID(ev))
	}
}

// Fold records a fold of the compact transform; register it with
// compact.WithOnFold. A fold that was applied becomes a compaction
// entry naming the entry of the first kept item as first_kept, with
// the summary, the settings in force, the token estimate and the
// usage; a fold that failed becomes a custom entry in [FailedFoldNS].
// The run ID on the context says which session the fold belongs to, so
// a child's transform reports into the child's session; without one,
// the fold is the agent's. An error fails the transform, and so the
// turn.
func (r *Recorder) Fold(ctx context.Context, f compact.Fold) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	w := r.root
	if cw, ok := r.runs[agentturn.RunIDFromContext(ctx)]; ok {
		w = cw
	}
	// The fold may have been cut off by an abort; the record of it is
	// written regardless.
	return w.fold(context.WithoutCancel(ctx), f)
}

// newChild creates a session under parent and returns its writer.
func (r *Recorder) newChild(ctx context.Context, parent string) (*writer, error) {
	s, err := r.store.Create(ctx, agentsession.Header{ParentSession: parent, Harness: r.harness})
	if err != nil {
		return nil, fmt.Errorf("session: create child session: %w", err)
	}
	return &writer{rec: r, id: s.ID()}, nil
}

// runID returns the run an event belongs to.
func runID(ev agentturn.Event) string {
	switch e := ev.(type) {
	case *agentturn.RunStart:
		return e.RunID
	case *agentturn.TurnStart:
		return e.RunID
	case *agentturn.ModelRetry:
		return e.RunID
	case *agentturn.ItemStart:
		return e.RunID
	case *agentturn.ItemUpdate:
		return e.RunID
	case *agentturn.ItemEnd:
		return e.RunID
	case *agentturn.ResponseEnd:
		return e.RunID
	case *agentturn.ToolStart:
		return e.RunID
	case *agentturn.ToolUpdate:
		return e.RunID
	case *agentturn.ToolEnd:
		return e.RunID
	case *agentturn.TurnEnd:
		return e.RunID
	case *agentturn.RunEnd:
		return e.RunID
	}
	return ""
}

// Canonical returns the request in the form the session format hashes:
// the request as sent minus the members that describe the one call's
// transport rather than what the model received. Streaming flags and
// background are cleared, previous_response_id is dropped and store is
// false, which is what Settings.Request rebuilds from a stored path.
func Canonical(req openresponses.Request) openresponses.Request {
	store := false
	out := req
	out.Stream = false
	out.StreamOptions = nil
	out.Background = false
	out.PreviousResponseID = ""
	out.Store = &store
	return out
}

// handle writes the entry for one event of the writer's run.
func (w *writer) handle(ctx context.Context, ev agentturn.Event) error {
	switch e := ev.(type) {
	case *agentturn.RunStart:
		return w.runStart(ctx)
	case *agentturn.TurnStart:
		return w.turnStart(ctx, e)
	case *agentturn.ItemEnd:
		return w.item(ctx, e.Item, e.ResponseID)
	case *agentturn.ResponseEnd:
		return w.response(ctx, e)
	case *agentturn.RunEnd:
		return w.runEnd(ctx, e)
	case *agentturn.ToolEnd:
		if info, ok := e.Result.Details.(agent.ChildInfo); ok && w.rec.children {
			return w.child(ctx, e.CallID, info)
		}
	}
	return nil
}

// runStart writes the initial config from the agent's configuration
// when the writer has one and nothing has been written yet.
func (w *writer) runStart(ctx context.Context) error {
	if w.wroteConfig || w.cfg == nil {
		return nil
	}
	req := w.cfg.BaseRequest(ctx)
	return w.settle(ctx, Canonical(req))
}

func (w *writer) turnStart(ctx context.Context, e *agentturn.TurnStart) error {
	req := Canonical(e.Request)
	hash, err := RequestHash(req)
	if err != nil {
		return err
	}
	if err := w.settle(ctx, req); err != nil {
		return err
	}
	w.pending = hash
	w.started = w.rec.now()
	return nil
}

// settle brings the recorded settings to those of req, writing a full
// config first and deltas after.
func (w *writer) settle(ctx context.Context, req openresponses.Request) error {
	full, err := agentsession.ConfigFromRequest(req)
	if err != nil {
		return fmt.Errorf("session: %w", err)
	}
	next := agentsession.Settings{}.Apply(full)
	if !w.wroteConfig {
		if _, err := w.append(ctx, full); err != nil {
			return err
		}
	} else if delta := configDelta(w.settings, next, full); delta != nil {
		if _, err := w.append(ctx, delta); err != nil {
			return err
		}
	}
	w.settings = next
	w.wroteConfig = true
	return nil
}

// item writes an item entry, or a custom entry for an item the model
// did not see, and records its ID against the working transcript.
func (w *writer) item(ctx context.Context, item openresponses.Item, responseID string) error {
	if item == nil {
		return nil
	}
	var entry agentsession.Entry
	if responseID == "" && len(w.rec.filter(agentturn.Transcript{item})) == 0 {
		raw, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("session: encode %s item: %w", item.ItemType(), err)
		}
		entry = &agentsession.CustomEntry{NS: item.ItemType(), Data: raw}
	} else {
		entry = &agentsession.ItemEntry{Item: item, ResponseID: responseID}
	}
	id, err := w.append(ctx, entry)
	if err != nil {
		return err
	}
	w.items = append(w.items, id)
	return nil
}

func (w *writer) response(ctx context.Context, e *agentturn.ResponseEnd) error {
	resp := e.Response
	if resp == nil {
		return errors.New("session: response_end without a response")
	}
	entry := &agentsession.ResponseEntry{
		ResponseID:  resp.ID,
		Model:       resp.Model,
		Status:      resp.Status,
		Usage:       resp.Usage,
		Incomplete:  resp.IncompleteDetails,
		Error:       resp.Error,
		RequestHash: w.pending,
		LatencyMS:   w.latency(),
	}
	w.pending = ""
	w.started = time.Time{}
	_, err := w.append(ctx, entry)
	return err
}

// runEnd records a call that was sent and never answered: the run
// ended while a request hash was still in flight, so the model failed
// before producing a response or the run was aborted mid-stream. The
// in-flight state is cleared either way, so a writer reused for a
// later run cannot attribute its first response to this call.
func (w *writer) runEnd(ctx context.Context, e *agentturn.RunEnd) error {
	hash := w.pending
	latency := w.latency()
	w.pending = ""
	w.started = time.Time{}
	if hash == "" {
		return nil
	}
	entry := &agentsession.ResponseEntry{
		Status:      openresponses.ResponseStatusFailed,
		Error:       errorPayload(e.Err),
		RequestHash: hash,
		LatencyMS:   latency,
	}
	_, err := w.append(ctx, entry)
	return err
}

// latency is the time since the call in flight was sent, in
// milliseconds, or zero when none is.
func (w *writer) latency() int64 {
	if w.started.IsZero() {
		return 0
	}
	if ms := w.rec.now().Sub(w.started).Milliseconds(); ms > 0 {
		return ms
	}
	return 0
}

// errorPayload is the wire form of err: the payload of an
// openresponses error, or a server_error carrying its message.
func errorPayload(err error) *openresponses.ErrorPayload {
	if err == nil {
		return nil
	}
	var oe *openresponses.Error
	if errors.As(err, &oe) {
		p := oe.Payload()
		return &p
	}
	return &openresponses.ErrorPayload{Type: openresponses.ErrorTypeServerError, Message: err.Error()}
}

// fold writes the compaction entry for an applied fold, or the failed
// fold entry for one that failed.
func (w *writer) fold(ctx context.Context, f compact.Fold) error {
	if f.Err != nil {
		raw, err := json.Marshal(FailedFold{Error: f.Err.Error(), TokensBefore: f.TokensBefore})
		if err != nil {
			return fmt.Errorf("session: encode failed fold: %w", err)
		}
		_, err = w.append(ctx, &agentsession.CustomEntry{NS: FailedFoldNS, Data: raw})
		return err
	}
	if f.Summary == nil {
		return errors.New("session: fold has no summary item")
	}
	if f.Split < 0 || f.Split >= len(w.items) {
		return fmt.Errorf("session: fold keeps the transcript from item %d, but the recorder wrote %d items", f.Split, len(w.items))
	}
	_, err := w.append(ctx, &agentsession.CompactionEntry{
		FirstKept:    w.items[f.Split],
		Summary:      f.Summary,
		Config:       w.settings,
		TokensBefore: f.TokensBefore,
		Usage:        f.Usage,
	})
	return err
}

// child links the session of a child run to this one. A run that was
// observed has its session already; its writer is released. One that
// was not gets a session holding only its items.
func (w *writer) child(ctx context.Context, callID string, info agent.ChildInfo) error {
	r := w.rec
	if cw, ok := r.runs[info.RunID]; ok {
		delete(r.runs, info.RunID)
		_, err := w.append(ctx, agentsession.NewSubsessionLink(cw.id, callID))
		return err
	}
	cw, err := r.newChild(ctx, w.id)
	if err != nil {
		return err
	}
	for _, item := range info.Items {
		responseID := ""
		if isModelOutput(item) {
			responseID = info.RunID
		}
		if err := cw.item(ctx, item, responseID); err != nil {
			return err
		}
	}
	_, err = w.append(ctx, agentsession.NewSubsessionLink(cw.id, callID))
	return err
}

// isModelOutput reports whether an item can only have come from the
// model, so a replayed child transcript can mark it as response output.
func isModelOutput(item openresponses.Item) bool {
	switch v := item.(type) {
	case *openresponses.Message:
		return v.Role == openresponses.RoleAssistant
	case *openresponses.FunctionCall, *openresponses.ReasoningItem:
		return true
	}
	return false
}

func (w *writer) append(ctx context.Context, e agentsession.Entry) (string, error) {
	id, err := w.rec.store.Append(ctx, w.id, e)
	if err != nil {
		return "", fmt.Errorf("session: append %s: %w", e.EntryType(), err)
	}
	return id, nil
}

// configDelta returns the config entry that takes prev to next, nil when
// they are equal, or full (a replace entry) when the change cannot be
// expressed as a delta: a model being cleared, a tool list change that
// a delta would not replay in the request's order, or a delta that
// would be larger than the replacement.
func configDelta(prev, next agentsession.Settings, full *agentsession.ConfigEntry) *agentsession.ConfigEntry {
	if equalJSON(prev, next) {
		return nil
	}
	if next.Model == "" && prev.Model != "" {
		return full
	}
	d := &agentsession.ConfigEntry{}
	if !equalJSON(prev.Tools, next.Tools) {
		d.ToolsAdded, d.ToolsRemoved = toolDelta(prev.Tools, next.Tools)
		// Replay appends added tools after the kept ones, so a delta
		// stands only when that yields the request's tool order; the
		// hash of the stored path depends on it.
		if !equalJSON(prev.Apply(d).Tools, next.Tools) {
			return full
		}
	}
	if next.Model != prev.Model {
		d.Model = next.Model
	}
	if next.Instructions != prev.Instructions {
		s := next.Instructions
		d.Instructions = &s
	}
	if next.Reasoning != prev.Reasoning {
		rc := next.Reasoning
		d.Reasoning = &rc
	}
	if !equalJSON(prev.Text, next.Text) {
		tc := next.Text
		d.Text = &tc
	}
	for k, v := range next.Extra {
		if p, ok := prev.Extra[k]; !ok || string(p) != string(v) {
			if err := d.SetExtra(k, v); err != nil {
				// v is raw JSON that already decoded once; it cannot fail
				// to re-encode, so a full replace is the safe fallback.
				return full
			}
		}
	}
	for k := range prev.Extra {
		if _, ok := next.Extra[k]; !ok {
			d.ClearExtra(k)
		}
	}
	if jsonLen(d) >= jsonLen(full) {
		return full
	}
	return d
}

// toolDelta returns the tools of next that prev lacks or defines
// differently, in next's order, and the names of prev's tools that next
// lacks, in prev's order. A tool whose definition changed under the
// same name is in added: replay removes the old definition by name
// before appending the new one.
func toolDelta(prev, next openresponses.Tools) (added openresponses.Tools, removed []string) {
	before := make(map[string]openresponses.Tool, len(prev))
	for _, t := range prev {
		before[agentsession.ToolName(t)] = t
	}
	after := make(map[string]bool, len(next))
	for _, t := range next {
		name := agentsession.ToolName(t)
		after[name] = true
		if old, ok := before[name]; !ok || !equalJSON(old, t) {
			added = append(added, t)
		}
	}
	for _, t := range prev {
		if name := agentsession.ToolName(t); !after[name] {
			removed = append(removed, name)
		}
	}
	return added, removed
}

// jsonLen is the encoded size of v, or zero when it cannot be encoded.
func jsonLen(v any) int {
	data, err := json.Marshal(v)
	if err != nil {
		return 0
	}
	return len(data)
}

func equalJSON(a, b any) bool {
	da, errA := json.Marshal(a)
	db, errB := json.Marshal(b)
	return errA == nil && errB == nil && string(da) == string(db)
}
