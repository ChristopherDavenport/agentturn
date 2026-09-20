// Package session records a live agentturn run into an agentsession
// store. It is the subscriber the session format was designed around:
// every item the loop appends becomes an item entry as soon as its
// item_end is delivered, every model call becomes a response entry
// carrying the hash of the exact request sent, config deltas track the
// settings between calls, and a child run made through tools/agent
// becomes a linked subsession.
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
//     after), so the stored path replays to the request's settings.
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
//   - tool_end with a tools/agent ChildInfo in its details: a new
//     session holding the child's items, with parent_session set, and a
//     link entry in this session with rel subsession and the call ID.
//
// # Verifiability
//
// The hash recorded on a response is computed from the request as sent,
// in the canonical form [Canonical] defines. When the loop's Transform
// changes the input for a call, the request differs from the stored
// path and Session.Verify reports a mismatch for that call; recording a
// compaction entry for such a change is the caller's job.
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
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
)

// Recorder is an agentturn subscriber that writes one session. It is
// safe to attach to one agent at a time; events of a run arrive from
// one goroutine.
type Recorder struct {
	store    agentsession.Store
	id       string
	filter   func(agentturn.Transcript) agentturn.Transcript
	harness  *agentsession.Harness
	children bool
	now      func() time.Time
	cfg      *agentturn.Config

	// mu serialises Handle with Attach and Resume. Events of a run
	// arrive from one goroutine, so holding it across every Append is
	// not contention; it is what keeps entries in event order when the
	// recorder is attached to a second agent.
	mu       sync.Mutex
	settings agentsession.Settings
	// wroteConfig is set once a config entry is on the path, written by
	// this recorder or replayed by Resume, so settle writes deltas.
	wroteConfig bool
	pending     string    // request hash of the call in flight
	started     time.Time // when the call in flight was sent
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
	return func(r *Recorder) { r.cfg = &cfg }
}

// WithoutChildSessions disables recording tools/agent child runs as
// linked sessions.
func WithoutChildSessions() Option {
	return func(r *Recorder) { r.children = false }
}

// New returns a recorder writing to the session with the given ID in
// store. The session must exist; [Start] creates one and [Resume]
// reopens one with its settings replayed, which New does not do.
func New(store agentsession.Store, sessionID string, opts ...Option) *Recorder {
	r := &Recorder{
		store:    store,
		id:       sessionID,
		filter:   agentturn.DefaultFilter,
		children: true,
		now:      time.Now,
	}
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
// than a full copy on every resume. Child sessions name the header's
// harness unless [WithHarness] says otherwise.
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
		r.settings = cx.Settings
		r.wroteConfig = hasConfig(cx.Entries)
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
func (r *Recorder) SessionID() string { return r.id }

// Store returns the store the recorder writes to.
func (r *Recorder) Store() agentsession.Store { return r.store }

// Attach subscribes the recorder to a and returns the unsubscribe
// function.
func (r *Recorder) Attach(a *agentturn.Agent) (unsubscribe func()) {
	r.mu.Lock()
	if r.cfg == nil {
		cfg := a.Config()
		r.cfg = &cfg
	}
	r.mu.Unlock()
	return a.Subscribe(r.Handle)
}

// Handle records one event. It is the subscriber function; use it
// directly with the low-level loop:
//
//	for ev := range agentturn.Run(ctx, t, prompts, cfg) {
//		if err := rec.Handle(ctx, ev); err != nil { ... }
//	}
func (r *Recorder) Handle(ctx context.Context, ev agentturn.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch e := ev.(type) {
	case *agentturn.RunStart:
		return r.runStart(ctx)
	case *agentturn.TurnStart:
		return r.turnStart(ctx, e)
	case *agentturn.ItemEnd:
		return r.item(ctx, e.Item, e.ResponseID)
	case *agentturn.ResponseEnd:
		return r.response(ctx, e)
	case *agentturn.RunEnd:
		return r.runEnd(ctx, e)
	case *agentturn.ToolEnd:
		if info, ok := e.Result.Details.(agent.ChildInfo); ok && r.children {
			return r.child(ctx, e.CallID, info)
		}
	}
	return nil
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

// runStart writes the initial config from the agent's configuration
// when the recorder has one and nothing has been written yet.
func (r *Recorder) runStart(ctx context.Context) error {
	if r.wroteConfig || r.cfg == nil {
		return nil
	}
	req := r.cfg.BaseRequest(ctx)
	return r.settle(ctx, Canonical(req))
}

func (r *Recorder) turnStart(ctx context.Context, e *agentturn.TurnStart) error {
	req := Canonical(e.Request)
	hash, err := RequestHash(req)
	if err != nil {
		return err
	}
	if err := r.settle(ctx, req); err != nil {
		return err
	}
	r.pending = hash
	r.started = r.now()
	return nil
}

// settle brings the recorded settings to those of req, writing a full
// config first and deltas after.
func (r *Recorder) settle(ctx context.Context, req openresponses.Request) error {
	full, err := agentsession.ConfigFromRequest(req)
	if err != nil {
		return fmt.Errorf("session: %w", err)
	}
	next := agentsession.Settings{}.Apply(full)
	if !r.wroteConfig {
		if err := r.append(ctx, full); err != nil {
			return err
		}
	} else if delta := configDelta(r.settings, next, full); delta != nil {
		if err := r.append(ctx, delta); err != nil {
			return err
		}
	}
	r.settings = next
	r.wroteConfig = true
	return nil
}

func (r *Recorder) item(ctx context.Context, item openresponses.Item, responseID string) error {
	if item == nil {
		return nil
	}
	if responseID == "" && len(r.filter(agentturn.Transcript{item})) == 0 {
		raw, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("session: encode %s item: %w", item.ItemType(), err)
		}
		return r.append(ctx, &agentsession.CustomEntry{NS: item.ItemType(), Data: raw})
	}
	return r.append(ctx, &agentsession.ItemEntry{Item: item, ResponseID: responseID})
}

func (r *Recorder) response(ctx context.Context, e *agentturn.ResponseEnd) error {
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
		RequestHash: r.pending,
		LatencyMS:   r.latency(),
	}
	r.pending = ""
	r.started = time.Time{}
	return r.append(ctx, entry)
}

// runEnd records a call that was sent and never answered: the run
// ended while a request hash was still in flight, so the model failed
// before producing a response or the run was aborted mid-stream. The
// in-flight state is cleared either way, so a recorder reused for a
// later run cannot attribute its first response to this call.
func (r *Recorder) runEnd(ctx context.Context, e *agentturn.RunEnd) error {
	hash := r.pending
	latency := r.latency()
	r.pending = ""
	r.started = time.Time{}
	if hash == "" {
		return nil
	}
	entry := &agentsession.ResponseEntry{
		Status:      openresponses.ResponseStatusFailed,
		Error:       errorPayload(e.Err),
		RequestHash: hash,
		LatencyMS:   latency,
	}
	return r.append(ctx, entry)
}

// latency is the time since the call in flight was sent, in
// milliseconds, or zero when none is.
func (r *Recorder) latency() int64 {
	if r.started.IsZero() {
		return 0
	}
	if ms := r.now().Sub(r.started).Milliseconds(); ms > 0 {
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

// child writes the items of a tools/agent run as a session of its own
// and links it from this one.
func (r *Recorder) child(ctx context.Context, callID string, info agent.ChildInfo) error {
	h := agentsession.Header{ParentSession: r.id, Harness: r.harness}
	s, err := r.store.Create(ctx, h)
	if err != nil {
		return fmt.Errorf("session: create child session: %w", err)
	}
	childRec := New(r.store, s.ID(), WithFilter(r.filter))
	for _, item := range info.Items {
		responseID := ""
		if isModelOutput(item) {
			responseID = info.RunID
		}
		if err := childRec.item(ctx, item, responseID); err != nil {
			return err
		}
	}
	return r.append(ctx, agentsession.NewSubsessionLink(s.ID(), callID))
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

func (r *Recorder) append(ctx context.Context, e agentsession.Entry) error {
	if _, err := r.store.Append(ctx, r.id, e); err != nil {
		return fmt.Errorf("session: append %s: %w", e.EntryType(), err)
	}
	return nil
}

// configDelta returns the config entry that takes prev to next, nil when
// they are equal, or full (a replace entry) when the change cannot be
// expressed as a delta, which is a tool list change or a model being
// cleared.
func configDelta(prev, next agentsession.Settings, full *agentsession.ConfigEntry) *agentsession.ConfigEntry {
	if equalJSON(prev, next) {
		return nil
	}
	if !equalJSON(prev.Tools, next.Tools) || (next.Model == "" && prev.Model != "") {
		return full
	}
	d := &agentsession.ConfigEntry{}
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
	return d
}

func equalJSON(a, b any) bool {
	da, errA := json.Marshal(a)
	db, errB := json.Marshal(b)
	return errA == nil && errB == nil && string(da) == string(db)
}
