// Package session records a live agentturn run into an agentsession
// store. It is the subscriber the session format was designed around:
// every item the loop appends becomes an item entry as soon as its
// item_end is delivered, every model call becomes a response entry
// carrying the hash of the exact request sent, config deltas track the
// settings between calls, a fold by the compact transform becomes a
// compaction entry, a child run made through tools/agent becomes a
// linked subsession written live, and the record entries of format 0.2,
// run, dispatch and decision, say why each run started, how it ended,
// which calls reached their tools and what was decided about them.
//
// Because the [agentturn.Agent] delivers events as barriers, a
// [Recorder] returns from each event only after the store's Append has
// returned under its sync policy, so tool preflight for a turn cannot
// start before the assistant items that requested it are durable, and
// a dispatch entry is durable before its tool runs.
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
//   - run_start: a run entry with phase start, the source the loop
//     reports (input, or resume for a run that answers pending calls)
//     and the [agentturn.Trigger] on the context as ref; then, for the
//     recorder's own session, the env entry [WithEnv] supplies when it
//     differs from the last one written; then, when the recorder knows
//     the agent's configuration ([Recorder.Attach] and [WithConfig]
//     give it one), a full config entry before the first item, so a
//     root starts with one as the format recommends.
//   - turn_start: a config entry when the settings in force changed
//     since the last call (a full one first if none was written, deltas
//     after, a tool list change as tools_added and tools_removed), so
//     the stored path replays to the request's settings.
//   - model_blocked: the config settle for the request that was built,
//     then a response entry with status failed, the hook's error and
//     the request hash, so a call a BeforeModelCall guard refused is on
//     the record and distinct from one that was made and failed.
//   - item_end: an item entry; an item streamed by the model carries its
//     response ID, and an item the caller marked with agentturn.Hidden
//     carries visible false, the format's word for an item that is part
//     of the model context and that a renderer should hide. An item the
//     filter in force would hide from the
//     model, an app-only extension item, is written as a custom entry
//     instead so the path rebuilds exactly the input that was sent. A
//     function_call_output for a call the path holds no dispatch and no
//     reject for, one the caller answered through Agent.Resume with an
//     output of their own, is preceded by a reject decision carrying
//     the output's text as its reason: a call held by a hold decision
//     is the common case, and a call seeded from a branch that left its
//     dispatch behind is the same thing to a reader.
//   - tool_start: the call's decision when there is one to record, then
//     its dispatch. A BeforeToolCall that blocked the call is a reject
//     decision with its reason and no dispatch; one that deferred it is
//     a hold carrying the same reason, the rule that raised the prompt,
//     and no dispatch; one that rewrote the arguments, or an approval
//     through Agent.Resume of a held call, is a proceed decision
//     carrying the arguments the tool ran with when they differ from
//     the model's. The dispatch follows every decision that lets the
//     call run, and stands alone for a call nothing decided about. The
//     decision's by is ToolDecision.By, which Answer.By sets for an
//     approval, and policy for a hook's decision about a call nothing
//     was holding; an answer that names nobody is written with no by,
//     since a policy engine answers through Resume as often as a person
//     does.
//   - response_end: the response entry with status, usage, error and the
//     request hash, after the items it produced and before any tool
//     output of the turn.
//   - run_end: a response entry with status failed, the error, the
//     request hash and the ID of the response the stream had named,
//     when a call was still in flight because the model failed or the
//     run was aborted before the stream produced a response; the ID is
//     what lets the context algorithm strip the items that call
//     produced, a reasoning item completed before the cut for one,
//     rather than read them as its input; then the run entry with
//     phase end, the reason in the
//     format's terms and the IDs of the run's calls left without an
//     output. The loop's reasons map onto the format's cascade: done
//     and input_required as they are, error with the error as ref,
//     aborted as interrupted with the context error as ref, since the
//     host asked for the stop, and stopped as stopped when the last
//     response requested tools, as done when it did not and as aborted
//     when the run made no model call, with the stop's cause as ref in
//     every case.
//   - a fold reported through [Recorder.Fold]: a compaction entry whose
//     first_kept is the entry of the first item the transform kept, with
//     the summary and the settings in force, and a fold member naming
//     the fold's own model call by response ID, model and request hash;
//     that hash is of the fold's request, which no path rebuilds, and
//     is kept so a replay can recognise the call. A fold that failed is
//     a custom entry in the agentturn:compaction_failed namespace
//     carrying the error, so an abort or a failure during the fold
//     leaves a trace.
//   - a child run observed through [Recorder.Observe]: a session of its
//     own whose ID is derived from the parent's and the call's as the
//     format recommends, with parent_session, spawned_by and the same
//     records promise set, the child's configuration, items, responses,
//     records and folds written as they happen, and a link entry with
//     rel subsession and the call ID in the parent written when the
//     child starts, at dispatch. A child of a child nests the same way.
//   - tool_end with a tools/agent ChildInfo whose run was not observed:
//     a session holding only the child's items, with no records
//     promise, and the link; wire Observe to get the full record.
//   - tool_end whose Result.Details implements agenttool.Recordable: a
//     custom entry in the namespace the value names, carrying its JSON,
//     between the call's dispatch and its output. This is how a tool
//     keeps what its output does not carry, the full bytes of a
//     truncated result for one, in the session without the recorder
//     knowing its type. Details for in-process subscribers alone are
//     not recorded.
//
// # Header
//
// [Start] promises the run, dispatch and decision records in the
// header unless the caller set Records, so a reader takes a call with
// no dispatch as never started and a run with no end entry as cut off.
// The promise is kept by the barrier: the recorder returns from
// tool_start only after the dispatch is appended, and the loop does
// not hand the call to its tool before then.
//
// # Child runs
//
// A tools/agent child runs inside a tool call, so its events reach the
// parent's recorder only through the tool's observer:
//
//	specialist := agent.New(childCfg, agent.WithObserver(rec.Observe))
//
// Observe finds the parent run through the run ID the loop attaches to
// every tool call's context, and the call through agenttool.CallFrom,
// so the same function serves every level of nesting: a child's child
// is linked from the child's session.
//
// # Request hashes and compaction
//
// The hash recorded on a response is computed from the request as sent,
// in the canonical form [Canonical] defines, and is written only when
// the recorder can stand behind it: when the request's input is what
// the stored path rebuilds, which the recorder checks against the items
// it wrote, the fold it last recorded and the filter in force. A
// request whose input a Transform or a BeforeModelCall changed in a way
// the record does not describe, or that carries items the recorder
// never wrote, such as a child's seed transcript, is recorded without
// a hash, and Session.Verify reports it as unverified rather than as a
// mismatch. For the compact transform, [Recorder.Fold] writes the
// compaction entry that describes the change, so its requests keep
// their hashes:
//
//	c := compact.NewLocal(model, compact.WithOnFold(rec.Fold))
//	cfg.Transform = c.Transform
//
// The recorder keeps the entry ID and the value of every item it wrote,
// aligned with the agent's working transcript, and names the entry at
// the fold's split as first_kept. [Resume] seeds that alignment from
// the context at the leaf, so an agent resumed from Context.Items
// records folds too; an agent seeded with a transcript the recorder did
// not write and did not resume from cannot, and Fold returns an error,
// which fails the turn rather than let the record drift.
//
// # Following the agent
//
// [Recorder.Attach] follows the agent: at every run_start the recorder
// takes the agent's configuration again, so Agent.SetConfig needs no
// consumer action, and the filter that decides which items the model
// saw is the configuration's. [WithFilter] serves [Recorder.Handle]
// used without an agent, and a configuration whose Filter is nil.
//
// # Branching, continuing and annotating
//
// A session branches by moving its leaf; a recorder writing it has to
// be told, or it keeps writing deltas against the branch it left.
// [Recorder.Rebase] moves the leaf and reseeds the recorder from the
// context there, as [Resume] seeds it from the leaf at open; it appends
// nothing and refuses while a run is active. Rebase with the empty
// entry ID is the reset a /clear makes: the next entry starts a new
// root, and the recorder forgets the items and the settings of the
// branch it left, so the root opens with a full config and its
// responses carry hashes. A conversation that
// outgrows its file rolls over with [Continue], which creates the
// successor with agentsession.Continue and returns a recorder seeded
// from it; the successor's context begins with the summary, and the
// agent that continues it is seeded with that context. A link is never
// a context edge: a subsession runs on a fresh transcript and its
// session is self-contained, a conversation continuing under new
// settings within one session is a config entry, and a rollover is a
// successor whose first entries carry what it needs.
//
// [Recorder.Annotate] appends a custom entry at the current leaf of the
// session of the run on the context, the recorder's own when none is,
// so a consumer records its own state, a render manifest or a policy's
// verdict, next to the turn it describes without reproducing the
// recorder's parenting. An annotation made from a subscriber during
// turn_start lands before that turn's config entry, whatever the
// subscriber's registration order: the recorder holds the settle it
// computed on turn_start until the next event of the turn.
package session

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
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

// FoldMember is the member a compaction entry carries, beyond those the
// format defines, naming the fold's own model call. Its value is a
// [FoldCall]. Readers of the format preserve members they do not
// define, and Session.Verify never reads it: the request it hashes is
// the fold's, which no path rebuilds.
const FoldMember = "fold"

// FoldCall names the model call a fold made.
type FoldCall struct {
	// RequestHash is the hash, in the format's canonical form, of the
	// request the fold sent, for the local summary fold; empty for the
	// compaction endpoint.
	RequestHash string `json:"request_hash,omitempty"`
	// ResponseID is the ID of the response the call produced.
	ResponseID string `json:"response_id,omitempty"`
	// Model is the model the request named.
	Model string `json:"model,omitempty"`
}

// ErrRunActive is returned by [Recorder.Rebase] while a run is being
// written.
var ErrRunActive = errors.New("session: a run is active")

// Recorder is an agentturn subscriber that writes one session, and the
// sessions of the child runs it observes. It is safe to attach to one
// agent at a time; events of a run arrive from one goroutine, and
// Observe may be called from the goroutines of parallel child tools.
type Recorder struct {
	store    agentsession.Store
	filter   func(agentturn.Transcript) agentturn.Transcript
	harness  *agentsession.Harness
	children bool
	env      func(context.Context) (*agentsession.EnvEntry, error)
	now      func() time.Time
	// agent is the agent Attach subscribed to, whose configuration is
	// taken again at every run_start.
	agent *agentturn.Agent

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
	// settleReq is the request whose settings the next entry must
	// follow: turn_start computes it and the next event of the turn
	// writes it, so an annotation made during turn_start lands first.
	settleReq *openresponses.Request
	// inFlight is set from turn_start to the response; pending is the
	// hash of the request in flight, empty when the recorder cannot
	// stand behind it; started is when it was sent; inFlightID is the
	// ID of the response streaming it, learned from the items it
	// produced, so a call that never reached its response entry can
	// still be written naming the response its items belong to.
	inFlight   bool
	pending    string
	started    time.Time
	inFlightID string
	// items holds the ID of the entry contributing each item of the
	// agent's working transcript, in order, so a fold's split index
	// names the first kept entry; values holds the items themselves and
	// custom says which were written as custom entries, outside the
	// context, so the input the path rebuilds can be compared with a
	// request's.
	items  []string
	values openresponses.Items
	custom []bool
	// foldSet says the last fold recorded replaces the first foldSplit
	// items with foldSummary in the rebuilt context.
	foldSet     bool
	foldSplit   int
	foldSummary openresponses.Item
	// calls maps a call ID to what the path holds for it, for the calls
	// this writer wrote or Resume found pending.
	calls map[string]*callRecord
	// linked marks the calls whose subsession link is written.
	linked map[string]bool
	// env is the last env entry written, encoded, so the next is
	// written only when it differs.
	env []byte

	// run is the ID of the run being written, "" between runs; open
	// lists, in order, the calls of that run with no output yet;
	// responses counts its model calls and lastCalls says whether the
	// last one requested tools, which is what the end reason turns on.
	run       string
	open      []string
	responses int
	lastCalls bool
}

// callRecord is what the path holds for one function call.
type callRecord struct {
	// entry is the ID of the item entry holding the call.
	entry string
	// args are the arguments as the model wrote them.
	args string
	// held is set while the latest decision is a hold that nothing has
	// answered; dispatched once a dispatch is written; rejected once a
	// reject is.
	held       bool
	dispatched bool
	rejected   bool
}

// Option configures a Recorder.
type Option func(*Recorder)

// WithFilter sets the filter that decides which items the model saw
// when the recorder has no configuration to take it from: [Handle]
// without an agent, or a configuration whose Filter is nil. Items it
// drops are written as custom entries rather than item entries. The
// default is agentturn.DefaultFilter. A recorder attached to an agent
// takes the filter from the agent's configuration at every run.
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

// WithEnv sets a function the recorder calls once per run, on
// run_start, for the environment the run works in: the working
// directory, the version control state, file hashes, tool versions,
// the workspace. The entry is written when it differs from the last
// one written, or found on the path by [Resume], so a run in an
// unchanged environment adds nothing; a nil entry writes nothing. The
// recorder gathers nothing itself: what the host knows about its
// environment is the host's to supply, and the recorder stays free of
// the file system. An error fails the run. Only the recorder's own
// session gets env entries; a child session inherits its parent's
// environment through parent_session.
func WithEnv(fn func(context.Context) (*agentsession.EnvEntry, error)) Option {
	return func(r *Recorder) { r.env = fn }
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
	r.root = newWriter(r, sessionID)
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func newWriter(r *Recorder, id string) *writer {
	return &writer{rec: r, id: id, calls: map[string]*callRecord{}, linked: map[string]bool{}}
}

// Start creates a session from h and returns a recorder for it. When
// h.Records is nil the header promises the run, dispatch and decision
// records, which this recorder writes whenever their event occurs.
// Child sessions name h.Harness as their writer unless [WithHarness]
// says otherwise. The caller keeps store for Sync, Release and Close.
func Start(ctx context.Context, store agentsession.Store, h agentsession.Header, opts ...Option) (*Recorder, *agentsession.Session, error) {
	if h.Records == nil {
		h.Records = append([]string(nil), agentsession.AllRecords...)
	}
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
// than a full copy on every resume; from the items of the context
// there, so an agent seeded with Context.Items can record folds and
// its requests carry hashes; from the calls pending there, so their
// dispatches and decisions anchor to the entries that hold them; and
// from the last env entry on the path. Child sessions name the
// header's harness unless [WithHarness] says otherwise.
func Resume(ctx context.Context, store agentsession.Store, sessionID string, opts ...Option) (*Recorder, *agentsession.Session, error) {
	s, err := store.Open(ctx, sessionID)
	if err != nil {
		return nil, nil, fmt.Errorf("session: open: %w", err)
	}
	return resume(s, store, opts)
}

// Continue rolls the session with the given ID over into a successor
// through agentsession.Continue: a new session with parent_session
// naming the old one, a full config carrying the settings in force at
// the old leaf, summary as its first item when not nil, and a
// continued_in link on the old session, which lists it as superseded.
// It returns a recorder seeded from the successor as [Resume] seeds
// one, and the successor, whose Context().Items is what the agent that
// continues the conversation should be seeded with. The old session's
// recorder, if any, should be detached first; a store that holds the
// old session open may need it released before it can be continued.
func Continue(ctx context.Context, store agentsession.Store, sessionID string, summary openresponses.Item, opts ...Option) (*Recorder, *agentsession.Session, error) {
	next, err := agentsession.Continue(ctx, store, sessionID, summary)
	if err != nil {
		return nil, nil, fmt.Errorf("session: continue: %w", err)
	}
	return resume(next, store, opts)
}

func resume(s *agentsession.Session, store agentsession.Store, opts []Option) (*Recorder, *agentsession.Session, error) {
	r := New(store, s.ID(), opts...)
	if r.harness == nil {
		r.harness = s.Header().Harness
	}
	if err := r.root.seed(s); err != nil {
		return nil, nil, err
	}
	return r, s, nil
}

// Rebase moves the session's leaf to entryID and reseeds the recorder
// from the context there, as [Resume] seeds it from the leaf at open,
// so the next run records against the settings, items and pending
// calls of the branch it continues rather than the one it left. It
// appends nothing: a rebase is not a change to the record. It refuses
// with [ErrRunActive] while a run is being written, and s must be the
// session the recorder writes. The agent's transcript is the caller's
// to set, with Agent.SetTranscript from s.Context().Items.
//
// The empty entry ID is the reset a product's /clear makes: the
// session's leaf is reset so the next append starts a new root
// (agentsession.Session.ResetLeaf), and the recorder forgets the
// settings, the items and the calls of the branch it left, so the new
// root opens with a full config entry and the responses on it carry
// hashes again. It is the one rebase that changes what the next entry
// is, which is why it is spelled as the empty ID rather than left to a
// caller to arrange.
func (r *Recorder) Rebase(s *agentsession.Session, entryID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s.ID() != r.root.id {
		return fmt.Errorf("session: rebase: session %s is not the recorder's %s", s.ID(), r.root.id)
	}
	if r.root.run != "" {
		return ErrRunActive
	}
	if entryID == "" {
		s.ResetLeaf()
		r.root.reset()
		return nil
	}
	if err := s.Branch(entryID); err != nil {
		return fmt.Errorf("session: rebase: %w", err)
	}
	r.root.reset()
	return r.root.seed(s)
}

// Annotate appends a custom entry in namespace ns carrying data,
// encoded as JSON, at the current leaf of the session of the run on
// the context, or of the recorder's own session when the context
// names no run it is writing. It never contributes an item or a
// setting, so the context and the request hashes are untouched. Made
// from a subscriber during turn_start, the entry lands before that
// turn's config entry, whatever the order the subscribers were
// registered in; made during any later event of the turn, after it.
func (r *Recorder) Annotate(ctx context.Context, ns string, data any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	w := r.root
	if cw, ok := r.runs[agentturn.RunIDFromContext(ctx)]; ok {
		w = cw
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("session: encode annotation %s: %w", ns, err)
	}
	_, err = w.append(context.WithoutCancel(ctx), &agentsession.CustomEntry{NS: ns, Data: raw})
	return err
}

// reset clears what seed sets, keeping the writer's identity and its
// configuration.
func (w *writer) reset() {
	w.settings = agentsession.Settings{}
	w.wroteConfig = false
	w.settleReq = nil
	w.inFlight, w.pending, w.started, w.inFlightID = false, "", time.Time{}, ""
	w.items, w.values, w.custom = nil, nil, nil
	w.foldSet, w.foldSplit, w.foldSummary = false, 0, nil
	w.calls = map[string]*callRecord{}
	w.env = nil
}

// seed sets the writer's state from the session at its leaf.
func (w *writer) seed(s *agentsession.Session) error {
	if s.Leaf() == "" {
		return nil
	}
	cx, err := s.Context()
	if err != nil {
		return fmt.Errorf("session: context at leaf: %w", err)
	}
	w.settings = cx.Settings
	w.wroteConfig = hasConfig(cx.Entries)
	w.items = make([]string, len(cx.ItemEntries))
	for i, e := range cx.ItemEntries {
		w.items[i] = e.Base().ID
	}
	w.values = append(openresponses.Items(nil), cx.Items...)
	w.custom = make([]bool, len(w.values))
	for _, e := range cx.Entries {
		if env, ok := e.(*agentsession.EnvEntry); ok {
			w.env = envBody(env)
		}
	}
	pending, err := s.PendingCalls(s.Leaf())
	if err != nil {
		return fmt.Errorf("session: pending calls at leaf: %w", err)
	}
	for _, c := range pending {
		w.calls[c.ID()] = &callRecord{entry: c.Entry.Base().ID, args: c.Call.Arguments, held: c.Held(), dispatched: c.Dispatch != nil, rejected: c.Rejected()}
	}
	return nil
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
// function. The recorder takes the agent's configuration at every
// run_start from then on, so a change through Agent.SetConfig reaches
// the record as a config delta and as the filter in force.
func (r *Recorder) Attach(a *agentturn.Agent) (unsubscribe func()) {
	r.mu.Lock()
	r.agent = a
	cfg := a.Config()
	r.root.cfg = &cfg
	r.mu.Unlock()
	unsub := a.Subscribe(r.Handle)
	return func() {
		unsub()
		r.mu.Lock()
		if r.agent == a {
			r.agent = nil
		}
		r.mu.Unlock()
	}
}

// filter returns the filter in force for the writer's session: the
// configuration's when it has one, the recorder's otherwise.
func (w *writer) filter() func(agentturn.Transcript) agentturn.Transcript {
	if w.cfg != nil && w.cfg.Filter != nil {
		return w.cfg.Filter
	}
	return w.rec.filter
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
		if r.agent != nil {
			cfg := r.agent.Config()
			r.root.cfg = &cfg
		}
	}
	return r.root.handle(ctx, ev)
}

// Observe records one event of a child run made through tools/agent;
// register it with agent.WithObserver. On the child's run_start it
// creates the child's session, with parent_session naming the session
// of the run that called the tool, the run ID on the context, or this
// recorder's session when the tool ran outside a loop it records, and
// an ID derived from the parent's and the call's when the call is on
// the context, as tools/agent puts it; writes the link from the parent
// at once, since the call has been dispatched; and writes the child's
// configuration, which tools/agent puts on the context, as its first
// entry. Every later event of the run is written to that session as
// [Recorder.Handle] writes the agent's.
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
		callID := ""
		if call, ok := agenttool.CallFrom(ctx); ok {
			callID = call.ID
		}
		w, err := r.newChild(ctx, parent, callID, true)
		if err != nil {
			return
		}
		if cfg, ok := agent.ConfigFromContext(ctx); ok {
			w.cfg = &cfg
		}
		r.runs[e.RunID] = w
		if callID != "" {
			// The link is written at dispatch, which is now: the call
			// is running.
			_ = parent.link(ctx, w.id, callID)
		}
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
// the summary, the settings in force, the token estimate, the usage
// and the fold's own call under [FoldMember]; a fold that failed
// becomes a custom entry in [FailedFoldNS]. The run ID on the context
// says which session the fold belongs to, so a child's transform
// reports into the child's session; without one, the fold is the
// agent's. An error fails the transform, and so the turn.
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

// newChild creates a session under parent and returns its writer. With
// a call ID the session's ID is derived from the parent's and the
// call's, so a reader can compute it from the parent's link alone, and
// a second child for the same call, a retry, continues the existing
// session from a new root. live says the child is written from its
// events, so the header promises the record entries; a child replayed
// from its items promises nothing.
func (r *Recorder) newChild(ctx context.Context, parent *writer, callID string, live bool) (*writer, error) {
	h := agentsession.Header{ParentSession: parent.id, Harness: r.harness, SpawnedBy: callID}
	if live {
		h.Records = append([]string(nil), agentsession.AllRecords...)
	}
	if callID != "" {
		h.ID = agentsession.SubsessionID(parent.id, callID)
	}
	s, err := r.store.Create(ctx, h)
	if errors.Is(err, agentsession.ErrSessionExists) {
		if s, err = r.store.Open(ctx, h.ID); err == nil {
			s.ResetLeaf()
			return newWriter(r, s.ID()), nil
		}
		// The store cannot reopen it; a fresh session still records
		// the retry, under a minted ID.
		h.ID = ""
		s, err = r.store.Create(ctx, h)
	}
	if err != nil {
		return nil, fmt.Errorf("session: create child session: %w", err)
	}
	return newWriter(r, s.ID()), nil
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
	case *agentturn.ModelBlocked:
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

// handle writes the entry for one event of the writer's run. Any
// settle held since turn_start is written first, so an annotation made
// during turn_start precedes it.
func (w *writer) handle(ctx context.Context, ev agentturn.Event) error {
	if _, ok := ev.(*agentturn.TurnStart); !ok {
		if err := w.flush(ctx); err != nil {
			return err
		}
	}
	switch e := ev.(type) {
	case *agentturn.RunStart:
		return w.runStart(ctx, e)
	case *agentturn.TurnStart:
		return w.turnStart(ctx, e)
	case *agentturn.ModelBlocked:
		return w.blocked(ctx, e)
	case *agentturn.ItemEnd:
		return w.item(ctx, e.Item, e.ResponseID, e.Hidden)
	case *agentturn.ResponseEnd:
		return w.response(ctx, e)
	case *agentturn.ToolStart:
		return w.toolStart(ctx, e)
	case *agentturn.RunEnd:
		return w.runEnd(ctx, e)
	case *agentturn.ToolEnd:
		return w.toolEnd(ctx, e)
	}
	return nil
}

// toolEnd writes what a call's result carries for the record: a child
// run's link, and the tool's own side data when its Details value is
// an agenttool.Recordable, as a custom entry in the namespace the value
// names, between the call's dispatch and its output.
func (w *writer) toolEnd(ctx context.Context, e *agentturn.ToolEnd) error {
	if info, ok := e.Result.Details.(agent.ChildInfo); ok && w.rec.children {
		return w.child(ctx, e.CallID, info)
	}
	rec, err := agenttool.RecordOf(e.Result.Details)
	if err != nil {
		return fmt.Errorf("session: call %s: %w", e.CallID, err)
	}
	if rec == nil {
		return nil
	}
	_, err = w.append(ctx, &agentsession.CustomEntry{NS: rec.NS, Data: rec.Data})
	return err
}

// runStart opens the run on the record: the run entry, the env when
// the host supplies one and it changed, and the initial config from
// the agent's configuration when the writer has one and nothing has
// been written yet.
func (w *writer) runStart(ctx context.Context, e *agentturn.RunStart) error {
	w.run = e.RunID
	w.open = nil
	w.responses = 0
	w.lastCalls = false
	if _, err := w.append(ctx, agentsession.NewRunStart(e.RunID, string(e.Source), e.Trigger.String())); err != nil {
		return err
	}
	if w == w.rec.root && w.rec.env != nil {
		if err := w.writeEnv(ctx); err != nil {
			return err
		}
	}
	if w.wroteConfig || w.cfg == nil {
		return nil
	}
	req := w.cfg.BaseRequest(ctx)
	return w.settle(ctx, Canonical(req))
}

// writeEnv asks the host for the environment and writes it when it
// differs from the last one written.
func (w *writer) writeEnv(ctx context.Context) error {
	env, err := w.rec.env(ctx)
	if err != nil {
		return fmt.Errorf("session: env: %w", err)
	}
	if env == nil {
		return nil
	}
	data := envBody(env)
	if bytes.Equal(data, w.env) {
		return nil
	}
	if _, err := w.append(ctx, env); err != nil {
		return err
	}
	w.env = data
	return nil
}

// envBody encodes an env entry without its envelope, so one read from
// the path compares equal to a fresh one with the same content.
func envBody(env *agentsession.EnvEntry) []byte {
	body := *env
	body.EntryBase = agentsession.EntryBase{}
	data, err := json.Marshal(&body)
	if err != nil {
		return nil
	}
	return data
}

// turnStart holds the settle for the request and takes the hash the
// response will carry, when the recorder can stand behind it.
func (w *writer) turnStart(ctx context.Context, e *agentturn.TurnStart) error {
	if err := w.flush(ctx); err != nil {
		return err
	}
	req := Canonical(e.Request)
	hash, err := w.hash(req)
	if err != nil {
		return err
	}
	w.settleReq = &req
	w.inFlight = true
	w.pending = hash
	w.started = w.rec.now()
	w.inFlightID = ""
	return nil
}

// flush writes the settle held since turn_start, if any.
func (w *writer) flush(ctx context.Context) error {
	if w.settleReq == nil {
		return nil
	}
	req := *w.settleReq
	w.settleReq = nil
	return w.settle(ctx, req)
}

// hash returns the request's hash when its input is what the stored
// path rebuilds, "" otherwise.
func (w *writer) hash(req openresponses.Request) (string, error) {
	expected, ok := w.expectedInput()
	if !ok || !equalJSON(expected, req.Input) {
		return "", nil
	}
	return RequestHash(req)
}

// expectedInput is the input the stored path rebuilds, as the context
// algorithm reads it: the summary of the last fold recorded, then the
// items written as item entries from the fold's first kept one on.
func (w *writer) expectedInput() (openresponses.Items, bool) {
	from := 0
	var out openresponses.Items
	if w.foldSet {
		if w.foldSplit > len(w.values) {
			return nil, false
		}
		from = w.foldSplit
		out = append(out, w.foldSummary)
	}
	for i := from; i < len(w.values); i++ {
		if !w.custom[i] {
			out = append(out, w.values[i])
		}
	}
	return out, true
}

// blocked records a call BeforeModelCall refused: the settings the
// request was built under, then a failed response carrying the hook's
// error and the request's hash, with no response ID since no call was
// made.
func (w *writer) blocked(ctx context.Context, e *agentturn.ModelBlocked) error {
	req := Canonical(e.Request)
	hash, err := w.hash(req)
	if err != nil {
		return err
	}
	if err := w.settle(ctx, req); err != nil {
		return err
	}
	w.responses++
	w.lastCalls = false
	_, err = w.append(ctx, &agentsession.ResponseEntry{
		Status:      openresponses.ResponseStatusFailed,
		Error:       errorPayload(e.Err),
		RequestHash: hash,
	})
	return err
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
// did not see, and records its ID against the working transcript. A
// function call is remembered so its records can name the entry; an
// output the caller wrote for a call nothing dispatched is preceded by
// the reject that ends it; an item the caller marked with
// agentturn.Hidden is written with visible false.
func (w *writer) item(ctx context.Context, item openresponses.Item, responseID string, hidden bool) error {
	if item == nil {
		return nil
	}
	if out, ok := item.(*openresponses.FunctionCallOutput); ok {
		if c := w.calls[out.CallID]; c != nil && !c.dispatched && !c.rejected {
			// The caller wrote the output themselves: the call never
			// reached its tool, and the output is what the model sees.
			// A held call is the common case, but a call seeded from a
			// path that holds its item and no decision, the shape a
			// branch leaves when the dispatch is on the branch that was
			// left, is the same thing to a reader and gets the same
			// decision, which is what the format's record check asks
			// for.
			dec := agentsession.NewDecision(out.CallID, c.entry, agentsession.VerdictReject, agentturn.DeciderFromContext(ctx, out.CallID)).WithReason(outputText(out))
			if _, err := w.append(ctx, dec); err != nil {
				return err
			}
			c.held, c.rejected = false, true
		}
	}
	var entry agentsession.Entry
	// An item the filter in force drops never reached the model, so it
	// is outside the context and outside the item entries; the hidden
	// mark is about a renderer and adds nothing to it.
	appOnly := responseID == "" && len(w.filter()(agentturn.Transcript{item})) == 0
	if appOnly {
		raw, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("session: encode %s item: %w", item.ItemType(), err)
		}
		entry = &agentsession.CustomEntry{NS: item.ItemType(), Data: raw}
	} else {
		e := &agentsession.ItemEntry{Item: item, ResponseID: responseID}
		if hidden {
			visible := false
			e.Visible = &visible
		}
		entry = e
	}
	if w.inFlight && responseID != "" {
		// The stream named the response before it ended. A call cut off
		// after this item is written as a failed response carrying the
		// ID, so the context algorithm strips the items it produced
		// rather than reading them as its input.
		w.inFlightID = responseID
	}
	id, err := w.append(ctx, entry)
	if err != nil {
		return err
	}
	w.items = append(w.items, id)
	w.values = append(w.values, item)
	w.custom = append(w.custom, appOnly)
	switch v := item.(type) {
	case *openresponses.FunctionCall:
		w.calls[v.CallID] = &callRecord{entry: id, args: v.Arguments}
		w.open = append(w.open, v.CallID)
	case *openresponses.FunctionCallOutput:
		w.close(v.CallID)
	}
	return nil
}

// close removes a call from the run's open list.
func (w *writer) close(callID string) {
	for i, id := range w.open {
		if id == callID {
			w.open = append(w.open[:i:i], w.open[i+1:]...)
			return
		}
	}
}

// outputText is the text of an output, for a reject's reason.
func outputText(out *openresponses.FunctionCallOutput) string {
	if out.Output.Text != "" || out.Output.Parts == nil {
		return out.Output.Text
	}
	data, err := json.Marshal(out.Output.Parts)
	if err != nil {
		return ""
	}
	return string(data)
}

// toolStart writes what was decided about the call, when something
// was, and its dispatch when it goes to its tool.
func (w *writer) toolStart(ctx context.Context, e *agentturn.ToolStart) error {
	c := w.calls[e.CallID]
	if c == nil {
		// A call this recorder did not write and did not find pending:
		// there is no entry to anchor a record to.
		return nil
	}
	d := e.Decision
	by := ""
	if d != nil {
		by = d.By
		if by == "" && !c.held {
			by = agentsession.ByPolicy
		}
	}
	if d != nil {
		switch d.Action {
		case agentturn.Block:
			reason := d.Reason
			if reason == "" {
				reason = "call blocked"
			}
			if _, err := w.append(ctx, agentsession.NewDecision(e.CallID, c.entry, agentsession.VerdictReject, by).WithReason(reason)); err != nil {
				return err
			}
			c.held, c.rejected = false, true
			return nil
		case agentturn.Defer:
			// The reason is which rule raised the prompt, which is what
			// an auditor asks of a hold; the model never sees it.
			hold := agentsession.NewDecision(e.CallID, c.entry, agentsession.VerdictHold, by)
			if d.Reason != "" {
				hold.WithReason(d.Reason)
			}
			if _, err := w.append(ctx, hold); err != nil {
				return err
			}
			c.held = true
			return nil
		}
	}
	if c.rejected {
		return nil
	}
	if rewritten := !sameJSON(e.Args, c.args); c.held || rewritten {
		dec := agentsession.NewDecision(e.CallID, c.entry, agentsession.VerdictProceed, by)
		if rewritten {
			dec.WithArgs(e.Args)
		}
		if _, err := w.append(ctx, dec); err != nil {
			return err
		}
	}
	if _, err := w.append(ctx, agentsession.NewDispatch(e.CallID, c.entry)); err != nil {
		return err
	}
	c.held, c.dispatched = false, true
	return nil
}

// sameJSON reports whether two argument strings are the same object,
// an empty string standing for the empty object as the loop reads it.
func sameJSON(a json.RawMessage, b string) bool {
	if len(a) == 0 {
		a = json.RawMessage("{}")
	}
	if b == "" {
		b = "{}"
	}
	var ca, cb bytes.Buffer
	if json.Compact(&ca, a) != nil || json.Compact(&cb, []byte(b)) != nil {
		return string(a) == b
	}
	return ca.String() == cb.String()
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
	w.inFlight, w.pending, w.started, w.inFlightID = false, "", time.Time{}, ""
	w.responses++
	w.lastCalls = len(resp.FunctionCalls()) > 0
	_, err := w.append(ctx, entry)
	return err
}

// runEnd closes the run on the record. A call that was sent and never
// answered, because the model failed before producing a response or
// the run was aborted mid-stream, is written first as a failed
// response with the error, the request hash and the ID of the response
// the stream had already named, so the record shows the call was made
// and why it ended and the context algorithm strips the items it
// produced rather than reading them as its input; the in-flight state is cleared
// either way, so a writer reused for a later run cannot attribute its
// first response to this call. Then the run's end entry, with the
// reason in the format's terms and the run's calls left open.
func (w *writer) runEnd(ctx context.Context, e *agentturn.RunEnd) error {
	hash, inFlight, responseID := w.pending, w.inFlight, w.inFlightID
	latency := w.latency()
	w.inFlight, w.pending, w.started, w.inFlightID = false, "", time.Time{}, ""
	if inFlight {
		w.responses++
		w.lastCalls = false
		entry := &agentsession.ResponseEntry{
			ResponseID:  responseID,
			Status:      openresponses.ResponseStatusFailed,
			Error:       errorPayload(e.Err),
			RequestHash: hash,
			LatencyMS:   latency,
		}
		if _, err := w.append(ctx, entry); err != nil {
			return err
		}
	}
	if w.run == "" {
		return nil
	}
	reason, ref := w.endReason(e)
	entry := agentsession.NewRunEnd(w.run, reason, ref, append([]string(nil), w.open...))
	w.run, w.open = "", nil
	_, err := w.append(ctx, entry)
	return err
}

// endReason maps the loop's reason onto the format's cascade, with the
// loop's own reason as ref whenever the two differ and the error as
// ref for a failure or an interruption.
func (w *writer) endReason(e *agentturn.RunEnd) (reason, ref string) {
	switch e.Reason {
	case agentturn.ReasonDone:
		return agentsession.ReasonDone, ""
	case agentturn.ReasonInputRequired:
		return agentsession.ReasonInputRequired, ""
	case agentturn.ReasonError:
		return agentsession.ReasonError, errText(e.Err)
	case agentturn.ReasonAborted:
		// The host asked for the stop, through Abort or its context.
		return agentsession.ReasonInterrupted, errText(e.Err)
	case agentturn.ReasonStopped:
		// The cause is the ref throughout: what stopped the run is what a
		// reader asks, whichever shape the segment has.
		switch {
		case w.responses == 0:
			// A resume whose approved batch terminated, or a refusal on
			// Resume: the segment has no response, which the format reads
			// as aborted.
			return agentsession.ReasonAborted, string(e.Cause)
		case w.lastCalls:
			return agentsession.ReasonStopped, string(e.Cause)
		}
		// A guard or a turn budget stopped a run whose last response
		// requested nothing, which the format reads as done.
		return agentsession.ReasonDone, string(e.Cause)
	}
	return string(e.Reason), ""
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
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
	w.foldSet, w.foldSplit, w.foldSummary = true, f.Split, f.Summary
	entry := &agentsession.CompactionEntry{
		FirstKept:    w.items[f.Split],
		Summary:      f.Summary,
		Config:       w.settings,
		TokensBefore: f.TokensBefore,
		Usage:        f.Usage,
	}
	if f.Request != nil || f.ResponseID != "" {
		call := FoldCall{ResponseID: f.ResponseID}
		if f.Request != nil {
			hash, err := RequestHash(Canonical(*f.Request))
			if err != nil {
				return err
			}
			call.RequestHash = hash
			call.Model = f.Request.Model
		}
		raw, err := json.Marshal(call)
		if err != nil {
			return fmt.Errorf("session: encode fold call: %w", err)
		}
		entry.Unknown = map[string]json.RawMessage{FoldMember: raw}
	}
	_, err := w.append(ctx, entry)
	return err
}

// child links the session of a child run to this one. A run that was
// observed has its session already, and its link when the call was on
// the observer's context; its writer is released. One that was not
// observed gets a session holding only its items.
func (w *writer) child(ctx context.Context, callID string, info agent.ChildInfo) error {
	r := w.rec
	if cw, ok := r.runs[info.RunID]; ok {
		delete(r.runs, info.RunID)
		return w.link(ctx, cw.id, callID)
	}
	cw, err := r.newChild(ctx, w, callID, false)
	if err != nil {
		return err
	}
	for _, item := range info.Items {
		responseID := ""
		if isModelOutput(item) {
			responseID = info.RunID
		}
		if err := cw.item(ctx, item, responseID, false); err != nil {
			return err
		}
	}
	return w.link(ctx, cw.id, callID)
}

// link writes the subsession link for callID once.
func (w *writer) link(ctx context.Context, childID, callID string) error {
	if w.linked[callID] {
		return nil
	}
	if _, err := w.append(ctx, agentsession.NewSubsessionLink(childID, callID)); err != nil {
		return err
	}
	w.linked[callID] = true
	return nil
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
