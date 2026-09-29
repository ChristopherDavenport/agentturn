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
//     reports (input, or resume for a run that answers pending calls),
//     the [agentturn.Trigger] on the context joined as ref and in its
//     parts as trigger, and, when the recorder knows the agent's
//     configuration ([Recorder.Attach] and [WithConfig] give it one),
//     the hash of its base request as [ConfigBaseMember]; then, for
//     the recorder's own session, the env entry [WithEnv] supplies
//     when it differs from the last one written, members the library
//     does not define included; then, with a configuration, a full
//     config entry before the first item, so a root starts with one as
//     the format recommends, and a delta when the configuration
//     changed since the last run, so the items a new configuration's
//     BeforeTurn appends are filed under it. A recorder seeded from a
//     path, by [Resume], a [Start] on a based header,
//     [Recorder.Rebase], [Continue] or a child session
//     reopened under the same call, compares its first run with the
//     base the path's last run start recorded, or, on a path that
//     recorded none, with the settings at the leaf.
//   - turn_start: a config entry when the settings in force changed
//     since the last call (a full one first if none was written, deltas
//     after, a tool list change as tools_added and tools_removed), so
//     the stored path replays to the request's settings. With
//     [WithInstructionsParts] the instructions are written as the parts
//     they are composed of, a delta naming only the parts that moved,
//     and what the host left out as instructions_omitted.
//   - model_retry: a custom entry in the [ModelRetryNS] namespace
//     saying which attempt failed, why, how long the loop waited and
//     whether Retry.Revise changed the request; and the settings of the
//     request the next attempt will send, in place of the ones the
//     attempt that failed carried, since that attempt answered nothing.
//     A Retry.Revise that moves the turn to another model is then a
//     config delta, and the path names the model that answered.
//   - model_blocked: the config settle for the request that was built,
//     then a response entry with status failed, the hook's error and
//     the request hash, so a call a BeforeModelCall hook refused is on
//     the record and distinct from one that was made and failed; for a
//     guard's stop, an error wrapping agentturn.ErrGuard, a custom
//     entry in [ModelBlockedNS] carrying the same, since the run was
//     stopped rather than failed.
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
//     dispatch behind is the same thing to a reader. A call the loop
//     refused itself, for a name no tool has or arguments that are not
//     an object, is the same shape with policy as the decider. An
//     output the caller wrote for a call an earlier run dispatched,
//     the [agentturn.OutcomeUnknown] answer to a call that may have
//     run, has no decision before it: the output is the record, and
//     the absence of a second dispatch says the tool did not run again.
//   - tool_start: the call's decision when there is one to record. A
//     BeforeToolCall that blocked the call is a reject decision with
//     its reason; one that deferred it is a hold carrying the same
//     reason, the rule that raised the prompt; one that rewrote the
//     arguments, one that allowed the call and gave a reason, such as
//     the grant that allowed it, or an approval through Agent.Resume of
//     a held call, is a proceed decision carrying the arguments the
//     tool ran with when they differ from the model's and the reason
//     when there is one. The decision's by is
//     ToolDecision.By, which Answer.By sets for an approval, and policy
//     for a hook's decision about a call nothing was holding; an answer
//     that names nobody is written with no by, since a policy engine
//     answers through Resume as often as a person does. An approval's
//     Answer.Reason is the proceed's reason, which is how a call run
//     again after a restart says why: [ReplayAnswers] gives "run again:
//     safe" or "run again: keyed".
//   - tool_dispatch: the call's dispatch, written as the loop hands the
//     call to its tool and not before, so a call the cut reached first,
//     one waiting for a slot in the bound or its turn in a serial batch,
//     has none and reads as never started. The loop raises it on the
//     call's own goroutine and the barrier holds it there, so the
//     dispatch is durable before the tool runs, and a write that fails
//     stops the call. It carries the idempotency key the tool receives
//     as an idempotency_key member, which the format does not yet
//     define and keeps as written. A call run again after a restart
//     gets a second dispatch, so the path holds one per hand-off to
//     the tool, and a reader counts the times it may have run.
//   - response_end: the response entry with status, usage, error and the
//     request hash, after the items it produced and before any tool
//     output of the turn; for a call Retry tried again, attempts, the
//     calls it took with the one that answered.
//   - run_end: a response entry with status failed, the error, the
//     request hash, attempts as on response_end, an abort during a
//     retry's backoff counting the attempt that was due, and the ID of
//     the response the stream had named,
//     when a call was still in flight because the model failed or the
//     run was aborted before the stream produced a response; the ID
//     names the response the call was cut out of and lets the context
//     algorithm strip the items it appended, rather than read them as
//     its input; then the run entry with
//     phase end, the reason in the
//     format's terms and the IDs of the run's calls left without an
//     output. The loop's reasons map onto the format's cascade: done
//     and input_required as they are, error with the error as ref,
//     aborted as interrupted with the context error as ref, since the
//     host asked for the stop, and stopped as stopped when the last
//     response requested tools, as done when it did not, and, when the
//     run made no model call of its own, as stopped when it answered a
//     call an earlier one made and left nothing pending and as aborted
//     otherwise, with the stop's cause as ref in every case, followed
//     for a guard's stop by the guard's error.
//   - queued: a queued entry for each input [agentturn.Agent] accepted
//     into a queue, with its mode and the trigger it was queued with,
//     before anything appends it; the item entry that appends it names
//     the queued entry in queued_from and carries the trigger as
//     source. A run end closes the queued entries of the inputs the run
//     did not append, and since the agent still holds them the recorder
//     writes them again after the end, so the path says what is owed.
//     [Resume] writes again, after the end it gives a cut run, what
//     that run owed, and [Recorder.Requeue] hands it back to the agent;
//     an input nobody takes up is closed by the next run end, which is
//     how a host declines one. A rewind or a fork into a run leaves
//     what that run owed behind. An input the agent accepted while idle
//     is reported, and so written, at the start of the next run; a
//     host that must not lose one between runs queues it through
//     [Recorder.Queue], which writes it first. An
//     input the filter keeps from the model is a custom entry, which
//     cannot name its queued entry; the run's end closes that one.
//   - a fold reported through [Recorder.Fold]: a compaction entry whose
//     first_kept is the entry of the first item the transform kept, with
//     the summary and the settings in force, and a fold member naming
//     the fold's own model call by response ID, model and request hash;
//     that hash is of the fold's request, which no path rebuilds, and
//     is kept so a replay can recognise the call. A fold that pinned
//     items, compact.WithPin, writes them to the entry's pinned member,
//     where the context algorithm places them after the summary as the
//     request carries them, so the calls after such a fold keep their
//     hashes and the pinned items are in the context a resume seeds
//     from. A fold that failed is
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
//   - tool_start and tool_end of a call a tool made through
//     agentturn.Invoke: a custom entry in the [NestedCallNS] namespace
//     for each, carrying the parent call, the name, the arguments and
//     what a hook decided, then the outcome. A nested call has no
//     function_call item for a dispatch or a decision to name, so until
//     the format has a word for one these carry what a reader needs to
//     count the calls a turn ran.
//   - tool_end whose Result.Details implements agenttool.Recordable: a
//     custom entry in the namespace the value names, carrying its JSON,
//     between the call's dispatch and its output, with call_id naming
//     the call.
//   - a record a tool writes while it runs, through [Recorder.RecordFunc],
//     and a question it asks the user, through [Recorder.Elicitor]: a
//     custom entry at the leaf whose call_id names the call that wrote
//     it, or for a nested call the call whose tool made it, so the
//     records of a parallel batch say whose each one is. This is how a tool
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
// tool_dispatch only after the dispatch is appended, and the executor
// does not run the tool before then.
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
// is linked from the child's session. The child's session inherits the
// parent's working directory, so a store that buckets sessions by
// directory files it with its parent, and a second run under the same
// call continues it at its leaf rather than starting a new root, since
// a subagent that is messaged again answers from its own context;
// agent.ContextWithRetry says the other thing.
//
// [Recorder.ChildContext] puts the child's session ID on the context
// the child run is given, so a layer inside the child that attributes
// its writes to a session names the child's rather than the parent's:
//
//	specialist := agent.New(childCfg,
//		agent.WithObserver(rec.Observe),
//		agent.WithRunContext(rec.ChildContext))
//
// The host does the same for its own runs with
// [ContextWithSessionID](ctx, rec.SessionID()); [SessionIDFromContext]
// reads whichever is in force.
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
// a hash, and Session.Verify reports it with agentsession.ErrNoHash
// rather than as a mismatch. A host that gates on the record therefore
// tells "nothing was checked" from "checked and correct" with
// errors.Is and not with err == nil. For the compact transform,
// [Recorder.Fold] writes the
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
// A path on which a run is open that no recorder is running, because a
// crash cut it off or a rewind or a fork branched into it, is closed
// by the recorder that continues it before anything else is written,
// as the format has it: [Resume] closes a cut run with reason error,
// and [Recorder.Rebase] and a [Start] on a base inside a run close it
// interrupted, each with a ref saying why.
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

// UnplacedFoldNS is the namespace of the custom entry written for a fold
// the recorder cannot place on the path: its first kept item is not
// one entry the recorder wrote, after a transform chained before the
// fold replaced the items, or when one item value was appended twice.
// Its data is an [UnplacedFold]. The requests after it carry no hash,
// since the path does not rebuild what they sent.
const UnplacedFoldNS = "agentturn:compaction_unplaced"

// UnplacedFold is the data of an [UnplacedFoldNS] custom entry.
type UnplacedFold struct {
	Reason       string `json:"reason"`
	TokensBefore int    `json:"tokens_before,omitempty"`
	ResponseID   string `json:"response_id,omitempty"`
}

// FoldMember is the member a compaction entry carries, beyond those the
// format defines, naming the fold's own model call. Its value is a
// [FoldCall]. Readers of the format preserve members they do not
// define, and Session.Verify never reads it: the request it hashes is
// the fold's, which no path rebuilds.
const FoldMember = "fold"

// ConfigBaseMember is the member a run start entry carries, beyond
// those the format defines, when the recorder knows the configuration
// the run starts under: the request hash of that configuration's
// canonical base request, its tool provider's tools left out. A
// recorder seeded from a path compares its first run's configuration
// with it, as it compares a later run's with the one before, so a
// configuration that did not change writes nothing at run_start. It
// is a hash of settings the recorder compares, not of a request that
// was sent, and Session.Verify never reads it.
const ConfigBaseMember = "config_base"

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

// NestedCallNS is the namespace of the custom entries written for a
// call a tool made through [agentturn.Invoke]. Its data is a
// [NestedCall]: one entry with phase start before the call runs,
// carrying the name, the arguments and what a hook decided, and one
// with phase end carrying the outcome.
//
// A nested call has no function_call item in the transcript, because it
// is the work of the call that made it, and the format's dispatch and
// decision entries name the entry holding the call. These entries carry
// what the format has no word for yet, so a reader that counts the
// calls a turn ran, or an auditor asking what a code-execution tool did
// with the agent's other tools, has the same facts the model's own
// calls leave behind.
const NestedCallNS = "agentturn:nested_call"

// NestedCall is the data of a [NestedCallNS] custom entry.
type NestedCall struct {
	// Phase is start before the call runs and end after it.
	Phase string `json:"phase"`
	// CallID is the ID the loop minted for the nested call and Parent
	// the call whose tool made it.
	CallID string `json:"call_id"`
	Parent string `json:"parent"`
	Name   string `json:"name"`
	// Args are the arguments the tool ran with, on the start entry.
	Args json.RawMessage `json:"args,omitempty"`
	// Verdict, Reason and By are what a hook decided about the call,
	// in the format's terms, on the start entry.
	Verdict string `json:"verdict,omitempty"`
	Reason  string `json:"reason,omitempty"`
	By      string `json:"by,omitempty"`
	// Output is the text the caller received and Error the failure, on
	// the end entry.
	Output string `json:"output,omitempty"`
	Error  string `json:"error,omitempty"`
}

// ModelRetryNS is the namespace of the custom entry written for each
// failed attempt of a model call that [agentturn.Retry] tries again.
// Its data is a [ModelRetry]. It is appended where the next entry
// goes, before the response of the attempt that answered, whose
// response entry carries the count as attempts, so a reader counting
// calls reads that and needs no knowledge of this namespace; the entry
// is what the count cannot say, each failure, its delay and its model,
// and whether Retry.Revise changed the request. It lands
// before the config entry of the turn, which is written with the first
// entry of the attempt that answers, so only that attempt's settings
// are on the path; on a fresh session with no configuration to settle
// at run_start, that can put it before the first config entry.
const ModelRetryNS = "agentturn:model_retry"

// ModelRetry is the data of a [ModelRetryNS] custom entry.
type ModelRetry struct {
	// Attempt is the number of the attempt that failed, from 1.
	Attempt int `json:"attempt"`
	// Error is the failure's text.
	Error string `json:"error,omitempty"`
	// DelayMS is how long the loop waited before the next attempt.
	DelayMS int64 `json:"delay_ms"`
	// Model is the model the failed attempt named.
	Model string `json:"model,omitempty"`
	// Revised is set when Retry.Revise changed the request the next
	// attempt sends; the change itself is on the path as the config
	// delta the next settle writes.
	Revised bool `json:"revised,omitempty"`
}

// ModelBlockedNS is the namespace of the custom entry written for a
// request a BeforeModelCall guard refused with an error wrapping
// agentturn.ErrGuard, a policy stopping the run before the call. Its
// data is a [ModelBlocked]. A hook's other errors are a failed
// response entry, as a call that was refused for failing; a guard's
// stop is not a failure, and a failed response would make the run's
// end read as one.
const ModelBlockedNS = "agentturn:model_blocked"

// ModelBlocked is the data of a [ModelBlockedNS] custom entry.
type ModelBlocked struct {
	// Error is the guard's error.
	Error string `json:"error"`
	// RequestHash is the hash of the request that was refused, when the
	// path rebuilds its input, and Model the model it named.
	RequestHash string `json:"request_hash,omitempty"`
	Model       string `json:"model,omitempty"`
}

// ElicitationNS is the namespace of the custom entry written for a
// question a tool asked the user mid-call, through the elicitor
// [Recorder.Elicitor] wraps. Its data is an [Elicitation], and its
// call_id names the call that asked when the session holds it.
const ElicitationNS = "agentturn:elicitation"

// Elicitation is the data of an [ElicitationNS] custom entry: the
// question and what became of it.
type Elicitation struct {
	Message string          `json:"message,omitempty"`
	Schema  json.RawMessage `json:"schema,omitempty"`
	URL     string          `json:"url,omitempty"`
	// Action is accept, decline or cancel; empty when Error is set.
	Action  string          `json:"action,omitempty"`
	Content json.RawMessage `json:"content,omitempty"`
	// By is who answered, in the session format's terms; empty when
	// nobody was asked or the asking failed.
	By string `json:"by,omitempty"`
	// Error is the harness's failure to ask, which the tool sees as an
	// error rather than an answer.
	Error string `json:"error,omitempty"`
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
	parts    func(context.Context, openresponses.Request) ([]agentsession.InstructionPart, []agentsession.OmittedPart)
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
	// cwd is the working directory the session's header names, which a
	// child session inherits: a child runs in its parent's process and
	// its parent's directory, and a store buckets sessions by it.
	cwd string

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
	// ID of the response streaming it, learned from every item event
	// the stream raises, an item the attempt held and never committed
	// included, so a call that never reached its response entry is
	// still written naming the response it was cut out of.
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
	// items with foldSummary in the rebuilt context; foldPinned are the
	// items of the folded prefix that fold kept verbatim, which follow
	// the summary there as they do on the request.
	foldSet     bool
	foldSplit   int
	foldSummary openresponses.Item
	foldPinned  openresponses.Items
	// calls maps a call ID to what the path holds for it, for the calls
	// this writer wrote or Resume found pending.
	calls map[string]*callRecord
	// linked marks the calls whose subsession link is written.
	linked map[string]bool
	// parents maps a nested call, one a tool made through
	// agentturn.Invoke, to the call whose tool made it, so a record the
	// nested call writes names a call the path holds.
	parents map[string]string
	// detached is set for a child whose call returned while its run went
	// on, so the writer is released at that run's end rather than at the
	// call's.
	detached bool
	// replay is set while a transcript is being copied into a session
	// rather than written from a live run, so the outputs in it answer
	// calls that ran inside that run and no decision is written for
	// them.
	replay bool
	// env is the last env entry written, encoded without its
	// workspace, and envWorkspace that workspace, so the next is
	// written only when it differs.
	env          []byte
	envWorkspace *agentsession.Workspace
	// base is the request hash of the canonical base request of the
	// configuration the last run started under, from this writer's run
	// or the [ConfigBaseMember] of the last run start on a seeded path,
	// so a run whose configuration changed settles it before any of its
	// items.
	base string
	// attempt is the canonical request of the model call in flight,
	// encoded when it was reported and before a retry policy could edit
	// its maps in place, so a retry can say whether Revise changed it.
	attempt []byte
	// attemptModel is the model that request named.
	attemptModel string
	// retries is the number of attempts of the call in flight that
	// failed and were tried again, so its response entry says how many
	// calls it took.
	retries int
	// inbox lists, in the order they were accepted, the inputs that no
	// item has drained yet: the ones the agent holds, which a queued
	// event reported or Requeue handed back, and the ones a resumed or
	// forked path owes that nobody has taken up. The agent's own
	// outlive a rebase, since the agent still holds them.
	inbox []*inboxItem
	// omitted is the instructions_omitted of the last config entry that
	// carried one, encoded, so a change to what was left out is written
	// even when the settings did not move.
	omitted []byte

	// run is the ID of the run being written, "" between runs; open
	// lists, in order, the calls of that run with no output yet;
	// responses counts its model calls and lastCalls says whether the
	// last one requested tools, which is what the end reason turns on;
	// answeredCall says the run wrote an output or a decision, which is
	// how the format reads a run that answered a call an earlier one's
	// model call made.
	run          string
	open         []string
	responses    int
	lastCalls    bool
	answeredCall bool
}

// inboxItem is one input accepted into a queue and not appended yet.
type inboxItem struct {
	item    openresponses.Item
	mode    string
	trigger *agentsession.Trigger
	// entry is the ID of the queued entry holding the input on the
	// current path, "" when a run end or a rewind has closed the one
	// that did.
	entry string
	// held says the agent holds the input: its queued event was seen,
	// or Requeue handed it back. An input the path owes and nobody has
	// taken up is not held, and the next run end drops it, since the
	// format reads the end as closing it.
	held bool
	// awaiting is set while an input Requeue handed back waits for its
	// queued event, which then writes nothing: the path holds it.
	awaiting bool
}

// callRecord is what the path holds for one function call.
type callRecord struct {
	// entry is the ID of the item entry holding the call.
	entry string
	// args are the arguments as the model wrote them.
	args string
	// held is set while the latest decision is a hold that nothing has
	// answered; dispatched once a dispatch is written; rejected once a
	// reject is; answered once an output for it is on the path, which
	// is what the format reads as a call that is no longer pending.
	// dispatchRun is the run the last dispatch was written in, so a
	// call an earlier run dispatched and this one runs again gets a
	// dispatch of its own.
	held        bool
	dispatched  bool
	rejected    bool
	answered    bool
	dispatchRun string
	// settledRun is the run whose tool_end ended the call without a
	// dispatch: the loop refused it itself, for a name no tool has or
	// arguments that are not an object, or a cut ended it before it
	// was handed over. An output arriving in that run is then the
	// loop's own refusal rather than a caller's answer.
	settledRun string
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
// the workspace, with whatever tells one file system from another, a
// container's host or instance, inside it through
// agentsession.Workspace.SetMember rather than beside it, where the
// format's substitution rule does not look. The entry is written when
// it differs from the last one written, or found on the path by
// [Resume], its workspace compared by that rule, so a run in an
// unchanged environment adds nothing; a nil entry writes nothing. The
// recorder gathers nothing itself: what the host knows about its
// environment is the host's to supply, and the recorder stays free of
// the file system. An error fails the run. Only the recorder's own
// session gets env entries; a child session inherits its parent's
// environment through parent_session.
func WithEnv(fn func(context.Context) (*agentsession.EnvEntry, error)) Option {
	return func(r *Recorder) { r.env = fn }
}

// WithInstructionsParts names the parts a request's instructions are
// composed of, and the parts that were considered and left out, so the
// recorder's config entries carry instructions_parts and
// instructions_omitted rather than one string: a change to one layer,
// a memory block or a skill catalogue, is then a delta naming that
// part, each run of unchanged parts around it a keep, rather than the
// whole prompt again. fn is called with the canonical request whenever the
// recorder settles the settings, which is with the request about to be
// sent, after BeforeModelCall has rewritten it, so a product that edits
// its instructions in a hook takes its parts from what the hook left.
//
// fn is called for every session the recorder writes, a child run's
// included, with the run's context carrying the ID of the session being
// settled, so a host whose child agents compose prompts of their own
// routes by [SessionIDFromContext]; one that knows nothing of a
// session returns no parts, and that session keeps the string.
//
// Parts are used only when their texts, joined as the format joins
// them, equal the request's instructions; otherwise the entry carries
// the string as it does without this option, since the record must
// describe what the model was sent and must not fail the run over how
// a product composed it; so are parts the format refuses, one with no
// ID or two sharing one. The omitted parts are written on every config
// entry while they are non-empty, and on an entry of their own when
// they change and the settings do not. The format has no way to say
// that nothing is omitted any more, so a list that empties is not
// written and a reader keeps the last one. A session written before
// the option was set has the joined string on its path, and the first
// entry under the option carries the text of every part, once.
func WithInstructionsParts(fn func(ctx context.Context, req openresponses.Request) ([]agentsession.InstructionPart, []agentsession.OmittedPart)) Option {
	return func(r *Recorder) { r.parts = fn }
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
	return &writer{rec: r, id: id, calls: map[string]*callRecord{}, linked: map[string]bool{}, parents: map[string]string{}}
}

// Start creates a session from h and returns a recorder for it. When
// h.Records is nil the header promises the run, dispatch, decision and
// queued records, which this recorder writes whenever their event
// occurs. Child sessions name h.Harness as their writer unless
// [WithHarness] says otherwise. The caller keeps store for Sync,
// Release and Close.
//
// A header with a Base forks the session that holds it: the store
// writes the prefix, and the recorder is seeded from the context at
// the base, as [Resume] seeds one at the leaf, so an agent seeded with
// s.Context().Items records requests that carry hashes. A base inside
// a run leaves that run open on the fork, as a rewind does, and Start
// closes it, interrupted, with a ref naming the fork, before it
// returns; what that run had queued and not appended is closed with
// it, as a rewind leaves it. Inputs the prefix owes after a run's end
// are the fork's to take up with [Recorder.Requeue].
func Start(ctx context.Context, store agentsession.Store, h agentsession.Header, opts ...Option) (*Recorder, *agentsession.Session, error) {
	if h.Records == nil {
		h.Records = append(append([]string(nil), agentsession.AllRecords...), agentsession.TypeQueued)
	}
	s, err := store.Create(ctx, h)
	if err != nil {
		return nil, nil, fmt.Errorf("session: create: %w", err)
	}
	r := New(store, s.ID(), opts...)
	if r.harness == nil {
		r.harness = s.Header().Harness
	}
	r.root.cwd = s.Header().CWD
	if s.Header().Base != "" {
		if err := r.root.closeOpenRun(ctx, s, agentsession.ReasonInterrupted, "fork at "+s.Header().Base, false); err != nil {
			return nil, nil, err
		}
		if err := r.root.seed(s, true); err != nil {
			return nil, nil, err
		}
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
//
// A run with no end at the leaf was cut off: the process died inside
// it, since a recorder that is running one holds it. The recorder owns
// that run now, and closes it before it returns, as the format has it:
// a run end with reason error and a ref naming the cut, whose pending
// list is the segment's. The inputs queued on the path and not yet
// appended are queued again after it, so the run end does not close
// them; [Recorder.Requeue] hands them to the agent. Resume is for the
// one process continuing the session: a second one writing it while the
// first still runs would close a run that is not cut off.
func Resume(ctx context.Context, store agentsession.Store, sessionID string, opts ...Option) (*Recorder, *agentsession.Session, error) {
	s, err := store.Open(ctx, sessionID)
	if err != nil {
		return nil, nil, fmt.Errorf("session: open: %w", err)
	}
	return resume(ctx, s, store, opts)
}

// IdempotencyKeyMember is the member of a dispatch entry that carries
// the idempotency key the call was handed to its tool with. The format
// does not define it yet, and a reader that does not know it keeps it
// as written.
const IdempotencyKeyMember = "idempotency_key"

// DispatchKey returns the idempotency key a dispatch carries, or ""
// when it carries none.
func DispatchKey(d *agentsession.DispatchEntry) string {
	if d == nil {
		return ""
	}
	var key string
	if raw, ok := d.Unknown[IdempotencyKeyMember]; !ok || json.Unmarshal(raw, &key) != nil {
		return ""
	}
	return key
}

// Pending returns the calls pending at the session's leaf as the agent
// lists them, with the reason the record gives each: a call held by a
// hold decision is [agentturn.PendingDeferred]; one with a dispatch
// may have run and is [agentturn.PendingAborted], with the key its
// first dispatch carried; one with none, when the header promises
// dispatch records, never started and is
// [agentturn.PendingUndispatched]; and one the file cannot say about
// is [agentturn.PendingUnknown]. It is what [agentturn.WithPending]
// seeds an agent with beside the context's items, so the agent's
// Resume holds an approval to the replay rule.
func Pending(s *agentsession.Session) ([]agentturn.PendingCall, error) {
	if s.Leaf() == "" {
		return nil, nil
	}
	calls, err := s.PendingCalls(s.Leaf())
	if err != nil {
		return nil, fmt.Errorf("session: pending calls at leaf: %w", err)
	}
	var out []agentturn.PendingCall
	for _, c := range calls {
		p := agentturn.PendingCall{Call: c.Call, Reason: agentturn.PendingUnknown}
		switch c.State(s.Header()) {
		case agentsession.CallHeld:
			p.Reason = agentturn.PendingDeferred
		case agentsession.CallInFlight:
			p.Reason, p.IdempotencyKey = agentturn.PendingAborted, DispatchKey(c.Dispatch)
		case agentsession.CallNeverStarted:
			p.Reason = agentturn.PendingUndispatched
		}
		out = append(out, p)
	}
	return out, nil
}

// ReplayAnswers applies agenttool's rule for running a call again to
// the calls pending at the session's leaf, and returns an answer for
// each that is not held, by policy, in the order of the path. A call
// that never started is approved. A call that may have run, in flight
// when the record stopped or one the file cannot say about, is
// ambiguous: it is approved when its tool, looked up in tools by name,
// says replay is safe for its arguments, or keyed and its first
// dispatch carries the key, which the approval then carries; and it is
// answered with [agentturn.OutcomeUnknown] otherwise, including when
// no tool has its name. A held call is waiting for someone and is the
// caller's to answer; [Pending] lists it as deferred.
//
// The answers are what a host passes to Agent.Resume after [Resume],
// with its own for the held calls, once the agent is seeded with the
// context's items.
func ReplayAnswers(ctx context.Context, s *agentsession.Session, tools []agenttool.Tool) ([]agentturn.Answer, error) {
	pending, err := Pending(s)
	if err != nil {
		return nil, err
	}
	set := agenttool.Set(tools)
	var out []agentturn.Answer
	for _, p := range pending {
		id := p.Call.CallID
		var ans agentturn.Answer
		switch p.Reason {
		case agentturn.PendingDeferred:
			continue
		case agentturn.PendingUndispatched:
			ans = agentturn.Approve(id)
		default:
			ans = replayAnswer(ctx, set, p)
		}
		out = append(out, ans.WithBy(agentsession.ByPolicy))
	}
	return out, nil
}

// replayAnswer is the answer to one call that may have run.
func replayAnswer(ctx context.Context, tools agenttool.Set, p agentturn.PendingCall) agentturn.Answer {
	id := p.Call.CallID
	tool, ok := tools.Lookup(p.Call.Name)
	if !ok {
		return agentturn.OutcomeUnknown(id)
	}
	args := json.RawMessage(p.Call.Arguments)
	if len(args) == 0 {
		args = json.RawMessage("{}")
	}
	switch agenttool.ReplayOf(ctx, tool, args) {
	case agenttool.ReplaySafe:
		return agentturn.Approve(id).WithReason("run again: safe").WithIdempotencyKey(p.IdempotencyKey)
	case agenttool.ReplayKeyed:
		if p.IdempotencyKey != "" {
			return agentturn.Approve(id).WithReason("run again: keyed").WithIdempotencyKey(p.IdempotencyKey)
		}
	}
	return agentturn.OutcomeUnknown(id)
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
	return resume(ctx, next, store, opts)
}

func resume(ctx context.Context, s *agentsession.Session, store agentsession.Store, opts []Option) (*Recorder, *agentsession.Session, error) {
	r := New(store, s.ID(), opts...)
	if r.harness == nil {
		r.harness = s.Header().Harness
	}
	r.root.cwd = s.Header().CWD
	if err := r.root.closeOpenRun(ctx, s, agentsession.ReasonError, "cut off: closed on resume", true); err != nil {
		return nil, nil, err
	}
	if err := r.root.seed(s, true); err != nil {
		return nil, nil, err
	}
	return r, s, nil
}

// closeOpenRun ends the run open at the session's leaf, if there is
// one, with reason and ref, as the writer that continues the path. The
// end closes the queued entries of the inputs the run had not
// appended; keepOwed writes them again after it, for a cut, whose
// inputs were accepted and never answered, and not for a rewind or a
// fork, which leaves them behind with the rest of the branch.
func (w *writer) closeOpenRun(ctx context.Context, s *agentsession.Session, reason, ref string, keepOwed bool) error {
	if s.Leaf() == "" {
		return nil
	}
	run, err := s.OpenRun(s.Leaf())
	if err != nil {
		return fmt.Errorf("session: open run at leaf: %w", err)
	}
	if run == nil {
		return nil
	}
	owed, err := s.PendingQueued(s.Leaf())
	if err != nil {
		return fmt.Errorf("session: queued inputs at leaf: %w", err)
	}
	end, err := s.EndRun(reason, ref)
	if err != nil {
		return fmt.Errorf("session: close run %s: %w", run.RunID(), err)
	}
	if _, err := w.append(ctx, end); err != nil {
		return err
	}
	if !keepOwed {
		return nil
	}
	for _, q := range owed {
		again := agentsession.NewQueued(q.Item, q.Mode)
		if q.Trigger != nil {
			again.WithTrigger(q.Trigger.Kind, q.Trigger.Ref, q.Trigger.Source)
		}
		if _, err := w.append(ctx, again); err != nil {
			return err
		}
	}
	return nil
}

// Rebase moves the session's leaf to entryID and reseeds the recorder
// from the context there, as [Resume] seeds it from the leaf at open,
// so the next run records against the settings, items and pending
// calls of the branch it continues rather than the one it left. It
// refuses with [ErrRunActive] while a run is being written, and s must
// be the session the recorder writes. The agent's transcript is the
// caller's to set, with Agent.SetTranscript from s.Context().Items.
//
// A rebase appends nothing of its own, with two exceptions the format
// asks of the writer that continues a path. An entry inside a run, a
// checkpoint the run made, leaves that run open on the new branch, and
// Rebase closes it, interrupted, with a ref naming the rewind; what
// that run had queued and not appended is left behind with the rest
// of the branch. And an input the agent holds, queued and not
// appended, is written as a queued entry on the new branch unless the
// entry that holds it is still pending there.
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
	w := r.root
	ctx := context.Background()
	// Only what the agent holds carries over; an input the old path
	// owed and nobody took up stays with it.
	var held []*inboxItem
	for _, in := range w.inbox {
		if in.held {
			held = append(held, in)
		}
	}
	w.inbox = held
	if entryID == "" {
		s.ResetLeaf()
		w.reset()
		for _, in := range w.inbox {
			in.entry = ""
		}
		return w.requeue(ctx)
	}
	if err := s.Branch(entryID); err != nil {
		return fmt.Errorf("session: rebase: %w", err)
	}
	w.reset()
	if err := w.closeOpenRun(ctx, s, agentsession.ReasonInterrupted, "rewind to "+entryID, false); err != nil {
		return err
	}
	owed, err := s.PendingQueued(s.Leaf())
	if err != nil {
		return fmt.Errorf("session: queued inputs at leaf: %w", err)
	}
	pending := make(map[string]bool, len(owed))
	for _, q := range owed {
		pending[q.ID] = true
	}
	for _, in := range w.inbox {
		if !pending[in.entry] {
			in.entry = ""
		}
	}
	if err := w.requeue(ctx); err != nil {
		return err
	}
	return w.seed(s, false)
}

// endInbox closes the inbox at a run end: an input the agent holds is
// written again after it, since the end closed its entry and the agent
// still owes it to the conversation, and one it does not hold is
// dropped, as the format reads the end.
func (w *writer) endInbox(ctx context.Context) error {
	var held []*inboxItem
	for _, in := range w.inbox {
		if in.held {
			in.entry = ""
			held = append(held, in)
		}
	}
	w.inbox = held
	return w.requeue(ctx)
}

// requeue writes a queued entry for each input of the inbox that no
// entry on the current path holds.
func (w *writer) requeue(ctx context.Context) error {
	for _, in := range w.inbox {
		if in.entry != "" {
			continue
		}
		q := agentsession.NewQueued(in.item, in.mode)
		if in.trigger != nil {
			q.WithTrigger(in.trigger.Kind, in.trigger.Ref, in.trigger.Source)
		}
		id, err := w.append(ctx, q)
		if err != nil {
			return err
		}
		in.entry = id
	}
	return nil
}

// Queue queues items into a's queue in mode, as agentturn.Agent.Queue
// does with ctx's trigger, after writing each as a queued entry at the
// session's leaf, durably, before it returns: the way to accept an
// input that must not be lost while the agent is idle, whose queued
// event would otherwise wait for the next run. The event then writes
// nothing. An item whose entry cannot be written is not queued, and
// the error says so; the items before it are.
func (r *Recorder) Queue(ctx context.Context, a *agentturn.Agent, mode agentturn.QueueMode, items ...openresponses.Item) error {
	trigger := agentturn.TriggerFromContext(ctx)
	for _, item := range items {
		if item == nil {
			continue
		}
		base, _ := agentturn.Unhide(item)
		in := &inboxItem{item: base, mode: agentsession.ModeFollowUp, held: true, awaiting: true}
		if mode == agentturn.QueueSteer {
			in.mode = agentsession.ModeSteer
		}
		if !trigger.IsZero() {
			in.trigger = &agentsession.Trigger{Kind: trigger.Kind, Ref: trigger.Ref, Source: trigger.Source}
		}
		r.mu.Lock()
		r.root.inbox = append(r.root.inbox, in)
		err := r.root.requeue(context.WithoutCancel(ctx))
		if err != nil {
			r.root.inbox = r.root.inbox[:len(r.root.inbox)-1]
		}
		r.mu.Unlock()
		if err != nil {
			return err
		}
		a.Queue(ctx, mode, item)
	}
	return nil
}

// Requeue hands the agent the inputs the session owes, the ones queued
// on the path at [Resume], or at a [Start] on a base, and not yet
// appended, in the order they were accepted and each in its own mode
// and with its trigger, and returns how many. The path already holds
// each, so their queued events write nothing and the item that drains
// each names its entry. Call it after Resume and before the agent's
// first run: an input nobody has taken up when a run ends is closed by
// that end and dropped, which is how a host declines one. An input it
// has handed over is not handed over again, and an input marked hidden
// when it was accepted is queued without the mark, which the queued
// entry does not carry.
func (r *Recorder) Requeue(ctx context.Context, a *agentturn.Agent) int {
	r.mu.Lock()
	var owed []*inboxItem
	for _, in := range r.root.inbox {
		if !in.held && in.entry != "" {
			in.held, in.awaiting = true, true
			owed = append(owed, in)
		}
	}
	r.mu.Unlock()
	for _, in := range owed {
		qctx := ctx
		if in.trigger != nil {
			qctx = agentturn.ContextWithTrigger(ctx, agentturn.Trigger{Kind: in.trigger.Kind, Ref: in.trigger.Ref, Source: in.trigger.Source})
		}
		mode := agentturn.QueueFollowUp
		if in.mode == agentsession.ModeSteer {
			mode = agentturn.QueueSteer
		}
		a.Queue(qctx, mode, in.item)
	}
	return len(owed)
}

// Annotate appends a custom entry in namespace ns carrying data,
// encoded as JSON, at the current leaf of the session of the run on
// the context, or of the recorder's own session when the context
// names no run it is writing, and returns the entry's ID, which a
// checkpoint or a rewind can branch to. It never contributes an item
// or a setting, so the context and the request hashes are untouched.
// Made with the context of a tool call, from inside the tool, the
// entry's call_id names the call when the session holds it, so a
// record of one call of a parallel batch says whose it is. Made from a
// subscriber during turn_start, the entry lands before that turn's
// config entry, whatever the order the subscribers were registered in;
// made during any later event of the turn, after it.
func (r *Recorder) Annotate(ctx context.Context, ns string, data any) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w := r.writerOf(ctx)
	raw, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("session: encode annotation %s: %w", ns, err)
	}
	return w.append(context.WithoutCancel(ctx), &agentsession.CustomEntry{NS: ns, Data: raw, CallID: w.callOn(ctx)})
}

// EntryOf returns the ID of the entry the recorder wrote for item, the
// value the loop delivered on item_end, in the session of the run on
// the context, or the recorder's own when the context names none. It
// is false for an item the recorder did not write there, which
// includes an item whose item_end has not reached the recorder yet. A
// subscriber that marks a checkpoint asks this rather than reading the
// session's leaf, which moves with every entry: asked on the item's own
// item_end it needs to run after the recorder, and asked on any later
// event, turn_end or run_end, it finds the entry whatever the order.
func (r *Recorder) EntryOf(ctx context.Context, item openresponses.Item) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w := r.writerOf(ctx)
	for i := len(w.values) - 1; i >= 0; i-- {
		if w.values[i] == item {
			return w.items[i], true
		}
	}
	return "", false
}

// writerOf returns the writer of the run on the context, or the root.
func (r *Recorder) writerOf(ctx context.Context) *writer {
	if cw, ok := r.runs[agentturn.RunIDFromContext(ctx)]; ok {
		return cw
	}
	return r.root
}

// callOn returns the ID of the call on the context when the session
// holds its function call, the call whose tool made it for a nested
// call, and "" otherwise: a child run's context carries the parent's
// call, which is not in the child's session.
func (w *writer) callOn(ctx context.Context) string {
	call, ok := agenttool.CallFrom(ctx)
	if !ok {
		return ""
	}
	return w.heldCall(call.ID)
}

// RecordFunc returns the recorder a tool's agenttool.WriteRecord
// reaches: each record is written as a custom entry in the record's
// namespace, at the leaf of the run on the context, with call_id
// naming the call that wrote it, durably before it returns, as
// [Recorder.Annotate] writes one. Set it as
// agentturn.Config.ToolRecorder, or install it with
// agenttool.ContextWithRecorder on the context a run is prompted with.
func (r *Recorder) RecordFunc() agenttool.RecordFunc {
	return func(ctx context.Context, rec *agenttool.Record) error {
		_, err := r.Annotate(ctx, rec.NS, json.RawMessage(rec.Data))
		return err
	}
}

// Elicitor returns an elicitor that puts each question to fn and
// writes the question and its answer as an [ElicitationNS] custom
// entry, under the call that asked, before the answer returns to the
// tool; by names who answers, in the session format's terms. Set it as
// agentturn.Config.ToolElicitor, or install it with
// agenttool.ContextWithElicitor on the context a run is prompted with.
// A nil fn answers every question with agenttool.ActionCancel, since
// nobody was asked, and records that. A failure to write the entry is
// returned to the tool as the harness failing to ask.
//
// An agent run from inside a tool served by agenttool's mcpserver
// finds the elicitor that asks the MCP client on the call's context,
// not on its configuration; to record those questions, wrap that one
// and put it on the context the run is prompted with:
//
//	served, _ := agenttool.ElicitorFrom(ctx)
//	ctx = agenttool.ContextWithElicitor(ctx, rec.Elicitor("user", served))
//
// The served elicitor belongs to one call, so a configuration built
// once cannot name it as ToolElicitor. When the client offers no
// elicitation served is nil, and each question is recorded as
// cancelled.
func (r *Recorder) Elicitor(by string, fn agenttool.Elicitor) agenttool.Elicitor {
	return func(ctx context.Context, q agenttool.Elicitation) (agenttool.Answer, error) {
		ans := agenttool.Answer{Action: agenttool.ActionCancel}
		var err error
		who := ""
		if fn != nil {
			ans, err = fn(ctx, q)
			who = by
		}
		data := Elicitation{Message: q.Message, Schema: q.Schema, URL: q.URL}
		if err != nil {
			// Nobody answered: the harness failed to ask.
			data.Error = err.Error()
		} else {
			data.Action, data.Content, data.By = string(ans.Action), ans.Content, who
		}
		if _, werr := r.Annotate(ctx, ElicitationNS, data); werr != nil {
			return agenttool.Answer{}, errors.Join(err, werr)
		}
		return ans, err
	}
}

// reset clears what seed sets, keeping the writer's identity and its
// configuration.
func (w *writer) reset() {
	w.settings = agentsession.Settings{}
	w.wroteConfig = false
	w.settleReq = nil
	w.inFlight, w.pending, w.started, w.inFlightID = false, "", time.Time{}, ""
	w.items, w.values, w.custom = nil, nil, nil
	w.foldSet, w.foldSplit, w.foldSummary, w.foldPinned = false, 0, nil, nil
	w.calls = map[string]*callRecord{}
	w.env, w.envWorkspace = nil, nil
	w.omitted = nil
	w.base = ""
	w.attempt, w.attemptModel, w.retries = nil, "", 0
	w.parents = map[string]string{}
}

// seed sets the writer's state from the session at its leaf.
func (w *writer) seed(s *agentsession.Session, owed bool) error {
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
	w.omitted = omittedBody(cx.InstructionsOmitted())
	w.base = lastConfigBase(s.Path(s.Leaf()))
	for _, e := range cx.Entries {
		if env, ok := e.(*agentsession.EnvEntry); ok {
			w.env, w.envWorkspace = envBody(env), env.Workspace
		}
	}
	if owed {
		// The inputs the path still owes, for a recorder new to it:
		// Requeue hands them to the agent.
		queued, err := s.PendingQueued(s.Leaf())
		if err != nil {
			return fmt.Errorf("session: queued inputs at leaf: %w", err)
		}
		for _, q := range queued {
			w.inbox = append(w.inbox, &inboxItem{item: q.Item, mode: q.Mode, trigger: q.Trigger, entry: q.ID})
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

type sessionIDKey struct{}

// ContextWithSessionID attaches the ID of the session a run is being
// written to. A host puts its own recorder's on the context it prompts
// with; [Recorder.ChildContext] puts a child's on the context the child
// run is given.
func ContextWithSessionID(ctx context.Context, sessionID string) context.Context {
	return context.WithValue(ctx, sessionIDKey{}, sessionID)
}

// SessionIDFromContext returns the ID of the session the run on ctx is
// written to, or "" when nothing put one there. It is what a layer that
// attributes its writes to a session reads, beside
// agentturn.RunIDFromContext: inside a child run it names the child's
// session, which the recorder creates and the host never otherwise
// sees.
func SessionIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(sessionIDKey{}).(string)
	return id
}

// ChildContext returns ctx carrying the ID of the session the recorder
// will write a child run started under callID to, for
// agent.WithRunContext:
//
//	specialist := agent.New(childCfg,
//		agent.WithObserver(rec.Observe),
//		agent.WithRunContext(rec.ChildContext))
//
// The ID is derived from the session of the run on the context, the
// parent's, and the call, as the child's own header is, so the child's
// tools and hooks can attribute what they write to the session that
// holds the run making the write. A recorder that has to mint an ID
// instead, because the store could neither create nor reopen the
// derived one, writes that child elsewhere; the context then names a
// session the run did not go to, which is the one case this is wrong
// and the one where nothing was recorded properly anyway.
func (r *Recorder) ChildContext(ctx context.Context, callID string) context.Context {
	if callID == "" {
		return ctx
	}
	r.mu.Lock()
	parent := r.root
	if p, ok := r.runs[agentturn.RunIDFromContext(ctx)]; ok {
		parent = p
	}
	id := parent.id
	r.mu.Unlock()
	return ContextWithSessionID(ctx, agentsession.SubsessionID(id, callID))
}

// SessionID returns the ID of the session being written.
func (r *Recorder) SessionID() string { return r.root.id }

// Store returns the store the recorder writes to.
func (r *Recorder) Store() agentsession.Store { return r.store }

// Attach subscribes the recorder to a and returns the unsubscribe
// function. The recorder takes the agent's configuration at every
// run_start from then on, so a change through Agent.SetConfig reaches
// the record as a config delta and as the filter in force.
//
// Subscribers are called in registration order and the recorder writes
// a call's dispatch when tool_dispatch reaches it, durably, before the
// tool runs. A subscriber that may refuse a dispatch, a policy that
// vetoes a call at hand-off, is therefore subscribed before the
// recorder: one subscribed after it refuses a call whose dispatch is
// already on the record, and the record reads that call as possibly
// run although the loop never handed it over.
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
		return
	}
	if _, end := ev.(*agentturn.RunEnd); end && w.detached {
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

// newChild creates a session under parent and returns its writer. The
// header names the parent's session, the call and the parent's working
// directory, which is the child's too: it runs in the same process and
// the same directory, and a store that buckets sessions by directory
// would otherwise file every child under none. With a call ID the
// session's ID is derived from the parent's and the call's, so a
// reader can compute it from the parent's link alone.
//
// A second run under the same call continues the existing session from
// its leaf: a subagent that is messaged again answers from its own
// context, so its run belongs after the one before it, with the
// hashes that follow from that. A host that means a retry from a clean
// start says so with agent.ContextWithRetry, and the leaf is reset as
// for a fresh child.
//
// live says the child is written from its events, so the header
// promises the record entries; a child replayed from its items
// promises nothing.
func (r *Recorder) newChild(ctx context.Context, parent *writer, callID string, live bool) (*writer, error) {
	h := agentsession.Header{ParentSession: parent.id, Harness: r.harness, SpawnedBy: callID, CWD: parent.cwd}
	if live {
		h.Records = append([]string(nil), agentsession.AllRecords...)
	}
	if callID != "" {
		h.ID = agentsession.SubsessionID(parent.id, callID)
	}
	s, err := r.store.Create(ctx, h)
	if errors.Is(err, agentsession.ErrSessionExists) {
		if s, err = r.store.Open(ctx, h.ID); err == nil {
			w := newWriter(r, s.ID())
			w.cwd = parent.cwd
			if agent.RetryFromContext(ctx) {
				s.ResetLeaf()
				return w, nil
			}
			if err := w.seed(s, false); err != nil {
				return nil, err
			}
			return w, nil
		}
		// The store cannot reopen it; a fresh session still records
		// the run, under a minted ID.
		h.ID = ""
		s, err = r.store.Create(ctx, h)
	}
	if err != nil {
		return nil, fmt.Errorf("session: create child session: %w", err)
	}
	w := newWriter(r, s.ID())
	w.cwd = parent.cwd
	return w, nil
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
	case *agentturn.ToolDispatch:
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
	switch ev.(type) {
	case *agentturn.RunStart, *agentturn.ModelBlocked, *agentturn.ItemEnd, *agentturn.ResponseEnd,
		*agentturn.ToolStart, *agentturn.ToolDispatch, *agentturn.ToolEnd, *agentturn.RunEnd:
		// The events that write an entry flush the settle held for the
		// call in flight, so the settings are on the path before what
		// they describe. The others, item_start and item_update among
		// them, write nothing and leave it held: an attempt that
		// streamed and then failed is replaced by the settle of the
		// attempt that follows it, so a turn that answered once
		// records one config.
		if err := w.flush(ctx); err != nil {
			return err
		}
	}
	if w.inFlight {
		// The stream names its response on every item event, an item
		// the attempt held and never committed included, so a call cut
		// off before its response entry is written naming the response
		// it was cut out of: the interrupted turn of a reasoning
		// model, which opens its reasoning item before anything else.
		if id := streamedResponseID(ev); id != "" {
			w.inFlightID = id
		}
	}
	switch e := ev.(type) {
	case *agentturn.RunStart:
		return w.runStart(ctx, e)
	case *agentturn.TurnStart:
		return w.turnStart(ctx, e)
	case *agentturn.ModelRetry:
		return w.retry(ctx, e)
	case *agentturn.ModelBlocked:
		return w.blocked(ctx, e)
	case *agentturn.ItemEnd:
		return w.item(ctx, e.Item, e.ResponseID, e.Hidden)
	case *agentturn.ResponseEnd:
		return w.response(ctx, e)
	case *agentturn.ToolStart:
		return w.toolStart(ctx, e)
	case *agentturn.ToolDispatch:
		return w.toolDispatch(ctx, e)
	case *agentturn.RunEnd:
		return w.runEnd(ctx, e)
	case *agentturn.ToolEnd:
		return w.toolEnd(ctx, e)
	case *agentturn.Queued:
		return w.queued(ctx, e)
	}
	return nil
}

// queued writes the queued entry for an input the agent accepted,
// before anything appends it, unless the path already holds it: an
// input Requeue handed back.
func (w *writer) queued(ctx context.Context, e *agentturn.Queued) error {
	if e.Item == nil {
		return nil
	}
	for _, in := range w.inbox {
		if in.awaiting && in.item == e.Item {
			in.awaiting = false
			return nil
		}
	}
	mode := agentsession.ModeFollowUp
	if e.Mode == agentturn.QueueSteer {
		mode = agentsession.ModeSteer
	}
	in := &inboxItem{item: e.Item, mode: mode, held: true}
	if !e.Trigger.IsZero() {
		in.trigger = &agentsession.Trigger{Kind: e.Trigger.Kind, Ref: e.Trigger.Ref, Source: e.Trigger.Source}
	}
	w.inbox = append(w.inbox, in)
	return w.requeue(ctx)
}

// drained removes item from the inbox and returns the input it was,
// the first accepted when it was queued more than once, or nil when
// the agent did not accept it through a queue.
func (w *writer) drained(item openresponses.Item) *inboxItem {
	for i, in := range w.inbox {
		if in.held && in.item == item {
			w.inbox = append(w.inbox[:i:i], w.inbox[i+1:]...)
			return in
		}
	}
	return nil
}

// toolEnd writes what a call's result carries for the record: a child
// run's link, and the tool's own side data when its Details value is
// an agenttool.Recordable, as a custom entry in the namespace the value
// names, between the call's dispatch and its output.
func (w *writer) toolEnd(ctx context.Context, e *agentturn.ToolEnd) error {
	if c := w.calls[e.CallID]; c != nil && e.Parent == "" && !e.Deferred && !c.dispatched && !c.rejected {
		c.settledRun = w.run
	}
	if e.Parent != "" {
		if err := w.nested(ctx, NestedCall{
			Phase:  agentsession.RunEnd,
			CallID: e.CallID,
			Parent: e.Parent,
			Name:   e.Name,
			Output: e.Result.Output.Text,
			Error:  errText(e.Err),
		}, e.Parent); err != nil {
			return err
		}
	}
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
	// A nested call has no function call on the path; its record is
	// the work of the call that made it.
	owner := e.CallID
	if e.Parent != "" {
		owner = e.Parent
	}
	_, err = w.append(ctx, &agentsession.CustomEntry{NS: rec.NS, Data: rec.Data, CallID: w.heldCall(owner)})
	return err
}

// heldCall returns callID when the session holds its function call,
// the nearest call up its chain that the session holds for a nested
// call, and "" otherwise.
func (w *writer) heldCall(callID string) string {
	for range len(w.parents) + 1 {
		if _, ok := w.calls[callID]; ok {
			return callID
		}
		parent, ok := w.parents[callID]
		if !ok {
			return ""
		}
		callID = parent
	}
	return ""
}

// nested writes one custom entry for a call a tool made through
// agentturn.Invoke, with call_id naming the call whose tool made it.
func (w *writer) nested(ctx context.Context, n NestedCall, parent string) error {
	raw, err := json.Marshal(n)
	if err != nil {
		return fmt.Errorf("session: encode nested call %s: %w", n.CallID, err)
	}
	_, err = w.append(ctx, &agentsession.CustomEntry{NS: NestedCallNS, Data: raw, CallID: w.heldCall(parent)})
	return err
}

// decisionReason is a decision's reason, or "" when there is none.
func decisionReason(d *agentturn.ToolDecision) string {
	if d == nil {
		return ""
	}
	return d.Reason
}

// decisionBy is who a decision names, or "" when there is none.
func decisionBy(d *agentturn.ToolDecision) string {
	if d == nil {
		return ""
	}
	return d.By
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
	w.answeredCall = false
	start := agentsession.NewRunStart(e.RunID, string(e.Source), e.Trigger.String())
	if !e.Trigger.IsZero() {
		start.Trigger = &agentsession.Trigger{Kind: e.Trigger.Kind, Ref: e.Trigger.Ref, Source: e.Trigger.Source}
	}
	// The comparison below does not ask a tool provider, which may cost
	// a round trip or list its tools in another order each time; what
	// it offers reaches the path at turn_start.
	var probeReq openresponses.Request
	var base string
	if w.cfg != nil {
		probe := *w.cfg
		if probe.ToolProvider != nil {
			probe.ToolProvider, probe.Tools = nil, nil
		}
		probeReq = Canonical(probe.BaseRequest(ctx))
		var err error
		if base, err = RequestHash(probeReq); err != nil {
			return fmt.Errorf("session: base request: %w", err)
		}
		raw, err := json.Marshal(base)
		if err != nil {
			return fmt.Errorf("session: encode base request hash: %w", err)
		}
		start.Unknown = map[string]json.RawMessage{ConfigBaseMember: raw}
	}
	if _, err := w.append(ctx, start); err != nil {
		return err
	}
	if w == w.rec.root && w.rec.env != nil {
		if err := w.writeEnv(ctx); err != nil {
			return err
		}
	}
	if w.cfg == nil {
		return nil
	}
	// The configuration is settled before any item of the run when
	// nothing has been written, and when it changed since the last run
	// this writer saw, so the items a new configuration's BeforeTurn
	// appends are filed under it. A configuration that did not change
	// is left to turn_start, which settles the request as sent: a hook
	// that edits the request every turn would otherwise be undone here
	// and redone there on every run. A writer seeded from a path, by
	// Resume, Start on a based header, Rebase, Continue or a child
	// session reopened under the same call, compares with the base the
	// path's last run start recorded, as it would with its own. A path
	// that recorded none, written before the member was or by a
	// recorder that did not know its configuration, is compared by its
	// settings at the leaf instead.
	prev := w.base
	w.base = base
	switch {
	case !w.wroteConfig:
	case prev != "" && prev != base:
	case prev == "" && w.differs(ctx, probeReq, w.cfg.ToolProvider == nil):
	default:
		return nil
	}
	return w.settle(ctx, Canonical(w.cfg.BaseRequest(ctx)))
}

// lastConfigBase returns the [ConfigBaseMember] of the last run start
// on path, or "" when that start carries none.
func lastConfigBase(path []agentsession.Entry) string {
	for i := len(path) - 1; i >= 0; i-- {
		run, ok := path[i].(*agentsession.RunEntry)
		if !ok || !run.IsStart() {
			continue
		}
		var base string
		if raw, ok := run.Unknown[ConfigBaseMember]; ok {
			_ = json.Unmarshal(raw, &base)
		}
		return base
	}
	return ""
}

// differs reports whether req's settings differ from those in force
// on the path, the instructions compared as the string and the tools
// only when tools is set. Instructions composed of parts on the path
// are not compared with a base the host's parts do not compose: those
// parts are rendered into the request by a hook, so the base is not
// what they stand for, and settling it would replace them with a
// string that was never sent. A request the format cannot hold is
// left to turn_start, which settles the one sent.
func (w *writer) differs(ctx context.Context, req openresponses.Request, tools bool) bool {
	full, err := agentsession.ConfigFromRequest(req)
	if err != nil {
		return false
	}
	next, have := agentsession.Settings{}.Apply(full), w.settings
	have.InstructionsParts = nil
	if len(w.settings.InstructionsParts) > 0 {
		if parts, _ := w.instructionParts(ctx, req); len(parts) == 0 {
			next.Instructions, have.Instructions = "", ""
		}
	}
	if !tools {
		next.Tools, have.Tools = nil, nil
	}
	return !equalJSON(have, next)
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
	if w.env != nil && bytes.Equal(data, w.env) && agentsession.SameWorkspace(env.Workspace, w.envWorkspace) {
		return nil
	}
	if _, err := w.append(ctx, env); err != nil {
		return err
	}
	w.env, w.envWorkspace = data, env.Workspace
	return nil
}

// envBody encodes an env entry without its envelope or its workspace,
// so one read from the path compares equal to a fresh one with the
// same content; the workspace is compared by the format's rule, which
// reads its members in canonical form, so one written with its keys in
// another order is the same workspace. The members the library does
// not define are kept: a container restart a host names in one of them
// is a change of environment.
func envBody(env *agentsession.EnvEntry) []byte {
	body := *env
	body.EntryBase = agentsession.EntryBase{Unknown: env.Unknown}
	body.Workspace = nil
	data, err := agentsession.MarshalEntry(&body)
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
	if w.attempt, err = json.Marshal(req); err != nil {
		return fmt.Errorf("session: encode request: %w", err)
	}
	w.attemptModel, w.retries = req.Model, 0
	w.inFlight = true
	w.pending = hash
	w.started = w.rec.now()
	w.inFlightID = ""
	return nil
}

// attempts is the attempts member of the response entry of the call in
// flight: the calls it took, itself included, or zero for one, which
// the format reads as one. The loop reports a retry before its backoff
// and nothing more until the next attempt streams, so an abort during
// the backoff counts the attempt that was due.
func (w *writer) attempts() int {
	if w.retries == 0 {
		return 0
	}
	return w.retries + 1
}

// retry takes the settings and the hash of the request the next
// attempt will send, in place of the ones the attempt that failed was
// built with: a fallback that moved the turn to another model is a
// config delta on the path, so the settings in force name the model
// that answered.
func (w *writer) retry(ctx context.Context, e *agentturn.ModelRetry) error {
	req := Canonical(e.Request)
	hash, err := w.hash(req)
	if err != nil {
		return err
	}
	next, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("session: encode request: %w", err)
	}
	rec := ModelRetry{Attempt: e.Attempt, Error: errText(e.Err), DelayMS: e.Delay.Milliseconds(), Model: w.attemptModel}
	if w.attempt != nil {
		rec.Revised = !bytes.Equal(w.attempt, next)
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("session: encode model retry: %w", err)
	}
	// The settle held for the failed attempt stays held: the attempt
	// that follows replaces it, and only the one that answers is
	// configured on the path.
	if _, err := w.append(ctx, &agentsession.CustomEntry{NS: ModelRetryNS, Data: raw}); err != nil {
		return err
	}
	w.attempt, w.attemptModel, w.retries = next, req.Model, e.Attempt
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
// items that fold pinned, then the items written as item entries from
// the fold's first kept one on.
func (w *writer) expectedInput() (openresponses.Items, bool) {
	from := 0
	var out openresponses.Items
	if w.foldSet {
		if w.foldSplit > len(w.values) {
			return nil, false
		}
		from = w.foldSplit
		out = append(out, w.foldSummary)
		out = append(out, w.foldPinned...)
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
	if errors.Is(e.Err, agentturn.ErrGuard) {
		// A policy stopped the run: the refused request is a record of
		// its own, since a failed response would read as the run
		// failing.
		raw, err := json.Marshal(ModelBlocked{Error: errText(e.Err), RequestHash: hash, Model: req.Model})
		if err != nil {
			return fmt.Errorf("session: encode model blocked: %w", err)
		}
		_, err = w.append(ctx, &agentsession.CustomEntry{NS: ModelBlockedNS, Data: raw})
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
// config first and deltas after. With [WithInstructionsParts] the
// entries carry the parts of the instructions when they join to the
// request's, and what was left out.
func (w *writer) settle(ctx context.Context, req openresponses.Request) error {
	parts, omitted := w.instructionParts(ctx, req)
	full, err := agentsession.ConfigFromRequestParts(req, parts...)
	if err != nil {
		return fmt.Errorf("session: %w", err)
	}
	omittedChanged := !bytes.Equal(omittedBody(omitted), w.omitted)
	var entry *agentsession.ConfigEntry
	switch {
	case !w.wroteConfig:
		entry = full
	default:
		entry = configDelta(w.settings, agentsession.Settings{}.Apply(full), full, parts)
		if entry == nil && omittedChanged && len(omitted) > 0 {
			// Nothing in force moved, but what was left out did: the
			// entry says so and changes no setting.
			entry = &agentsession.ConfigEntry{}
		}
	}
	if entry != nil {
		if len(omitted) > 0 {
			entry.InstructionsOmitted = omitted
		}
		if _, err := w.append(ctx, entry); err != nil {
			return err
		}
		w.settings = w.settings.Apply(entry)
		if len(omitted) > 0 {
			w.omitted = omittedBody(omitted)
		}
	}
	w.wroteConfig = true
	return nil
}

// instructionParts asks the host for the parts of req's instructions
// and what it left out, and returns them when the format can hold them
// and the parts join to the instructions the request carries. Parts
// that do not join, or that the format refuses, a part with no ID or
// two with one, are dropped, and the omitted parts with them, since
// they describe a composition that is not the one sent or cannot be
// written; an omitted part with no ID is dropped alone. The record
// falls back to the string rather than fail the run. The host is
// asked with ctx naming the writer's session, whatever the run's
// context named, so a child's parts are routed to the child.
func (w *writer) instructionParts(ctx context.Context, req openresponses.Request) ([]agentsession.InstructionPart, []agentsession.OmittedPart) {
	if w.rec.parts == nil {
		return nil, nil
	}
	parts, omitted := w.rec.parts(ContextWithSessionID(ctx, w.id), req)
	if len(parts) == 0 || agentsession.JoinInstructions(parts) != req.Instructions {
		return nil, nil
	}
	seen := make(map[string]bool, len(parts))
	for _, p := range parts {
		if p.ID == "" || seen[p.ID] {
			return nil, nil
		}
		seen[p.ID] = true
	}
	var named []agentsession.OmittedPart
	for _, o := range omitted {
		if o.ID != "" {
			named = append(named, o)
		}
	}
	return parts, named
}

// omittedBody encodes omitted parts for comparison, nil for none.
func omittedBody(omitted []agentsession.OmittedPart) []byte {
	if len(omitted) == 0 {
		return nil
	}
	data, err := json.Marshal(omitted)
	if err != nil {
		return nil
	}
	return data
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
	if out, ok := item.(*openresponses.FunctionCallOutput); ok && !w.replay {
		// A call an earlier run dispatched, answered now with an output
		// of the caller's, gets no decision: a proceed says the call
		// went on toward its tool, which it did not, and a reject that
		// it never reached its tool, which may be false. The output is the record,
		// and no second dispatch before it says the tool did not run
		// again.
		if c := w.calls[out.CallID]; c != nil && !c.dispatched && !c.rejected {
			// The caller wrote the output themselves: the call never
			// reached its tool, and the output is what the model sees.
			// A held call is the common case, but a call seeded from a
			// path that holds its item and no decision, the shape a
			// branch leaves when the dispatch is on the branch that was
			// left, is the same thing to a reader and gets the same
			// decision, which is what the format's record check asks
			// for.
			//
			// A call the loop settled itself in this run, for a name
			// no tool has or arguments that are not an object, has the
			// same shape with the loop as the decider: it refused the
			// call before any tool, which is a policy's refusal.
			by := agentturn.DeciderFromContext(ctx, out.CallID)
			if c.settledRun != "" && c.settledRun == w.run {
				// The loop refused it, whoever approved it: a call
				// the caller approved on resume and the loop then
				// found no tool for is the loop's reject.
				by = agentsession.ByPolicy
			}
			dec := agentsession.NewDecision(out.CallID, c.entry, agentsession.VerdictReject, by).WithReason(outputText(out))
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
	if responseID == "" {
		// An input the agent accepted through a queue names the queued
		// entry it was accepted as, with what brought it in; one the
		// filter keeps from the model is a custom entry, which cannot,
		// and the run's end closes its queued entry.
		if in := w.drained(item); in != nil && !appOnly && in.entry != "" {
			e := entry.(*agentsession.ItemEntry)
			e.QueuedFrom = in.entry
			if in.trigger != nil {
				trigger := *in.trigger
				e.Source = &trigger
			}
		}
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

// streamedResponseID is the response an item event names, and "" for
// an event that names none.
func streamedResponseID(ev agentturn.Event) string {
	switch e := ev.(type) {
	case *agentturn.ItemStart:
		return e.ResponseID
	case *agentturn.ItemUpdate:
		return e.ResponseID
	case *agentturn.ItemEnd:
		return e.ResponseID
	}
	return ""
}

// close marks a call answered and removes it from the run's open list.
func (w *writer) close(callID string) {
	if c, ok := w.calls[callID]; ok {
		c.answered = true
	}
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
// was. The dispatch is written when the call is handed to its tool,
// which tool_dispatch reports.
func (w *writer) toolStart(ctx context.Context, e *agentturn.ToolStart) error {
	if e.Parent != "" {
		w.parents[e.CallID] = e.Parent
		return w.nested(ctx, NestedCall{
			Phase:  agentsession.RunStart,
			CallID: e.CallID,
			Parent: e.Parent,
			Name:   e.Name,
			Args:   json.RawMessage(e.Args),
			Verdict: func() string {
				switch {
				case e.Decision == nil:
					return ""
				case e.Decision.Action != agentturn.Allow:
					return agentsession.VerdictReject
				case e.Decision.Args != nil:
					return agentsession.VerdictProceed
				}
				return ""
			}(),
			Reason: decisionReason(e.Decision),
			By:     decisionBy(e.Decision),
		}, e.Parent)
	}
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
	// An allowed call is recorded when something was decided about it:
	// it was held and is now approved, its arguments were rewritten, or
	// the hook gave a reason, such as the grant that allowed it.
	reason := decisionReason(d)
	if rewritten := !sameJSON(e.Args, c.args); c.held || rewritten || reason != "" {
		dec := agentsession.NewDecision(e.CallID, c.entry, agentsession.VerdictProceed, by)
		if rewritten {
			dec.WithArgs(e.Args)
		}
		if reason != "" {
			dec.WithReason(reason)
		}
		if _, err := w.append(ctx, dec); err != nil {
			return err
		}
	}
	c.held = false
	return nil
}

// toolDispatch writes the call's dispatch: the loop has handed it to
// its tool, so from here its side effect may have happened. It is
// durable before this returns, and a failure here stops the call, so
// no tool runs after a dispatch the record does not hold. A call an
// earlier run dispatched and this one runs again gets a second, so
// each time the tool may have run is on the path. A nested call has
// no function_call item to anchor one to and its record is the custom
// entries tool_start and tool_end write.
func (w *writer) toolDispatch(ctx context.Context, e *agentturn.ToolDispatch) error {
	if e.Parent != "" {
		return nil
	}
	c := w.calls[e.CallID]
	if c == nil || c.rejected || (c.dispatched && c.dispatchRun == w.run) {
		return nil
	}
	d := agentsession.NewDispatch(e.CallID, c.entry)
	if e.IdempotencyKey != "" {
		key, err := json.Marshal(e.IdempotencyKey)
		if err != nil {
			return fmt.Errorf("session: encode idempotency key of %s: %w", e.CallID, err)
		}
		d.Unknown = map[string]json.RawMessage{IdempotencyKeyMember: key}
	}
	if _, err := w.append(ctx, d); err != nil {
		return err
	}
	c.dispatched, c.dispatchRun = true, w.run
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
		Attempts:    w.attempts(),
	}
	w.inFlight, w.pending, w.started, w.inFlightID, w.retries = false, "", time.Time{}, "", 0
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
	latency, attempts := w.latency(), w.attempts()
	w.inFlight, w.pending, w.started, w.inFlightID, w.retries = false, "", time.Time{}, "", 0
	if inFlight {
		w.responses++
		w.lastCalls = false
		entry := &agentsession.ResponseEntry{
			ResponseID:  responseID,
			Status:      openresponses.ResponseStatusFailed,
			Error:       errorPayload(e.Err),
			RequestHash: hash,
			LatencyMS:   latency,
			Attempts:    attempts,
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
	if _, err := w.append(ctx, entry); err != nil {
		return err
	}
	// The end closes the queued entries of the inputs the run did not
	// append; the ones the agent still holds are written again.
	return w.endInbox(ctx)
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
		// reader asks, whichever shape the segment has. A guard's error
		// says which guard and why, so it follows the cause, as an
		// error's text is the ref of a failure.
		cause := string(e.Cause)
		if e.Cause == agentturn.StopGuard && e.Err != nil {
			cause += ": " + e.Err.Error()
		}
		switch {
		case w.responses == 0:
			// A resume whose approved batch terminated, a refusal on
			// Resume, or a guard's stop before the run's first model
			// call: the segment has no response of its own. The format
			// reads it as stopped when it answered a call an earlier
			// run's model call made and left nothing on the path
			// pending, and as aborted otherwise.
			//
			// A resume reaches only the stopped side: Resume refuses a
			// partial answer, so a run of its that stops without calling
			// the model has answered every call that was pending. A
			// guard at BeforeTurn or BeforeModelCall that stops a run
			// before its first model call, having answered nothing,
			// reaches the aborted side, and so does a host driving
			// agentturn.Run itself through Handle, which ends a run where
			// it likes. The format's stopped step asks one further thing
			// that is not checked here, that the path's last response is
			// the one whose calls are being answered; no arrangement of
			// the loop's events can break that, since a call without an
			// output keeps the model from being called again.
			if w.answeredCall && !w.pendingOnPath() {
				return agentsession.ReasonStopped, cause
			}
			return agentsession.ReasonAborted, cause
		case w.lastCalls:
			return agentsession.ReasonStopped, cause
		}
		// A guard or a turn budget stopped a run whose last response
		// requested nothing, which the format reads as done.
		return agentsession.ReasonDone, cause
	}
	return string(e.Reason), ""
}

// pendingOnPath reports whether any call the writer knows of is still
// without an output. The format's stopped step reads every call on the
// path, not only the run's own, so a call an earlier run left
// unanswered keeps this one from reading as stopped.
func (w *writer) pendingOnPath() bool {
	for _, c := range w.calls {
		if !c.answered {
			return true
		}
	}
	return false
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
	split, placed, err := w.foldSplitOf(f)
	if err != nil {
		return err
	}
	if !placed {
		// The fold kept an item the path holds nowhere, or in more than
		// one place: a compaction naming a guess would drop or repeat
		// items on a resume. The record says a fold happened and the
		// requests after it, which the path cannot rebuild, carry no
		// hash; the run goes on.
		raw, err := json.Marshal(UnplacedFold{Reason: "the fold's first kept item is not one entry of the path", TokensBefore: f.TokensBefore, ResponseID: f.ResponseID})
		if err != nil {
			return fmt.Errorf("session: encode unplaced fold: %w", err)
		}
		_, err = w.append(ctx, &agentsession.CustomEntry{NS: UnplacedFoldNS, Data: raw})
		return err
	}
	w.foldSet, w.foldSplit, w.foldSummary, w.foldPinned = true, split, f.Summary, f.Pinned
	entry := &agentsession.CompactionEntry{
		FirstKept:    w.items[split],
		Summary:      f.Summary,
		Pinned:       f.Pinned,
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
	_, err = w.append(ctx, entry)
	return err
}

// foldSplitOf returns the index into the items the writer wrote at which
// a fold's kept tail starts, and whether the fold can be placed at all.
// A fold that names its first kept item, as compact's does, is placed
// at its own index when the item is there, and otherwise at the one
// place the writer wrote that item, since a transform chained before
// the fold may have dropped or added items and the fold's index counts
// those; an item written nowhere, or in more than one place, the same
// value appended twice, is a fold the path cannot place. A fold that
// names no item is taken at its index, and one out of range is an
// error, as a transcript the recorder did not write.
func (w *writer) foldSplitOf(f compact.Fold) (int, bool, error) {
	if f.First != nil {
		if f.Split >= 0 && f.Split < len(w.values) && w.values[f.Split] == f.First {
			return f.Split, true, nil
		}
		at, n := -1, 0
		for i, v := range w.values {
			if v == f.First {
				at, n = i, n+1
			}
		}
		return at, n == 1, nil
	}
	if f.Split < 0 || f.Split >= len(w.items) {
		return 0, false, fmt.Errorf("session: fold keeps the transcript from item %d, but the recorder wrote %d items", f.Split, len(w.items))
	}
	return f.Split, true, nil
}

// child links the session of a child run to this one. A run that was
// observed has its session already, and its link when the call was on
// the observer's context; its writer is released, unless the call
// returned while the child runs on, detached, whose writer is released
// at the child's own run end. One that was not observed gets a session
// holding only its items, and a detached one that was not observed has
// none to write yet.
func (w *writer) child(ctx context.Context, callID string, info agent.ChildInfo) error {
	r := w.rec
	running := info.Reason == ""
	if cw, ok := r.runs[info.RunID]; ok {
		if running && cw.run != "" {
			cw.detached = true
		} else {
			delete(r.runs, info.RunID)
		}
		return w.link(ctx, cw.id, callID)
	}
	if running {
		return nil
	}
	cw, err := r.newChild(ctx, w, callID, false)
	if err != nil {
		return err
	}
	// The items are a transcript being copied, not a run being
	// watched: its outputs came from the child's own tools.
	cw.replay = true
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
	// The format reads a segment holding a function call output or a
	// decision as one that answered a call, which is the shape its
	// stopped step asks for; this is that test, taken as the entries
	// are written rather than by reading the segment back.
	switch v := e.(type) {
	case *agentsession.DecisionEntry:
		w.answeredCall = true
	case *agentsession.ItemEntry:
		if _, ok := v.Item.(*openresponses.FunctionCallOutput); ok {
			w.answeredCall = true
		}
	}
	return id, nil
}

// configDelta returns the config entry that takes prev to next, nil when
// they are equal, or full (a replace entry) when the change cannot be
// expressed as a delta: a model being cleared, a tool list change that
// a delta would not replay in the request's order, or a delta that
// would be larger than the replacement. parts, when set, are the parts
// next's instructions are composed of, and the instructions change is
// written as the parts that moved; otherwise it is the joined string,
// and parts on the path whose join is unchanged are left in place.
func configDelta(prev, next agentsession.Settings, full *agentsession.ConfigEntry, parts []agentsession.InstructionPart) *agentsession.ConfigEntry {
	if len(parts) == 0 {
		// The string is what is compared: parts in force that join to
		// the same text still describe it.
		prev.InstructionsParts = nil
	}
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
	switch {
	case len(parts) > 0:
		if pd := prev.InstructionsDelta(parts); pd != nil {
			d.InstructionsParts = pd.InstructionsParts
		}
	case next.Instructions != prev.Instructions:
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
	if equalJSON(d, &agentsession.ConfigEntry{}) {
		return nil
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
