// Package a2a is the consume side of A2A for agentturn: a remote A2A
// agent as a agenttool.Tool, so any agent reachable over A2A can be a
// component of a loop. The serve side, a loop exposed as an A2A agent,
// is the front/a2a module; the two round-trip in memory.
//
// One call is one task: the tool sends the arguments as a user message,
// follows the task to a final state and returns its text. A task that
// stops in input-required becomes a returned error carrying the agent's
// question, so the model can answer it in its next turn, with a new call
// and so a new task: the call gives up the task it leaves asking, as it
// does one left in auth-required. [WithInputRequired] answers the
// question inside the call instead, on the same task. The task and
// context IDs travel in Result.Details; a host that wants to resume a
// task itself makes the callee a peer. [WithDetach] makes a call the
// background task: it returns once the task runs, and the task's end
// goes to a host function that delivers it to the parent.
//
// The call records the callee it reaches as soon as it knows it, with
// agenttool.WriteRecord: a [Callee] naming the URL it called, the task
// and its context. The task is the call's own work, and the record names
// it a subsession; with [WithoutCancelOnAbort] it is a peer's, which
// outlives the call. When a call ends, no one asks anything more of the
// tasks it leaves unfinished, so it cancels each: the one it ends on,
// if that has not ended, and any the remote agent moved off in reply to
// an answer. A cancel is sent and not waited on, and recorded as
// [CancelSent].
//
//	card, _ := agentcard.DefaultResolver.Resolve(ctx, url)
//	client, _ := a2aclient.NewFromCard(ctx, card)
//	cfg.Tools = append(cfg.Tools, a2a.New(client, card))
package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
)

// Args are the arguments of the tool: the text sent to the remote agent.
type Args struct {
	Input string `json:"input" desc:"The request for the agent, in natural language"`
}

// TaskInfo is the Result.Details of every call: which task the call ran
// as and where it ended, the counterpart of tools/agent's ChildInfo.
type TaskInfo struct {
	TaskID    a2a.TaskID
	ContextID string
	State     a2a.TaskState
	// RecordErr is a failure to write the call's [Callee] record after
	// the remote agent had answered: the answer is returned, and a host
	// learns here that the record does not name the callee.
	RecordErr error
}

// InputRequiredError is returned when the remote agent needs more input
// before it can finish. The model sees the message. A host can resume a
// peer's task with the IDs; a subsession's task left in input-required
// or auth-required has been sent tasks/cancel. It matches
// agentturn.ErrInputRequired under errors.Is and implements
// agentturn.InputRequired.
type InputRequiredError struct {
	TaskID    a2a.TaskID
	ContextID string
	// Message is the agent's question, or empty when it sent none.
	Message string
	// waiting is what the task's status message waits on.
	waiting []agentturn.WaitingCall
}

// Error phrases the question for the model.
func (e *InputRequiredError) Error() string {
	if e.Message == "" {
		return "the agent needs more input before it can continue; call it again with the missing details"
	}
	return "the agent needs more input: " + e.Message
}

// Is reports whether target is agentturn.ErrInputRequired.
func (e *InputRequiredError) Is(target error) bool { return target == agentturn.ErrInputRequired }

// PendingCalls names what the task waits on, the task as the callee:
// each call its status message lists as a function_call data part, as
// front/a2a lists a call it puts to its caller, with its call ID, name
// and arguments, or else the question itself, by its message ID, or
// the task's ID when it sent none. For [agentturn.InputRequired].
func (e *InputRequiredError) PendingCalls() []agentturn.WaitingCall {
	if e.waiting != nil {
		return e.waiting
	}
	return []agentturn.WaitingCall{{ID: string(e.TaskID), Callee: string(e.TaskID)}}
}

// waitingOn names what an input-required task's status message waits
// on, as [InputRequiredError.PendingCalls] describes.
func waitingOn(task a2a.TaskID, status *a2a.Message) []agentturn.WaitingCall {
	var out []agentturn.WaitingCall
	if status == nil {
		return []agentturn.WaitingCall{{ID: string(task), Callee: string(task)}}
	}
	for _, part := range status.Parts {
		dp, ok := part.(a2a.DataPart)
		if !ok || dp.Data["type"] != "function_call" {
			continue
		}
		id, _ := dp.Data["call_id"].(string)
		name, _ := dp.Data["name"].(string)
		if id == "" {
			continue
		}
		var args json.RawMessage
		if a, ok := dp.Data["arguments"].(string); ok {
			args = json.RawMessage(a)
		}
		out = append(out, agentturn.WaitingCall{ID: id, Name: name, Args: args, Callee: string(task)})
	}
	if len(out) == 0 {
		id := status.ID
		if id == "" {
			id = string(task)
		}
		out = append(out, agentturn.WaitingCall{ID: id, Callee: string(task)})
	}
	return out
}

// Option configures the tool. The type it configures is unexported, so
// options come only from this package.
type Option func(*remote)

// WithName overrides the tool name, which defaults to the card's name
// made safe for a function name.
func WithName(name string) Option { return func(r *remote) { r.name = name } }

// WithDescription overrides the description, which defaults to the
// card's description followed by its skills.
func WithDescription(d string) Option { return func(r *remote) { r.description = d } }

// WithContextID sends every call in the same A2A context, so the remote
// agent sees the earlier calls as one conversation. The default is a
// fresh context per call.
func WithContextID(id string) Option { return func(r *remote) { r.contextID = id } }

// WithBlocking uses message/send and waits for the final task instead
// of streaming. No progress is reported.
func WithBlocking() Option { return func(r *remote) { r.blocking = true } }

// WithSequential marks the tool agenttool.Sequential.
func WithSequential() Option { return func(r *remote) { r.sequential = true } }

// WithoutCancelOnAbort leaves a task running when its call is given
// up: the remote agent is a peer whose work outlives the call and keeps
// its own record, and the call's [Callee] record names it with
// [RelationPeer]. By default a call sends tasks/cancel for each task it
// leaves unfinished, its context ended before the task did or the task
// left in input-required or auth-required, and the task is the call's
// own work, a [RelationSubsession].
func WithoutCancelOnAbort() Option { return func(r *remote) { r.peer = true } }

// CancelTimeout bounds each tasks/cancel a call sends as it ends. The call
// does not wait for it: it is sent from a goroutine of its own, under a
// context that outlives the call by at most this long.
const CancelTimeout = 10 * time.Second

// InputRequired answers a remote agent that asked for more input
// before it can finish a task: info names the task, and question is the
// agent's status message, its text and any data parts. The answer is
// sent as a user message on the same task and the call goes on. A hook
// that cannot answer returns an error matching agentturn.ErrInputRequired,
// and the call ends as it would without the hook, with an
// [InputRequiredError] the model reads; any other error ends the call
// with it.
type InputRequired func(ctx context.Context, info TaskInfo, question *a2a.Message) ([]a2a.Part, error)

// WithInputRequired answers a task that enters input-required through
// fn, on the same task, inside the call: a product wires fn to its
// human plane, so the remote agent's question reaches whoever answers
// the caller's own questions. A question with no status message is put
// as an empty agent message. An auth-required task is not put to fn; it
// ends the call, as does a task still asking after [WithInputRounds]
// answers. A call that ends input-required or auth-required, fn unable
// to answer or out of rounds, cancels the task as a call without fn
// does, unless the callee is a peer.
func WithInputRequired(fn InputRequired) Option { return func(r *remote) { r.inputRequired = fn } }

// DefaultInputRounds is how many times a call answers a task's
// questions through [WithInputRequired] before it ends input-required.
const DefaultInputRounds = 8

// WithInputRounds bounds the answers a call gives a task's questions
// through [WithInputRequired]; after n the call ends input-required as
// it would without the hook. The default is [DefaultInputRounds].
func WithInputRounds(n int) Option { return func(r *remote) { r.inputRounds = n } }

// WithDetach makes a call the background task (RFC 0001 Composition,
// as a peer): the call returns as soon as the remote agent names a task
// it has not finished, with an output saying the task is running and a
// [TaskInfo] naming it, and the task is followed on, by the stream the
// message was sent on or, under [WithBlocking], by a send that returns
// once the task exists (A2A §3.2.2) and tasks/get every
// [WithPollInterval] (§3.1.7). When the task is final, or waits on
// input [WithInputRequired] did not give, fn gets the call's ID, the
// task's [TaskInfo], the task as last seen and the error the call would
// have returned, nil when it completed; [Answer] is the output it
// would have returned. fn runs on the goroutine following the task, so
// the host delivers the answer to the parent from it with
// agentturn.Agent.Deliver, under agentturn.CalleeTrigger naming the
// call and the task, which a session recorder writes as the item's
// from_link to the call's peer link.
//
// The callee is a peer, whose work outlives the call: its [Callee]
// record names [RelationPeer], and the call's end leaves the task
// running. The task is followed on agentturn.RunContext of the call, so
// an abort of the parent's run cuts it, sending tasks/cancel and
// recording [CancelSent], and the parent's batch or run ending by
// itself does not; the records written after the call returned name
// the call. [WithInputRequired] still answers the task's questions
// while it is followed. A call whose answer is final before the task
// runs, a message answer or a task that ended at once, or that fails
// first, returns as any call does, and fn is not called: its callee is
// the call's own subsession, and a task it fails on is cancelled. Progress
// updates stop when the call returns.
func WithDetach(fn func(callID string, info TaskInfo, final *a2a.Task, err error)) Option {
	return func(r *remote) { r.detach = fn }
}

// DefaultPollInterval is how often a detached call under
// [WithBlocking] asks for its task with tasks/get.
const DefaultPollInterval = time.Second

// MinPollInterval is the shortest interval [WithPollInterval] takes.
const MinPollInterval = 50 * time.Millisecond

// WithPollInterval sets how often a detached call under [WithBlocking]
// asks for its task; the default is [DefaultPollInterval], and an
// interval shorter than [MinPollInterval] is raised to it.
func WithPollInterval(d time.Duration) Option {
	return func(r *remote) {
		r.pollInterval = max(d, MinPollInterval)
	}
}

// Answer is the output a call returns for a task that completed, the
// agent's text and the files of its artifacts, for the final task a
// [WithDetach] function gets: what a host delivers to the parent.
func Answer(final *a2a.Task) openresponses.FunctionCallOutputData {
	if final == nil {
		return openresponses.FunctionCallOutputData{}
	}
	o := outcome{status: final.Status.Message, artifacts: final.Artifacts}
	return output(o.text(), o.fileParts())
}

// Steer sends parts to a task that is still working, such as the one a
// [WithDetach] call follows, which info names: a message naming the
// task, the steer A2A allows an agent to accept (§3.3.3), as
// front/a2a's WithDeliverWhileBusy does. The send does not wait for the
// task (§3.2.2); it returns the task as the remote agent answered with
// it, and the call that follows the task reads the rest. A remote agent
// that refuses a message for a working task returns its error, and one
// that answers with a message rather than the task an error saying so.
func Steer(ctx context.Context, client *a2aclient.Client, info TaskInfo, parts ...a2a.Part) (*a2a.Task, error) {
	if info.TaskID == "" {
		return nil, errors.New("a2a: steer: no task")
	}
	msg := a2a.NewMessageForTask(a2a.MessageRoleUser, a2a.TaskInfo{TaskID: info.TaskID, ContextID: info.ContextID}, parts...)
	res, err := client.SendMessage(ctx, &a2a.MessageSendParams{Message: msg, Config: &a2a.MessageSendConfig{Blocking: new(bool)}})
	if err != nil {
		return nil, fmt.Errorf("a2a: steer: %w", err)
	}
	task, ok := res.(*a2a.Task)
	if !ok {
		return nil, fmt.Errorf("a2a: steer: the agent answered with %T, not the task", res)
	}
	return task, nil
}

// New wraps a remote agent as a tool. The client is usually built from
// the same card with a2aclient.NewFromCard. Build it with the
// [ObserveEndpoint] interceptor, however it is built, so a call records
// the URL its requests went to; New changes nothing on client, and
// without the interceptor a call records no URL, since the tool cannot
// tell which endpoint a client it did not build calls.
func New(client *a2aclient.Client, card *a2a.AgentCard, opts ...Option) agenttool.Tool {
	r := &remote{client: client, inputRounds: DefaultInputRounds, pollInterval: DefaultPollInterval}
	if card != nil {
		r.name = functionName(card.Name)
		r.description = describe(card)
	}
	for _, opt := range opts {
		opt(r)
	}
	if r.name == "" {
		r.name = "agent"
	}
	r.schema = argsSchema
	return r
}

// argsSchema is reflected once at init; Args is a fixed struct, so a
// failure is a programming error caught the first time the package
// loads.
var argsSchema = mustSchema()

func mustSchema() json.RawMessage {
	schema, err := agenttool.SchemaFor[Args]()
	if err != nil {
		panic("a2a: reflect Args: " + err.Error())
	}
	return schema
}

type remote struct {
	client        *a2aclient.Client
	name          string
	description   string
	contextID     string
	blocking      bool
	sequential    bool
	peer          bool
	inputRequired InputRequired
	inputRounds   int
	detach        func(callID string, info TaskInfo, final *a2a.Task, err error)
	pollInterval  time.Duration
	schema        json.RawMessage
}

func (r *remote) Name() string                { return r.name }
func (r *remote) Description() string         { return r.description }
func (r *remote) Parameters() json.RawMessage { return r.schema }
func (r *remote) Sequential() bool            { return r.sequential }
func (r *remote) Execute(ctx context.Context, call agenttool.Call) (agenttool.Result, error) {
	args, err := agenttool.Decode[Args](call.Args)
	if err != nil {
		return agenttool.Result{}, err
	}
	if strings.TrimSpace(args.Input) == "" {
		return agenttool.Result{}, errors.New("invalid arguments: input is required")
	}
	msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.TextPart{Text: args.Input})
	msg.ContextID = r.contextID

	ep := &endpoint{}
	c := &callState{remote: r, ep: ep}
	if r.detach != nil {
		return r.detached(ctx, call, msg, c)
	}
	var out outcome
	return r.follow(context.WithValue(ctx, endpointKey{}, ep), call, msg, &out, c)
}

// follow sends msg, follows the task it starts to a final state,
// answering its questions through [WithInputRequired], and returns the
// call's result, out holding the task as it was last seen.
func (r *remote) follow(ctx context.Context, call agenttool.Call, msg *a2a.Message, out *outcome, c *callState) (agenttool.Result, error) {
	for rounds := 0; ; rounds++ {
		err := r.send(ctx, call, &a2a.MessageSendParams{Message: msg}, out, c)
		if err != nil {
			if c.detached && ctx.Err() != nil {
				// The parent's run was aborted: a detached call's task
				// is cut with it, the peer though it is, and the abort
				// is what ended it.
				c.stop(ctx, out)
				return agenttool.Result{}, fmt.Errorf("a2a: %w", context.Cause(ctx))
			}
			// The call gives up the tasks that have not ended, aborted or
			// failed alike: its own work is stopped, a detached call's
			// too when it fails before it returns.
			c.undetach()
			c.leave(ctx, out)
			return agenttool.Result{}, err
		}
		if out.state != a2a.TaskStateInputRequired || r.inputRequired == nil || out.taskID == "" || rounds >= r.inputRounds {
			break
		}
		info := TaskInfo{TaskID: out.taskID, ContextID: out.contextID, State: out.state}
		question := out.status
		if question == nil {
			question = a2a.NewMessageForTask(a2a.MessageRoleAgent, a2a.TaskInfo{TaskID: out.taskID, ContextID: out.contextID})
		}
		answer, err := r.inputRequired(ctx, info, question)
		if errors.Is(err, agentturn.ErrInputRequired) {
			break
		}
		if err != nil {
			// The call gives the task up: its own work is stopped.
			c.undetach()
			c.leave(ctx, out)
			return agenttool.Result{Details: info}, err
		}
		msg = a2a.NewMessageForTask(a2a.MessageRoleUser, a2a.TaskInfo{TaskID: out.taskID, ContextID: out.contextID}, answer...)
		out.answered()
	}
	// The call is over, and no one is asking for the work of a task it
	// leaves unfinished: one that waits on input or credentials nobody
	// gave it, since the model that reads the question calls again and
	// so starts a new task; one whose stream closed, or whose blocking
	// call returned, before it ended; an earlier task the remote agent
	// moved off with its reply to an answer. Each is stopped as an abort
	// stops it.
	c.leave(ctx, out)
	return out.result()
}

// detached sends msg and returns once the remote agent names a task it
// has not finished, with an output saying the task is running, and
// follows the task on the run context, handing its end to the
// [WithDetach] function. A call whose answer is final before then, a
// message or a task already ended, or that fails first, returns as a
// call that is not detached does, and fn is not called.
func (r *remote) detached(ctx context.Context, call agenttool.Call, msg *a2a.Message, c *callState) (agenttool.Result, error) {
	// The task outlives the call: it is followed on the run's context,
	// with the call on it, so its records name the call after the call
	// has returned, and an abort of the run still cuts it.
	fctx := ctx
	if rc, ok := agentturn.RunContext(ctx); ok {
		fctx = agenttool.WithCall(rc, call)
	}
	fctx = context.WithValue(fctx, endpointKey{}, c.ep)
	started := make(chan TaskInfo, 1)
	c.detached = true
	c.started = func(info TaskInfo) { started <- info }
	type ended struct {
		res agenttool.Result
		err error
	}
	done := make(chan ended, 1)
	go func() {
		var out outcome
		res, err := r.follow(fctx, call, msg, &out, c)
		if c.started == nil {
			// The call returned saying the task runs: its end is fn's.
			r.detach(call.ID, out.info(), out.task(), err)
			return
		}
		done <- ended{res, err}
	}()
	select {
	case info := <-started:
		return agenttool.Result{
			Output:  openresponses.FunctionCallOutputData{Text: fmt.Sprintf("agent %s is working on it in the background as task %s; its answer will follow", strconv.Quote(r.name), info.TaskID)},
			Details: info,
		}, nil
	case e := <-done:
		return e.res, e.err
	}
}

// callState is what one call knows about the callee it reached.
type callState struct {
	remote *remote
	ep     *endpoint
	// tasks are the tasks the call reached, in the order it reached them.
	tasks []*task
	// reportedM is whether the [Callee] record of a message answer is
	// written.
	reportedM bool
	// detached is whether the call is [WithDetach]'s; started, until
	// the call returns, is called once the remote agent names a task it
	// has not finished, and is nil from then on: the call has returned,
	// and its progress updates have nowhere to go.
	detached bool
	started  func(TaskInfo)
	// detaching is whether a detached call's callee is a peer: the task
	// it is about to return on, or has returned on, runs on. A detached
	// call that returns before then, its answer final at once or a
	// failure first, is an ordinary call, and its task the call's own.
	detaching bool
}

// detachOn notes, for a detached call that has not returned, whether
// the answer it just read leaves a task running, on which it returns.
func (c *callState) detachOn(out *outcome, final bool) {
	if c.detached && c.started != nil {
		c.detaching = !final && out.taskID != ""
	}
}

// undetach makes a detached call that has not returned an ordinary
// call, for a failure before it returns: the task it leaves is stopped
// as a call that is not detached stops it.
func (c *callState) undetach() {
	if c.detached && c.started != nil {
		c.detaching = false
	}
}

// task is one task a call reached.
type task struct {
	id        a2a.TaskID
	contextID string
	// state is the last state the task was seen in. running is whether
	// a task event named it; a task only a message answer names is not
	// known to run.
	state   a2a.TaskState
	running bool
	// reported is whether its [Callee] record is written, so the answers
	// on one task do not write it again; cancelled whether tasks/cancel
	// was sent for it.
	reported  bool
	cancelled bool
}

// seen folds out into the task it names, and returns it, or nil when
// out names no task.
func (c *callState) seen(out *outcome) *task {
	if out.taskID == "" {
		return nil
	}
	var t *task
	for _, have := range c.tasks {
		if have.id == out.taskID {
			t = have
			break
		}
	}
	if t == nil {
		t = &task{id: out.taskID}
		c.tasks = append(c.tasks, t)
	}
	if out.contextID != "" {
		t.contextID = out.contextID
	}
	if out.state != "" {
		t.state = out.state
	}
	if out.message == nil {
		t.running = true
	}
	return t
}

// relation is the link the call's callee is.
func (c *callState) relation() string {
	if c.remote.peer || c.detaching {
		return RelationPeer
	}
	return RelationSubsession
}

// url is the endpoint the call's requests went to, which the client
// tells when it carries the [ObserveEndpoint] interceptor. Without it
// the call cannot tell which endpoint was called, the client having
// been built some way the tool does not see, and records none rather
// than one that may be wrong.
func (c *callState) url() string { return c.ep.get() }

// report writes the [Callee] record once the callee is known: a task,
// or a plain message answer with no task. It is written once per task,
// under a context the call's end does not cancel, since it is the
// handle a given-up call leaves behind. A message that answers after a
// task ran, the reply to an answer given on it, gets a record of its
// own beside the task's: the message answered the call, while the task
// is left and cancelled.
func (c *callState) report(ctx context.Context, out *outcome) error {
	t := c.seen(out)
	if t != nil {
		if err := c.callee(ctx, t); err != nil {
			return err
		}
	}
	if out.message == nil || c.reportedM || (t != nil && !t.running) {
		return nil
	}
	if err := agenttool.WriteRecord(context.WithoutCancel(ctx), Callee{URL: c.url(), Context: out.contextID, Relation: c.relation()}); err != nil {
		return err
	}
	c.reportedM = true
	return nil
}

// callee writes t's [Callee] record unless it is written. It is marked
// once it is written, so a failed write is tried again.
func (c *callState) callee(ctx context.Context, t *task) error {
	if t.reported {
		return nil
	}
	if err := agenttool.WriteRecord(context.WithoutCancel(ctx), Callee{URL: c.url(), Task: string(t.id), Context: t.contextID, Relation: c.relation()}); err != nil {
		return err
	}
	t.reported = true
	return nil
}

// leave sends tasks/cancel for each task the call leaves unfinished,
// unless the callee is a peer: every task a task event named whose last
// state was not terminal, the one the call ends on and any it moved off,
// each once. It records each cancel after the task's [Callee] record,
// which it writes first if it is not written yet; when that write fails
// again the cancel is recorded all the same, about a task no [Callee]
// names, rather than lost. It does not wait for the
// remote agent: each request goes from a goroutine of its own, under a
// context that outlives the call by at most [CancelTimeout], and the
// record says the stop was asked for, not that the work stopped.
func (c *callState) leave(ctx context.Context, out *outcome) {
	if c.relation() == RelationPeer {
		return
	}
	c.stop(ctx, out)
}

// stop sends tasks/cancel for each task the call leaves unfinished, as
// leave does, whatever the callee's relation: a detached call's task,
// a peer's, is stopped so when the parent's run is aborted.
func (c *callState) stop(ctx context.Context, out *outcome) {
	c.seen(out)
	for _, t := range c.tasks {
		if t.cancelled || !t.running || t.state.Terminal() {
			continue
		}
		t.cancelled = true
		id := t.id
		cctx, stop := context.WithTimeout(context.WithoutCancel(ctx), CancelTimeout)
		go func() {
			defer stop()
			_, _ = c.remote.client.CancelTask(cctx, &a2a.TaskIDParams{ID: id})
		}()
		// The call is over, so its context may be too; the records are
		// written under one that is not, and their failure changes
		// nothing the call returns.
		_ = c.callee(ctx, t)
		_ = agenttool.WriteRecord(context.WithoutCancel(ctx), CancelSent{URL: c.url(), Task: string(id)})
	}
}

// send sends one message and folds the answer into out, reporting the
// callee as soon as it is known. It returns when the task is final or
// waits on input. A record the call cannot write while the task runs
// fails the call, which then gives the task up; once the remote agent
// has answered, the answer stands and the record's failure is on
// [TaskInfo.RecordErr].
func (r *remote) send(ctx context.Context, call agenttool.Call, params *a2a.MessageSendParams, out *outcome, c *callState) error {
	if r.blocking {
		if c.detached {
			// The send returns once the task exists (A2A §3.2.2), and the
			// task is followed by tasks/get (§3.1.7).
			params.Config = &a2a.MessageSendConfig{Blocking: new(bool)}
		}
		res, err := r.client.SendMessage(ctx, params)
		if err != nil {
			return fmt.Errorf("a2a: %w", err)
		}
		final := out.absorb(res)
		c.detachOn(out, final)
		if err := out.recorded(c.report(ctx, out), final); err != nil || final || !c.detached || out.taskID == "" {
			return err
		}
		c.start(out)
		return r.poll(ctx, out, c)
	}
	for ev, err := range r.client.SendStreamingMessage(ctx, params) {
		if err != nil {
			return fmt.Errorf("a2a: %w", err)
		}
		final := out.absorb(ev)
		c.detachOn(out, final)
		if err := out.recorded(c.report(ctx, out), final); err != nil {
			return err
		}
		if final {
			break
		}
		c.start(out)
		if _, ok := ev.(*a2a.TaskArtifactUpdateEvent); ok && !c.detached {
			call.Update(agenttool.Text(out.progress()))
		}
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("a2a: %w", err)
	}
	return nil
}

// start tells a detached call's Execute, once, that the remote agent
// named a task it has not finished, so the call returns.
func (c *callState) start(out *outcome) {
	if c.started == nil || out.taskID == "" {
		return
	}
	started := c.started
	c.started = nil
	started(out.info())
}

// poll follows a task a non-blocking send left unfinished with
// tasks/get, every [WithPollInterval], until it is final or waits on
// input or credentials.
func (r *remote) poll(ctx context.Context, out *outcome, c *callState) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("a2a: %w", ctx.Err())
		case <-time.After(r.pollInterval):
		}
		task, err := r.client.GetTask(ctx, &a2a.TaskQueryParams{ID: out.taskID})
		if err != nil {
			return fmt.Errorf("a2a: %w", err)
		}
		final := out.absorb(task)
		if err := out.recorded(c.report(ctx, out), final); err != nil || final {
			return err
		}
	}
}

// recorded applies a failure to write the [Callee] record: it fails the
// call while the task runs, and is kept on the result once the task
// has answered.
func (o *outcome) recorded(err error, final bool) error {
	if err == nil {
		return nil
	}
	if final && (o.state.Terminal() || o.message != nil) {
		o.recordErr = err
		return nil
	}
	return fmt.Errorf("a2a: record the callee: %w", err)
}

// outcome folds task events into a final view of the task.
type outcome struct {
	taskID    a2a.TaskID
	contextID string
	state     a2a.TaskState
	status    *a2a.Message
	message   *a2a.Message
	artifacts []*a2a.Artifact
	// streamed is the artifact text so far, grown chunk by chunk so
	// progress updates do not re-join every artifact; a replaced
	// artifact marks it for a rebuild.
	streamed strings.Builder
	rebuild  bool
	// recordErr is a failure to write the Callee record after the task
	// answered.
	recordErr error
}

// answered readies o for the agent's reply to an answer sent on the
// same task: the state and status are the reply's.
func (o *outcome) answered() {
	o.state = ""
	o.status = nil
}

// absorb applies one event and reports whether it was final.
func (o *outcome) absorb(ev a2a.Event) bool {
	info := ev.TaskInfo()
	if info.TaskID != "" {
		o.taskID = info.TaskID
	}
	if info.ContextID != "" {
		o.contextID = info.ContextID
	}
	switch e := ev.(type) {
	case *a2a.Message:
		o.message = e
		return true
	case *a2a.Task:
		o.state = e.Status.State
		o.status = e.Status.Message
		o.artifacts = e.Artifacts
		// A task waiting on input or credentials is as far as the call
		// follows it: a poll that met either would ask again forever.
		return e.Status.State.Terminal() || e.Status.State == a2a.TaskStateInputRequired || e.Status.State == a2a.TaskStateAuthRequired
	case *a2a.TaskStatusUpdateEvent:
		o.state = e.Status.State
		if e.Status.Message != nil {
			o.status = e.Status.Message
		}
		return e.Final
	case *a2a.TaskArtifactUpdateEvent:
		o.artifact(e)
	}
	return false
}

func (o *outcome) artifact(e *a2a.TaskArtifactUpdateEvent) {
	if e.Artifact == nil {
		return
	}
	// Parts are always copied: the event's slice belongs to the SDK
	// and a later append must not write into its backing array.
	cp := *e.Artifact
	cp.Parts = append(a2a.ContentParts(nil), e.Artifact.Parts...)
	for _, a := range o.artifacts {
		if a.ID == e.Artifact.ID {
			if e.Append {
				a.Parts = append(a.Parts, cp.Parts...)
				o.streamed.WriteString(partsText(cp.Parts))
			} else {
				*a = cp
				o.rebuild = true
			}
			return
		}
	}
	o.artifacts = append(o.artifacts, &cp)
	o.streamed.WriteString(partsText(cp.Parts))
}

// progress returns the artifact text streamed so far, for updates
// during a task.
func (o *outcome) progress() string {
	if o.rebuild {
		o.streamed.Reset()
		for _, a := range o.artifacts {
			o.streamed.WriteString(partsText(a.Parts))
		}
		o.rebuild = false
	}
	return o.streamed.String()
}

// text returns the agent's answer: the final message, else the status
// message, else the artifacts.
func (o *outcome) text() string {
	if o.message != nil {
		if s := partsText(o.message.Parts); s != "" {
			return s
		}
	}
	if o.status != nil {
		if s := partsText(o.status.Parts); s != "" {
			return s
		}
	}
	var b strings.Builder
	for _, a := range o.artifacts {
		b.WriteString(partsText(a.Parts))
	}
	return b.String()
}

// info is the [TaskInfo] of the task as last seen.
func (o *outcome) info() TaskInfo {
	return TaskInfo{TaskID: o.taskID, ContextID: o.contextID, State: o.state, RecordErr: o.recordErr}
}

// task is the task as last seen, for a detached call's end: its state,
// its status message, or the agent's closing message when the agent
// answered with a message, and its artifacts; nil when the agent named
// no task.
func (o *outcome) task() *a2a.Task {
	if o.taskID == "" && o.message == nil {
		return nil
	}
	status := o.status
	if o.message != nil {
		status = o.message
	}
	return &a2a.Task{ID: o.taskID, ContextID: o.contextID, Status: a2a.TaskStatus{State: o.state, Message: status}, Artifacts: o.artifacts}
}

func (o *outcome) result() (agenttool.Result, error) {
	res := agenttool.Result{Details: o.info()}
	if o.message != nil && o.state == "" {
		// A bare message reply, no task.
		res.Output = output(o.text(), o.fileParts())
		return res, nil
	}
	switch o.state {
	case a2a.TaskStateCompleted:
		res.Output = output(o.text(), o.fileParts())
		return res, nil
	case a2a.TaskStateInputRequired, a2a.TaskStateAuthRequired:
		return res, &InputRequiredError{TaskID: o.taskID, ContextID: o.contextID, Message: o.text(), waiting: waitingOn(o.taskID, o.status)}
	case a2a.TaskStateCanceled:
		return res, errors.New("the agent's task was canceled")
	case a2a.TaskStateRejected:
		return res, fmt.Errorf("the agent rejected the task: %s", o.text())
	case a2a.TaskStateFailed:
		return res, fmt.Errorf("the agent's task failed: %s", o.text())
	}
	return res, fmt.Errorf("the agent's task ended in state %q", o.state)
}

// fileParts converts file parts of the artifacts into content parts.
// The conversion mirrors front/a2a's contentFromFile; change both
// together.
func (o *outcome) fileParts() openresponses.Contents {
	var out openresponses.Contents
	for _, a := range o.artifacts {
		for _, p := range a.Parts {
			fp, ok := p.(a2a.FilePart)
			if !ok {
				continue
			}
			switch f := fp.File.(type) {
			case a2a.FileBytes:
				if strings.HasPrefix(f.MimeType, "image/") {
					out = append(out, &openresponses.InputImage{ImageURL: "data:" + f.MimeType + ";base64," + f.Bytes})
				} else {
					out = append(out, &openresponses.InputFile{Filename: f.Name, FileData: f.Bytes})
				}
			case a2a.FileURI:
				if strings.HasPrefix(f.MimeType, "image/") {
					out = append(out, &openresponses.InputImage{ImageURL: f.URI})
				} else {
					out = append(out, &openresponses.InputFile{Filename: f.Name, FileURL: f.URI})
				}
			}
		}
	}
	return out
}

func output(text string, files openresponses.Contents) openresponses.FunctionCallOutputData {
	if len(files) == 0 {
		return openresponses.FunctionCallOutputData{Text: text}
	}
	parts := openresponses.Contents{&openresponses.Text{Text: text}}
	return openresponses.FunctionCallOutputData{Parts: append(parts, files...)}
}

// partsText mirrors front/a2a's partsText; change both together.
func partsText(parts a2a.ContentParts) string {
	var b strings.Builder
	for _, p := range parts {
		if t, ok := p.(a2a.TextPart); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String()
}

// functionName turns a card name into a function name: lowercase, with
// runs of anything but letters, digits and underscores collapsed to one
// underscore.
func functionName(name string) string {
	var b strings.Builder
	underscore := false
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			underscore = false
		default:
			if !underscore && b.Len() > 0 {
				b.WriteByte('_')
			}
			underscore = true
		}
	}
	return strings.TrimSuffix(b.String(), "_")
}

func describe(card *a2a.AgentCard) string {
	var b strings.Builder
	b.WriteString(strings.TrimSpace(card.Description))
	if len(card.Skills) > 0 {
		b.WriteString("\nSkills:")
		for _, s := range card.Skills {
			b.WriteString("\n- ")
			b.WriteString(s.Name)
			if s.Description != "" {
				b.WriteString(": ")
				b.WriteString(s.Description)
			}
		}
	}
	return b.String()
}

var (
	_ agenttool.Tool       = (*remote)(nil)
	_ agenttool.Sequential = (*remote)(nil)
)
