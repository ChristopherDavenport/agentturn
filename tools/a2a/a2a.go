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
// does one left in auth-required.
// [WithInputRequired] answers the question inside the call instead, on
// the same task. The task and context IDs travel in Result.Details; a
// host that wants to resume a task itself makes the callee a peer.
//
// The call records the callee it reaches as soon as it knows it, with
// agenttool.WriteRecord: a [Callee] naming the URL it called, the task
// and its context. The task is the call's own work, which the call
// cancels when it gives it up, left asking for input included, and the
// record names it a subsession; with [WithoutCancelOnAbort] it is a
// peer's, which outlives the call. A cancel is sent and not waited on,
// and recorded as [CancelSent].
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
// agentturn.ErrInputRequired under errors.Is.
type InputRequiredError struct {
	TaskID    a2a.TaskID
	ContextID string
	// Message is the agent's question, or empty when it sent none.
	Message string
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
// [RelationPeer]. By default a call whose context ends before its task
// does, or that ends with its task in input-required or auth-required,
// sends tasks/cancel for the task, and the task is the call's own work,
// a [RelationSubsession].
func WithoutCancelOnAbort() Option { return func(r *remote) { r.peer = true } }

// CancelTimeout bounds the tasks/cancel a given-up call sends. The call
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
// ends the call as before, as does a task still asking after
// [WithInputRounds] answers. A call that ends input-required or
// auth-required, fn unable to answer or out of rounds, cancels the task
// as a call without fn does, unless the callee is a peer.
func WithInputRequired(fn InputRequired) Option { return func(r *remote) { r.inputRequired = fn } }

// DefaultInputRounds is how many times a call answers a task's
// questions through [WithInputRequired] before it ends input-required.
const DefaultInputRounds = 8

// WithInputRounds bounds the answers a call gives a task's questions
// through [WithInputRequired]; after n the call ends input-required as
// it would without the hook. The default is [DefaultInputRounds].
func WithInputRounds(n int) Option { return func(r *remote) { r.inputRounds = n } }

// New wraps a remote agent as a tool. The client is usually built from
// the same card with a2aclient.NewFromCard. Build it with the
// [ObserveEndpoint] interceptor, however it is built, so a call records
// the URL its requests went to; New changes nothing on client, and
// without the interceptor a call records no URL, since the tool cannot
// tell which endpoint a client it did not build calls.
func New(client *a2aclient.Client, card *a2a.AgentCard, opts ...Option) agenttool.Tool {
	r := &remote{client: client, inputRounds: DefaultInputRounds}
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
	ctx = context.WithValue(ctx, endpointKey{}, ep)
	c := &callState{remote: r, ep: ep}
	var out outcome
	for rounds := 0; ; rounds++ {
		err := r.send(ctx, call, &a2a.MessageSendParams{Message: msg}, &out, c)
		if err != nil {
			// The call gives up a task that has not ended, aborted or
			// failed alike: its own work is stopped.
			if !out.state.Terminal() {
				c.cancel(ctx, &out)
			}
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
			c.cancel(ctx, &out)
			return agenttool.Result{Details: info}, err
		}
		msg = a2a.NewMessageForTask(a2a.MessageRoleUser, a2a.TaskInfo{TaskID: out.taskID, ContextID: out.contextID}, answer...)
		out.answered()
	}
	switch out.state {
	case a2a.TaskStateInputRequired, a2a.TaskStateAuthRequired:
		// The call ends asking for input or credentials nobody gave it,
		// and the model that reads the question calls again, which
		// starts a new task: nothing comes back to this one, so the
		// call's own work is stopped. A peer's task is left to the peer.
		c.cancel(ctx, &out)
	default:
		if out.taskID != "" && !out.state.Terminal() && out.message == nil {
			// The stream closed, or a blocking call returned, before
			// the task ended: the call is over, and its own work is
			// stopped as an abort stops it. A message answer ends the
			// exchange; there is nothing left running.
			c.cancel(ctx, &out)
		}
	}
	return out.result()
}

// callState is what one call knows about the callee it reached.
type callState struct {
	remote *remote
	ep     *endpoint
	// reported is the task the call last wrote a [Callee] record for, so
	// the answers on one task do not write it again.
	reported  string
	reportedM bool
}

// relation is the link the call's callee is.
func (c *callState) relation() string {
	if c.remote.peer {
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
// handle a given-up call leaves behind.
func (c *callState) report(ctx context.Context, out *outcome) error {
	switch {
	case out.taskID != "":
		if c.reported == string(out.taskID) {
			return nil
		}
	case out.message != nil:
		if c.reportedM {
			return nil
		}
	default:
		return nil
	}
	if err := agenttool.WriteRecord(context.WithoutCancel(ctx), Callee{URL: c.url(), Task: string(out.taskID), Context: out.contextID, Relation: c.relation()}); err != nil {
		return err
	}
	// Marked once it is written, so a failed write is tried again.
	if out.taskID != "" {
		c.reported = string(out.taskID)
	} else {
		c.reportedM = true
	}
	return nil
}

// cancel sends tasks/cancel for a task the call gave up, unless the
// callee is a peer, and records that it was sent, after the [Callee]
// record if the call had not written it yet, so a cancel is never
// recorded about a callee the record does not name. It does not wait
// for the remote agent: the request goes from a goroutine of its own,
// under a context that outlives the call by at most [CancelTimeout],
// and the record says the stop was asked for, not that the work
// stopped.
func (c *callState) cancel(ctx context.Context, out *outcome) {
	task := out.taskID
	if c.remote.peer || task == "" {
		return
	}
	cctx, stop := context.WithTimeout(context.WithoutCancel(ctx), CancelTimeout)
	go func() {
		defer stop()
		_, _ = c.remote.client.CancelTask(cctx, &a2a.TaskIDParams{ID: task})
	}()
	// The call is over, so its context may be too; the records are
	// written under one that is not, and their failure changes nothing
	// the call returns.
	_ = c.report(ctx, out)
	_ = agenttool.WriteRecord(context.WithoutCancel(ctx), CancelSent{URL: c.url(), Task: string(task)})
}

// send sends one message and folds the answer into out, reporting the
// callee as soon as it is known. It returns when the task is final or
// waits on input. A record the call cannot write while the task runs
// fails the call, which then gives the task up; once the remote agent
// has answered, the answer stands and the record's failure is on
// [TaskInfo.RecordErr].
func (r *remote) send(ctx context.Context, call agenttool.Call, params *a2a.MessageSendParams, out *outcome, c *callState) error {
	if r.blocking {
		res, err := r.client.SendMessage(ctx, params)
		if err != nil {
			return fmt.Errorf("a2a: %w", err)
		}
		final := out.absorb(res)
		return out.recorded(c.report(ctx, out), final)
	}
	for ev, err := range r.client.SendStreamingMessage(ctx, params) {
		if err != nil {
			return fmt.Errorf("a2a: %w", err)
		}
		final := out.absorb(ev)
		if err := out.recorded(c.report(ctx, out), final); err != nil {
			return err
		}
		if final {
			break
		}
		if _, ok := ev.(*a2a.TaskArtifactUpdateEvent); ok {
			call.Update(agenttool.Text(out.progress()))
		}
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("a2a: %w", err)
	}
	return nil
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
		return e.Status.State.Terminal() || e.Status.State == a2a.TaskStateInputRequired
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

func (o *outcome) result() (agenttool.Result, error) {
	details := TaskInfo{TaskID: o.taskID, ContextID: o.contextID, State: o.state, RecordErr: o.recordErr}
	res := agenttool.Result{Details: details}
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
		return res, &InputRequiredError{TaskID: o.taskID, ContextID: o.contextID, Message: o.text()}
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
