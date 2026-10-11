// Package a2a is the consume side of A2A for agentturn: a remote A2A
// agent as a agenttool.Tool, so any agent reachable over A2A can be a
// component of a loop. The serve side, a loop exposed as an A2A agent,
// is the front/a2a module; the two round-trip in memory.
//
// One call is one task: the tool sends the arguments as a user message,
// follows the task to a final state and returns its text. A task that
// stops in input-required becomes a returned error carrying the agent's
// question, so the model can answer it in its next turn; the task and
// context IDs travel in Result.Details for a host that wants to resume
// the task itself. [WithInputRequired] answers the question inside the
// call instead, on the same task.
//
// The call records the callee it reaches as soon as it knows it, with
// agenttool.WriteRecord: a [Callee] naming the URL it called, the task
// and its context. The task is the call's own work, which the call
// cancels when it is given up, and the record names it a subsession; with
// [WithoutCancelOnAbort] it is a peer's, which outlives the call. A
// cancel is sent and not waited on, and recorded as [CancelSent].
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
}

// InputRequiredError is returned when the remote agent needs more input
// before it can finish. The model sees the message; a host can resume
// the task with the IDs. It matches agentturn.ErrInputRequired under
// errors.Is.
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
// does sends tasks/cancel for the task, and the task is the call's own
// work, a [RelationSubsession].
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
// the caller's own questions. An auth-required task is not put to fn;
// it ends the call as before.
func WithInputRequired(fn InputRequired) Option { return func(r *remote) { r.inputRequired = fn } }

// New wraps a remote agent as a tool. The client is usually built from
// the same card with a2aclient.NewFromCard. New adds an interceptor to
// client that reads the URL each request is sent to, which a call
// records as its [Callee]'s, so a client shared by several tools carries
// one per tool; each reads only its own calls.
func New(client *a2aclient.Client, card *a2a.AgentCard, opts ...Option) agenttool.Tool {
	r := &remote{client: client}
	if client != nil {
		client.AddCallInterceptor(endpointWatch{})
	}
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
	for {
		err := r.send(ctx, call, &a2a.MessageSendParams{Message: msg}, &out, c)
		if err != nil {
			// The call gives up a task that has not ended, aborted or
			// failed alike: its own work is stopped.
			if !out.state.Terminal() {
				c.cancel(ctx, out.taskID)
			}
			return agenttool.Result{}, err
		}
		if out.state != a2a.TaskStateInputRequired || r.inputRequired == nil || out.taskID == "" {
			break
		}
		info := TaskInfo{TaskID: out.taskID, ContextID: out.contextID, State: out.state}
		answer, err := r.inputRequired(ctx, info, out.status)
		if errors.Is(err, agentturn.ErrInputRequired) {
			break
		}
		if err != nil {
			// The call gives the task up: its own work is stopped.
			c.cancel(ctx, out.taskID)
			return agenttool.Result{Details: info}, err
		}
		msg = a2a.NewMessageForTask(a2a.MessageRoleUser, a2a.TaskInfo{TaskID: out.taskID, ContextID: out.contextID}, answer...)
		out.answered()
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

// report writes the [Callee] record once the callee is known: a task,
// or a plain message answer with no task. It is written once per task.
func (c *callState) report(ctx context.Context, out *outcome) error {
	switch {
	case out.taskID != "":
		if c.reported == string(out.taskID) {
			return nil
		}
		c.reported = string(out.taskID)
	case out.message != nil:
		if c.reportedM {
			return nil
		}
		c.reportedM = true
	default:
		return nil
	}
	return agenttool.WriteRecord(ctx, Callee{URL: c.ep.get(), Task: string(out.taskID), Context: out.contextID, Relation: c.relation()})
}

// cancel sends tasks/cancel for a task the call gave up, unless the
// callee is a peer, and records that it was sent. It does not wait for
// the remote agent: the request goes from a goroutine of its own, under
// a context that outlives the call by at most [CancelTimeout], and the
// record says the stop was asked for, not that the work stopped.
func (c *callState) cancel(ctx context.Context, task a2a.TaskID) {
	if c.remote.peer || task == "" {
		return
	}
	cctx, stop := context.WithTimeout(context.WithoutCancel(ctx), CancelTimeout)
	go func() {
		defer stop()
		_, _ = c.remote.client.CancelTask(cctx, &a2a.TaskIDParams{ID: task})
	}()
	// The call is over, so its context may be too; the record is written
	// under one that is not, and its failure changes nothing the call
	// returns.
	_ = agenttool.WriteRecord(context.WithoutCancel(ctx), CancelSent{URL: c.ep.get(), Task: string(task)})
}

// send sends one message and folds the answer into out, reporting the
// callee as soon as it is known. It returns when the task is final or
// waits on input.
func (r *remote) send(ctx context.Context, call agenttool.Call, params *a2a.MessageSendParams, out *outcome, c *callState) error {
	if r.blocking {
		res, err := r.client.SendMessage(ctx, params)
		if err != nil {
			return fmt.Errorf("a2a: %w", err)
		}
		out.absorb(res)
		if err := c.report(ctx, out); err != nil {
			return fmt.Errorf("a2a: record the callee: %w", err)
		}
		return nil
	}
	for ev, err := range r.client.SendStreamingMessage(ctx, params) {
		if err != nil {
			return fmt.Errorf("a2a: %w", err)
		}
		final := out.absorb(ev)
		if err := c.report(ctx, out); err != nil {
			return fmt.Errorf("a2a: record the callee: %w", err)
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
	details := TaskInfo{TaskID: o.taskID, ContextID: o.contextID, State: o.state}
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
