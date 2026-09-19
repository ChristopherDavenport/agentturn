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
// the task itself.
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

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
)

// Args are the arguments of the tool: the text sent to the remote agent.
type Args struct {
	Input string `json:"input" desc:"The request for the agent, in natural language"`
}

// Details is the app-only detail on every result: which task the call
// ran as and where it ended.
type Details struct {
	TaskID    a2a.TaskID
	ContextID string
	State     a2a.TaskState
}

// InputRequiredError is returned when the remote agent needs more input
// before it can finish. The model sees the message; a host can resume
// the task with the IDs.
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

// Option configures the tool.
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

// New wraps a remote agent as a tool. The client is usually built from
// the same card with a2aclient.NewFromCard.
func New(client *a2aclient.Client, card *a2a.AgentCard, opts ...Option) agenttool.Tool {
	r := &remote{client: client}
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
	schema, err := agenttool.SchemaFor[Args](false)
	if err != nil {
		panic(err) // Args is a fixed struct; this cannot fail.
	}
	r.schema = schema
	return r
}

type remote struct {
	client      *a2aclient.Client
	name        string
	description string
	contextID   string
	blocking    bool
	sequential  bool
	schema      json.RawMessage
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
	params := &a2a.MessageSendParams{Message: msg}

	var out outcome
	if r.blocking {
		res, err := r.client.SendMessage(ctx, params)
		if err != nil {
			return agenttool.Result{}, fmt.Errorf("a2a: %w", err)
		}
		out.absorb(res)
	} else {
		for ev, err := range r.client.SendStreamingMessage(ctx, params) {
			if err != nil {
				return agenttool.Result{}, fmt.Errorf("a2a: %w", err)
			}
			if out.absorb(ev) {
				break
			}
			if _, ok := ev.(*a2a.TaskArtifactUpdateEvent); ok {
				call.Update(agenttool.Text(out.text()))
			}
		}
	}
	return out.result()
}

// outcome folds task events into a final view of the task.
type outcome struct {
	taskID    a2a.TaskID
	contextID string
	state     a2a.TaskState
	status    *a2a.Message
	message   *a2a.Message
	artifacts []*a2a.Artifact
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
	for _, a := range o.artifacts {
		if a.ID == e.Artifact.ID {
			if e.Append {
				a.Parts = append(a.Parts, e.Artifact.Parts...)
			} else {
				*a = *e.Artifact
			}
			return
		}
	}
	cp := *e.Artifact
	cp.Parts = append(a2a.ContentParts(nil), e.Artifact.Parts...)
	o.artifacts = append(o.artifacts, &cp)
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
	details := Details{TaskID: o.taskID, ContextID: o.contextID, State: o.state}
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
