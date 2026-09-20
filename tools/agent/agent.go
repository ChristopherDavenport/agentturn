// Package agent wraps an agent configuration as a tool, so one loop can
// delegate to another in process. It is the sub-agent pattern: the
// parent's model calls a tool named after the child, the child runs its
// own loop on a fresh transcript with its own hooks and tools, and the
// child's final answer is the tool output.
//
//	specialist := agent.New(agentturn.Config{
//		Name:        "researcher",
//		Description: "Finds and summarises sources for a question.",
//		Model:       model,
//		Tools:       []agenttool.Tool{search, fetch},
//	})
//	parent := agentturn.Config{Model: model, Tools: []agenttool.Tool{specialist}}
//
// The loop never learns a sub-agent concept: to the parent this is a
// tool, and to the child this is a run. Nesting is unbounded and each
// level is the same code. The child's events stream out through the
// call's progress callback and [WithObserver], and its run ID and items
// come back in Result.Details as a [ChildInfo], so a session subscriber
// on the parent can write a subsession link keyed by the call ID.
//
// Abort on the parent reaches the child through the context.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// Input is the default argument type: one string rendered as the first
// user message of the child.
type Input struct {
	Input string `json:"input" desc:"The task or question for the agent"`
}

// ChildInfo is the Result.Details of a child run.
type ChildInfo struct {
	// RunID is the child's run ID.
	RunID string
	// Items are the items the child run appended to its transcript,
	// prompts included.
	Items agentturn.Transcript
	// Reason says how the child run ended.
	Reason agentturn.Reason
	// Pending lists the calls the child deferred to its caller when
	// Reason is ReasonInputRequired. A host that wants to answer them
	// appends their outputs to Items and continues the child with
	// agentturn.Continue.
	Pending []*openresponses.FunctionCall
}

// InputRequiredError is returned when the child run stopped on calls
// its BeforeToolCall deferred. The parent's model sees it as the error
// output and can decide what to do; a host that wants to resume the
// child finds the run and the pending calls in the result's ChildInfo.
// It matches agentturn.ErrInputRequired under errors.Is.
type InputRequiredError struct {
	Agent   string
	RunID   string
	Pending []*openresponses.FunctionCall
}

// Error describes the pending calls, arguments abbreviated.
func (e *InputRequiredError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "agent %q needs input before it can finish: %d pending tool call(s):", e.Agent, len(e.Pending))
	for _, fc := range e.Pending {
		args := fc.Arguments
		if len(args) > 200 {
			args = args[:200] + "..."
		}
		fmt.Fprintf(&b, " %s(%s)", fc.Name, args)
	}
	return b.String()
}

// Is reports whether target is agentturn.ErrInputRequired.
func (e *InputRequiredError) Is(target error) bool { return target == agentturn.ErrInputRequired }

// Option configures the tool built by [New].
type Option func(*options)

type options struct {
	schema   json.RawMessage
	strict   bool
	render   func(json.RawMessage) (openresponses.Items, error)
	seed     func(parent agentturn.Transcript) agentturn.Transcript
	observer func(context.Context, agentturn.Event)
}

// WithArgs replaces the default {"input": string} arguments with T,
// whose schema is reflected as in agenttool.New. render turns the decoded
// value into the items that open the child's transcript, usually one
// user message. It panics when T has no schema, as agenttool.New does,
// which a test of the tool's construction catches.
func WithArgs[T any](render func(T) openresponses.Items) Option {
	return withArgs(false, render)
}

// WithStrictArgs is [WithArgs] with a strict schema. It panics when T
// has no strict schema.
func WithStrictArgs[T any](render func(T) openresponses.Items) Option {
	return withArgs(true, render)
}

func withArgs[T any](strict bool, render func(T) openresponses.Items) Option {
	var schemaOpts []agenttool.Option
	if strict {
		schemaOpts = append(schemaOpts, agenttool.WithStrict())
	}
	schema, err := agenttool.SchemaFor[T](schemaOpts...)
	if err != nil {
		panic(fmt.Sprintf("agent.WithArgs: %v", err))
	}
	return func(o *options) {
		o.schema = schema
		o.strict = strict
		o.render = func(raw json.RawMessage) (openresponses.Items, error) {
			v, err := agenttool.Decode[T](raw)
			if err != nil {
				return nil, err
			}
			return render(v), nil
		}
	}
}

// WithTranscript seeds the child's transcript from the parent's, which
// the loop attaches to every tool call's context (see
// agentturn.TranscriptFromContext); seed receives nil when the tool is
// executed outside a loop. The seed goes before the rendered
// arguments.
func WithTranscript(seed func(parent agentturn.Transcript) agentturn.Transcript) Option {
	return func(o *options) { o.seed = seed }
}

// WithObserver receives every event of the child run, in order, from the
// tool's goroutine. It is the seam for linking the child to a session.
func WithObserver(fn func(context.Context, agentturn.Event)) Option {
	return func(o *options) { o.observer = fn }
}

// New wraps cfg as a tool named cfg.Name with cfg.Description. Each
// call runs a child loop with cfg; the child's own hooks apply and the
// parent's do not. It panics when cfg.Name is empty, since a tool
// without a name cannot be offered to a model.
func New(cfg agentturn.Config, opts ...Option) agenttool.Tool {
	if cfg.Name == "" {
		panic("agent.New: config has no Name")
	}
	o := options{}
	for _, opt := range opts {
		opt(&o)
	}
	if o.render == nil {
		WithArgs(func(in Input) openresponses.Items {
			return openresponses.Items{openresponses.UserText(in.Input)}
		})(&o)
	}
	return &agentTool{cfg: cfg, opts: o}
}

type agentTool struct {
	cfg  agentturn.Config
	opts options
}

func (a *agentTool) Name() string                { return a.cfg.Name }
func (a *agentTool) Description() string         { return a.cfg.Description }
func (a *agentTool) Parameters() json.RawMessage { return a.opts.schema }
func (a *agentTool) Strict() bool                { return a.opts.strict }

// Execute runs the child. The output is the text of the child's last
// assistant message; Details is a [ChildInfo]. A child that fails
// returns its error, one that is aborted returns the context error, and
// one that stopped on deferred calls returns an [*InputRequiredError],
// because a pause inside the child cannot become a pause of the parent
// after the fact. On every error path the Result still carries the
// ChildInfo: the loop keeps Details when it turns an error into the
// output the model sees, so a session subscriber can link the child run
// whether or not it succeeded.
func (a *agentTool) Execute(ctx context.Context, call agenttool.Call) (agenttool.Result, error) {
	prompts, err := a.opts.render(call.Args)
	if err != nil {
		return agenttool.Result{}, err
	}
	if len(prompts) == 0 {
		return agenttool.Result{}, fmt.Errorf("agent %q: arguments rendered no items", a.cfg.Name)
	}
	var seed agentturn.Transcript
	if a.opts.seed != nil {
		parent, _ := agentturn.TranscriptFromContext(ctx)
		seed = a.opts.seed(parent)
	}

	var soFar []string
	var end *agentturn.RunEnd
	for ev := range agentturn.Run(ctx, seed, prompts, a.cfg) {
		if a.opts.observer != nil {
			a.opts.observer(ctx, ev)
		}
		switch e := ev.(type) {
		case *agentturn.ItemEnd:
			if m, ok := e.Item.(*openresponses.Message); ok && m.Role == openresponses.RoleAssistant {
				soFar = append(soFar, m.Text())
				call.Update(agenttool.Result{
					Output:  openresponses.FunctionCallOutputData{Text: strings.Join(soFar, "\n\n")},
					Details: ChildInfo{RunID: e.RunID},
				})
			}
		case *agentturn.RunEnd:
			end = e
		}
	}
	if end == nil {
		return agenttool.Result{}, errors.New("agent " + strconv.Quote(a.cfg.Name) + ": child run produced no run_end")
	}
	info := ChildInfo{RunID: end.RunID, Items: end.Items, Reason: end.Reason, Pending: end.Pending}
	switch end.Reason {
	case agentturn.ReasonInputRequired:
		return agenttool.Result{Details: info}, &InputRequiredError{Agent: a.cfg.Name, RunID: end.RunID, Pending: end.Pending}
	case agentturn.ReasonError:
		return agenttool.Result{Details: info}, fmt.Errorf("agent %q: %w", a.cfg.Name, end.Err)
	case agentturn.ReasonAborted:
		err := end.Err
		if err == nil {
			err = context.Canceled
		}
		return agenttool.Result{Details: info}, err
	}
	return agenttool.Result{
		Output:  openresponses.FunctionCallOutputData{Text: lastAssistantText(end.Items)},
		Details: info,
	}, nil
}

func lastAssistantText(items agentturn.Transcript) string {
	for i := len(items) - 1; i >= 0; i-- {
		if m, ok := items[i].(*openresponses.Message); ok && m.Role == openresponses.RoleAssistant {
			return m.Text()
		}
	}
	return ""
}

var (
	_ agenttool.Tool   = (*agentTool)(nil)
	_ agenttool.Strict = (*agentTool)(nil)
)
