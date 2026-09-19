// Package agent wraps an agent configuration as a tool, so one loop can
// delegate to another in process. It is the sub-agent pattern: the
// parent's model calls a tool named after the child, the child runs its
// own loop on a fresh transcript with its own hooks and tools, and the
// child's final answer is the tool output.
//
//	specialist := agent.Tool(agentturn.Config{
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
	"fmt"
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

// Option configures the tool built by [Tool].
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
// user message.
func WithArgs[T any](render func(T) openresponses.Items) Option {
	schema, err := agenttool.SchemaFor[T](false)
	if err != nil {
		panic(fmt.Sprintf("agent.WithArgs: %v", err))
	}
	return func(o *options) {
		o.schema = schema
		o.render = func(raw json.RawMessage) (openresponses.Items, error) {
			v, err := agenttool.Decode[T](raw)
			if err != nil {
				return nil, err
			}
			return render(v), nil
		}
	}
}

// WithStrictArgs is [WithArgs] with a strict schema.
func WithStrictArgs[T any](render func(T) openresponses.Items) Option {
	schema, err := agenttool.SchemaFor[T](true)
	if err != nil {
		panic(fmt.Sprintf("agent.WithStrictArgs: %v", err))
	}
	return func(o *options) {
		WithArgs(render)(o)
		o.schema = schema
		o.strict = true
	}
}

// WithTranscript seeds the child's transcript from the parent's. The
// parent transcript is whatever the host attached to the context with
// [WithParentTranscript]; nil when nothing was attached. The seed goes
// before the rendered arguments.
func WithTranscript(seed func(parent agentturn.Transcript) agentturn.Transcript) Option {
	return func(o *options) { o.seed = seed }
}

// WithObserver receives every event of the child run, in order, from the
// tool's goroutine. It is the seam for linking the child to a session.
func WithObserver(fn func(context.Context, agentturn.Event)) Option {
	return func(o *options) { o.observer = fn }
}

type parentKey struct{}

// WithParentTranscript attaches the parent's transcript to ctx so a
// child built with [WithTranscript] can be seeded from it. A host that
// runs the loop attaches it to the context it passes to Run.
func WithParentTranscript(ctx context.Context, t agentturn.Transcript) context.Context {
	return context.WithValue(ctx, parentKey{}, t)
}

// ParentTranscript returns the transcript attached with
// [WithParentTranscript].
func ParentTranscript(ctx context.Context) (agentturn.Transcript, bool) {
	t, ok := ctx.Value(parentKey{}).(agentturn.Transcript)
	return t, ok
}

// Tool wraps cfg as a tool named cfg.Name with cfg.Description. Name is
// required. Each call runs a child loop with cfg; the child's own hooks
// apply and the parent's do not.
func Tool(cfg agentturn.Config, opts ...Option) agenttool.Tool {
	if cfg.Name == "" {
		panic("agent.Tool: config has no Name")
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
// after the fact.
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
		parent, _ := ParentTranscript(ctx)
		seed = a.opts.seed(parent)
	}

	var soFar []string
	var end *agentturn.RunEnd
	for ev, err := range agentturn.Run(ctx, seed, prompts, a.cfg) {
		if ev == nil && err != nil {
			return agenttool.Result{}, fmt.Errorf("agent %q: %w", a.cfg.Name, err)
		}
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
		return agenttool.Result{}, fmt.Errorf("agent %q: child run produced no run_end", a.cfg.Name)
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
