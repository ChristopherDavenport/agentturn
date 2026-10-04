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
// call's progress callback and [WithObserver], which is how
// agentturn/session records the child as a session of its own, and its
// run ID and items come back in Result.Details as a [ChildInfo], so a
// session subscriber on the parent can write a subsession link keyed by
// the call ID.
//
// Abort on the parent reaches the child through the context.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

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
	// Agent is the child's name, its Config.Name.
	Agent string
	// RunID is the child's run ID.
	RunID string
	// Items are the items the child run appended to its transcript,
	// prompts included.
	Items agentturn.Transcript
	// Reason says how the child run ended, and Cause what stopped it
	// when Reason is ReasonStopped.
	Reason agentturn.Reason
	Cause  agentturn.StopCause
	// Pending lists the calls the child deferred to its caller when
	// Reason is ReasonInputRequired, and the calls an abort cut off. A
	// host that wants to answer them appends their outputs to Items and
	// continues the child with agentturn.Continue.
	Pending []agentturn.PendingCall
}

// InputRequiredError is returned when the child run stopped on calls
// its BeforeToolCall deferred. The parent's model sees it as the error
// output and can decide what to do; a host that wants to resume the
// child finds the run and the pending calls in the result's ChildInfo.
// It matches agentturn.ErrInputRequired under errors.Is.
type InputRequiredError struct {
	Agent   string
	RunID   string
	Pending []agentturn.PendingCall
}

// Error describes the pending calls, arguments abbreviated.
func (e *InputRequiredError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "agent %q needs input before it can finish: %d pending tool call(s):", e.Agent, len(e.Pending))
	for _, p := range e.Pending {
		fc := p.Call
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
	name     string
	render   func(json.RawMessage) (openresponses.Items, error)
	call     func(ctx context.Context, args json.RawMessage, parent agentturn.Transcript, cfg agentturn.Config) (agentturn.Config, openresponses.Items, error)
	seed     func(parent agentturn.Transcript) agentturn.Transcript
	observer func(context.Context, agentturn.Event)
	noAnswer func(ChildInfo) (agenttool.Result, error)
	spawn    func(callID string, child *agentturn.Agent)
	runCtx   func(ctx context.Context, callID string) context.Context
	detach   func(callID string, end *agentturn.RunEnd)
}

// toolName is the form a provider accepts for a function tool's name.
var toolName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// WithToolName offers the child to the model under name rather than
// under Config.Name, for an agent whose display name is not a valid
// tool name. It panics on a name a provider rejects, as [New] does.
func WithToolName(name string) Option {
	if !toolName.MatchString(name) {
		panic(fmt.Sprintf("agent.WithToolName: %q is not a tool name: want %s", name, toolName))
	}
	return func(o *options) { o.name = name }
}

// WithNoAnswer sets what the parent's model sees when the child ends
// without an answer (agentturn.RunEnd.Answer): a run that hit its turn
// budget or a stop hook (agentturn.StopHook) still calling tools, text
// before a call being no answer, or one that finished with an empty
// message. The default returns an error naming the agent and the
// cause, so the model knows the child did not answer rather than
// reading an empty output as one; a child stopped by a terminating
// tool result is the exception, whose last output stands as the
// answer, since the tool answered on the model's behalf. A child a
// guard stopped (agentturn.StopGuard) without an answer is not asked
// about here, whichever hook the guard is on, ShouldStopAfterTurn
// included, nor one whose OutputGuard withheld a message after a
// message of the same response it let through
// (agentturn.RunEnd.Withheld): that is a refusal, and the call fails
// with the guard's error, which wraps agentturn.ErrGuard.
func WithNoAnswer(fn func(ChildInfo) (agenttool.Result, error)) Option {
	return func(o *options) { o.noAnswer = fn }
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
		o.call = nil // the later of WithArgs and WithCallConfig decides
		o.render = func(raw json.RawMessage) (openresponses.Items, error) {
			v, err := agenttool.Decode[T](raw)
			if err != nil {
				return nil, err
			}
			return render(v), nil
		}
	}
}

// WithCallConfig decides each call's configuration and opening items
// from its arguments, for a tool whose calls differ in more than their
// first message: a model the caller picks, or a child that starts from
// the conversation so far. fn receives the decoded arguments, the
// parent's transcript as [WithTranscript]'s seed does (nil outside a
// loop, its in-flight calls answered with placeholders) and the
// configuration New was given, and returns the configuration this call
// runs under and the items its run opens with. T's schema is reflected
// as in agenttool.New. It and [WithArgs] replace each other: the later
// of the two decides both the schema and the items.
//
// The returned configuration is the child's for this call everywhere:
// the run, the observer's [ConfigFromContext], so a recorder writes the
// configuration that ran, and the errors. Its Name must be the one New
// was given, which the tool is offered and the child is recorded under;
// a configuration that renames it fails the call. fn's error fails the
// call before any run starts. Items fn takes from the parent's
// transcript go in as the run's prompt, not as a seed, so a recorder
// writes them as the child's own and can stand behind the request that
// carries them; [WithTranscript]'s seed, which a recorder did not
// write, it cannot. A seed set as well still goes before the items.
func WithCallConfig[T any](fn func(ctx context.Context, args T, parent agentturn.Transcript, cfg agentturn.Config) (agentturn.Config, openresponses.Items, error)) Option {
	schema, err := agenttool.SchemaFor[T]()
	if err != nil {
		panic(fmt.Sprintf("agent.WithCallConfig: %v", err))
	}
	return func(o *options) {
		o.schema, o.strict = schema, false
		o.render = nil // the later of WithArgs and WithCallConfig decides
		o.call = func(ctx context.Context, raw json.RawMessage, parent agentturn.Transcript, cfg agentturn.Config) (agentturn.Config, openresponses.Items, error) {
			v, err := agenttool.Decode[T](raw)
			if err != nil {
				return cfg, nil, err
			}
			return fn(ctx, v, parent, cfg)
		}
	}
}

// WithTranscript seeds the child's transcript from the parent's, which
// the loop attaches to every tool call's context (see
// agentturn.TranscriptFromContext); seed receives nil when the tool is
// executed outside a loop. The seed goes before the rendered
// arguments.
//
// The snapshot the loop attaches holds the batch's calls, this one
// among them, with no outputs yet, which is not a valid input. seed
// receives a copy in which every such call is answered with a
// placeholder output saying the call is in progress, this one naming
// the agent, so a seed that keeps the conversation as it is hands the
// child a transcript a strict server accepts; the parent's snapshot is
// not changed.
func WithTranscript(seed func(parent agentturn.Transcript) agentturn.Transcript) Option {
	return func(o *options) { o.seed = seed }
}

// WithObserver receives every event of the child run, in order, as a
// subscriber of the child's agent, so every event is a barrier: the
// child does not move to its next phase until the observer has
// returned, which is what makes a dispatch durable before its tool
// runs. It is the seam for recording the child as a session of its
// own: agentturn/session's Recorder.Observe is made for it. The
// context it receives is the call's, so agentturn.RunIDFromContext names
// the parent run and agentturn.TranscriptFromContext holds the parent's
// transcript, with the child's configuration added for
// [ConfigFromContext] and the call itself for agenttool.CallFrom, so a
// recorder can derive the child's session ID from the parent's and the
// call's; its cancellation is lifted, as an Agent lifts it for
// subscribers, so an abort that cuts the child does not also cut what
// the observer writes about it.
func WithObserver(fn func(context.Context, agentturn.Event)) Option {
	return func(o *options) { o.observer = fn }
}

// WithSpawn is called with the child's agent, keyed by the call, after
// the observer is subscribed and before the run starts, so a host can
// steer it, abort it on its own without cutting its siblings, and
// prompt it again once the tool has returned: what a hub that messages
// a running subagent, cancels one job and revives a parked one needs,
// and what a child running inside [agentturn.Run] could not give.
// Nothing an observer sees changes, and the agent is the tool's: a
// host that prompts it again gets a second run on the child's own
// transcript.
//
// The observer stays subscribed after Execute returns, so a later run
// the host starts is recorded into the same child session, continuing
// it at its leaf, since the agent holds the context the leaf
// rebuilds; only a run Execute starts is marked with
// [ContextWithRetry], since a fresh agent holds none. The call's
// progress updates stop, since the call is over.
func WithSpawn(fn func(callID string, child *agentturn.Agent)) Option {
	return func(o *options) { o.spawn = fn }
}

// WithRunContext gives the child run a context of the host's making,
// derived from the call's: what only the host knows and the child
// should run under. A session recorder's ChildContext is the case it
// was added for, putting the child's session ID on the context so a
// layer that attributes its writes to a session, a memory journal for
// one, names the child's rather than the parent's. It runs once per
// call, before the run starts, and the context it returns is the one
// the child's hooks and tools see. The child's run carries no trigger
// of the parent's run, whose call started it; one this puts on the
// context with agentturn.ContextWithTrigger is the child's own.
func WithRunContext(fn func(ctx context.Context, callID string) context.Context) Option {
	return func(o *options) { o.runCtx = fn }
}

// WithDetach makes a call return as soon as the child's run has
// started, with an output saying the task is running and a [ChildInfo]
// naming the run, and hands the run's end to fn when it comes, on the
// child's goroutine. It is the background task: the parent's model
// goes on while the child works, and the host delivers the answer to
// the parent from fn with agentturn.Agent.Deliver, which joins the
// parent's run when it will still take the answer and otherwise starts
// one for it. State().Running is not the test: it reads true while the
// parent's run is ending and takes no more steers, and an answer
// steered then waits for the user's next prompt. fn runs on the
// child's goroutine, which is not inside a parent's event, so it may
// wait on the parent's run to end; a host that must not block the
// child calls Deliver from a goroutine of its own.
//
// The child runs on agentturn.RunContext of the call, so an abort of
// the parent's run still cuts it, and the parent's batch ending or its
// run ending by itself does not. The observer stays subscribed, as
// after any call, and WithSpawn still hands out the child's agent, so
// a host can steer it or abort it alone. fn is called once, for the
// run the call started, whatever its reason; a run that could not
// start is the call's error and fn is not called. The child's context
// is the run context with the call on it, so the parent's transcript,
// its invoker, its tool elicitor and its steer signal, which belong to
// the batch, do not reach it. A detached child is recorded only
// through [WithObserver]: the call's tool_end comes while it runs, with
// no items to copy.
func WithDetach(fn func(callID string, end *agentturn.RunEnd)) Option {
	return func(o *options) { o.detach = fn }
}

type configKey struct{}
type retryKey struct{}

// ConfigFromContext returns the configuration of the child run whose
// events an observer registered with [WithObserver] is receiving. It
// reports false on any other context.
func ConfigFromContext(ctx context.Context) (agentturn.Config, bool) {
	cfg, ok := ctx.Value(configKey{}).(agentturn.Config)
	return cfg, ok
}

// ContextWithConfig attaches a child's configuration to ctx, as this
// package does for the observer of a child it runs. A host that runs a
// child agent itself and observes it with the same function puts the
// configuration on the context it prompts with, so the observer can
// write it before the child's first item.
func ContextWithConfig(ctx context.Context, cfg agentturn.Config) context.Context {
	return context.WithValue(ctx, configKey{}, cfg)
}

// ContextWithRetry marks the runs under ctx as starting afresh under
// the call they belong to rather than continuing what an earlier run
// under it left. An observer that keeps a session per call,
// agentturn/session's does, opens a new root in the call's session for
// a run so marked, and continues the session at its leaf for one that
// is not, because a subagent that is messaged again answers from its
// own context. Execute puts the mark on every run it starts: it builds
// a fresh agent per call whose request is the new input alone, so a
// second Execute under one call ID, Agent.Resume running a cut-off
// call again for one, never holds the path it would continue, and its
// run belongs on a new root, where its responses rebuild and the
// earlier attempt's open call is not at the leaf. For a first call the
// session does not exist yet and the mark changes nothing. A host that
// keeps the child through [WithSpawn] and prompts it again does so
// with a context of its own, unmarked, and that run continues at the
// leaf, which is right because that agent does hold its context.
func ContextWithRetry(ctx context.Context) context.Context {
	return context.WithValue(ctx, retryKey{}, true)
}

// RetryFromContext reports whether the host marked the run a retry with
// [ContextWithRetry].
func RetryFromContext(ctx context.Context) bool {
	retry, _ := ctx.Value(retryKey{}).(bool)
	return retry
}

// New wraps cfg as a tool named cfg.Name with cfg.Description. Each
// call runs a child loop with cfg; the child's own hooks apply and the
// parent's do not. It panics when cfg.Name is empty, since a tool
// without a name cannot be offered to a model, and when it is not a
// name a provider accepts (letters, digits, underscores and hyphens,
// at most 64) unless [WithToolName] gives one that is.
func New(cfg agentturn.Config, opts ...Option) agenttool.Tool {
	if cfg.Name == "" {
		panic("agent.New: config has no Name")
	}
	o := options{}
	for _, opt := range opts {
		opt(&o)
	}
	if o.name == "" {
		if !toolName.MatchString(cfg.Name) {
			panic(fmt.Sprintf("agent.New: agent %q: name is not a tool name: want %s; use WithToolName", cfg.Name, toolName))
		}
		o.name = cfg.Name
	}
	if o.noAnswer == nil {
		o.noAnswer = defaultNoAnswer
	}
	if o.render == nil && o.call == nil {
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

func (a *agentTool) Name() string                { return a.opts.name }
func (a *agentTool) Description() string         { return a.cfg.Description }
func (a *agentTool) Parameters() json.RawMessage { return a.opts.schema }
func (a *agentTool) Strict() bool                { return a.opts.strict }

// Execute runs the child as an [agentturn.Agent], so every event is a
// barrier and a host given the agent by [WithSpawn] can reach it while
// it runs. The output is the text of the child's last assistant
// message; Details is a [ChildInfo]. A child that fails returns its
// error, one that is aborted returns the context error, and one that
// stopped on deferred calls returns an [*InputRequiredError], because a
// pause inside the child cannot become a pause of the parent after the
// fact. A child that ends without a final assistant message returns
// what [WithNoAnswer] says, by default an error naming the cause. On
// every error path the Result still carries the ChildInfo: the loop
// keeps Details when it turns an error into the output the model sees,
// so a session subscriber can link the child run whether or not it
// succeeded.
func (a *agentTool) Execute(ctx context.Context, call agenttool.Call) (agenttool.Result, error) {
	var parent agentturn.Transcript
	if a.opts.call != nil || a.opts.seed != nil {
		p, _ := agentturn.TranscriptFromContext(ctx)
		parent = answered(p, call.ID, a.cfg.Name)
	}
	cfg := a.cfg
	var prompts openresponses.Items
	var err error
	if a.opts.call != nil {
		cfg, prompts, err = a.opts.call(ctx, call.Args, parent, a.cfg)
		if err != nil {
			return agenttool.Result{}, err
		}
		if cfg.Name != a.cfg.Name {
			return agenttool.Result{}, fmt.Errorf("agent %q: the call's configuration is named %q", a.cfg.Name, cfg.Name)
		}
	} else if prompts, err = a.opts.render(call.Args); err != nil {
		return agenttool.Result{}, err
	}
	if len(prompts) == 0 {
		return agenttool.Result{}, fmt.Errorf("agent %q: arguments rendered no items", a.cfg.Name)
	}
	var seed agentturn.Transcript
	if a.opts.seed != nil {
		seed = a.opts.seed(parent)
	}

	// The observer sees the values of the call's context but never its
	// cancellation, as an Agent's subscribers do: an abort of the parent
	// cuts the child through ctx, and the events the cut leaves behind
	// still have to be written.
	obsCtx := agenttool.WithCall(context.WithValue(context.WithoutCancel(ctx), configKey{}, cfg), call)
	var opts []agentturn.Option
	if seed != nil {
		opts = append(opts, agentturn.WithTranscript(seed))
	}
	child := agentturn.New(cfg, opts...)
	var mu sync.Mutex
	var soFar []string
	done := false
	started := make(chan string, 1)
	child.Subscribe(func(evCtx context.Context, ev agentturn.Event) error {
		if a.opts.observer != nil {
			octx := obsCtx
			if RetryFromContext(evCtx) {
				// Execute marks the run's context, and so may a host
				// that prompts the child again; the observer's is the
				// call's, so the mark is carried to it.
				octx = ContextWithRetry(octx)
			}
			a.opts.observer(octx, ev)
		}
		if e, ok := ev.(*agentturn.RunStart); ok {
			select {
			case started <- e.RunID:
			default:
			}
		}
		e, ok := ev.(*agentturn.ItemEnd)
		if !ok {
			return nil
		}
		m, ok := e.Item.(*openresponses.Message)
		if !ok || m.Role != openresponses.RoleAssistant {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		if done {
			// The call is over; a later run is the host's and has
			// nowhere to report progress to.
			return nil
		}
		soFar = append(soFar, m.Text())
		call.Update(agenttool.Result{
			Output:  openresponses.FunctionCallOutputData{Text: strings.Join(soFar, "\n\n")},
			Details: ChildInfo{Agent: a.cfg.Name, RunID: e.RunID},
		})
		return nil
	})
	if a.opts.spawn != nil {
		a.opts.spawn(call.ID, child)
	}
	if a.opts.detach != nil {
		// The child outlives the call: it runs on the run's context,
		// with the call on it for the child's own tools and hooks.
		if rc, ok := agentturn.RunContext(ctx); ok {
			ctx = agenttool.WithCall(rc, call)
		}
	}
	// The parent's trigger names what started the parent's run; the
	// child's run was started by the call, which the child's link
	// names, so it carries none unless WithRunContext gives it one.
	// Who answered the parent's pending calls, why, and where their
	// outputs were taken from are the parent's too: a child's call
	// under the same ID is another call.
	ctx = agentturn.ContextWithTrigger(ctx, agentturn.Trigger{})
	ctx = agentturn.ContextWithOrigins(agentturn.ContextWithReasons(agentturn.ContextWithDeciders(ctx, nil), nil), nil)
	// The child is new and holds nothing of an earlier run under this
	// call, so its run starts afresh in the call's session: a second
	// Execute under one call ID, a cut-off call run again, opens a new
	// root rather than continue a path this agent never read. The
	// subscriber above carries the mark to the observer's context.
	ctx = ContextWithRetry(ctx)
	if a.opts.runCtx != nil {
		ctx = a.opts.runCtx(ctx, call.ID)
	}
	if a.opts.detach != nil {
		return a.detached(ctx, call, child, prompts, started, func() {
			mu.Lock()
			done = true
			mu.Unlock()
		})
	}
	end, err := child.Prompt(ctx, prompts...)
	mu.Lock()
	done = true
	mu.Unlock()
	if end == nil {
		if err == nil {
			err = errors.New("child run produced no run_end")
		}
		return agenttool.Result{}, fmt.Errorf("agent %s: %w", strconv.Quote(a.cfg.Name), err)
	}
	info := ChildInfo{Agent: a.cfg.Name, RunID: end.RunID, Items: end.Items, Reason: end.Reason, Cause: end.Cause, Pending: end.Pending}
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
	text, ok := end.Answer()
	if !ok && end.Reason == agentturn.ReasonStopped && end.Cause == agentturn.StopGuard {
		// A guard stopped the child before it answered, at whichever
		// hook. The parent gets the guard's error, which wraps
		// ErrGuard, as it gets a failed run's.
		return agenttool.Result{Details: info}, fmt.Errorf("agent %q: %w", a.cfg.Name, end.Err)
	}
	if !ok {
		res, err := a.opts.noAnswer(info)
		res.Details = info
		return res, err
	}
	return agenttool.Result{
		Output:  openresponses.FunctionCallOutputData{Text: text},
		Details: info,
	}, nil
}

// detached starts the child's run on its own goroutine and returns once
// the run has started, or with the error that kept it from starting.
// The run's end goes to the WithDetach function. finish stops the
// call's progress updates, which have nowhere to go once it returns.
func (a *agentTool) detached(ctx context.Context, call agenttool.Call, child *agentturn.Agent, prompts openresponses.Items, started <-chan string, finish func()) (agenttool.Result, error) {
	type outcome struct {
		end *agentturn.RunEnd
		err error
	}
	ended := make(chan outcome, 1)
	go func() {
		end, err := child.Prompt(ctx, prompts...)
		ended <- outcome{end, err}
		if end != nil && end.RunID != "" {
			a.opts.detach(call.ID, end)
		}
	}()
	accepted := func(runID string) (agenttool.Result, error) {
		finish()
		return agenttool.Result{
			Output:  openresponses.FunctionCallOutputData{Text: fmt.Sprintf("agent %s is working on it in the background as run %s; its answer will follow", strconv.Quote(a.cfg.Name), runID)},
			Details: ChildInfo{Agent: a.cfg.Name, RunID: runID},
		}, nil
	}
	select {
	case runID := <-started:
		return accepted(runID)
	case o := <-ended:
		// A run that started and ended before this goroutine looked is
		// still a run that started: its end has gone to fn.
		select {
		case runID := <-started:
			return accepted(runID)
		default:
		}
		finish()
		err := o.err
		if err == nil {
			err = errors.New("child run produced no run_start")
		}
		return agenttool.Result{}, fmt.Errorf("agent %s: %w", strconv.Quote(a.cfg.Name), err)
	}
}

// answered returns a copy of parent in which every function call
// without an output is answered with a placeholder, the call callID
// naming agent as the one answering it, so a seed that keeps the
// conversation hands the child a valid input. nil stays nil.
func answered(parent agentturn.Transcript, callID, agent string) agentturn.Transcript {
	if parent == nil {
		return nil
	}
	done := map[string]bool{}
	for _, item := range parent {
		if out, ok := item.(*openresponses.FunctionCallOutput); ok {
			done[out.CallID] = true
		}
	}
	out := append(agentturn.Transcript(nil), parent...)
	for _, item := range parent {
		fc, ok := item.(*openresponses.FunctionCall)
		if !ok || done[fc.CallID] {
			continue
		}
		text := "(in progress: this call is running alongside the one being answered)"
		if fc.CallID == callID {
			text = "(in progress: the agent " + strconv.Quote(agent) + " is answering this call)"
		}
		out = append(out, openresponses.NewFunctionCallOutput(fc.CallID, text))
	}
	return out
}

// defaultNoAnswer is the [WithNoAnswer] used when none is set.
func defaultNoAnswer(info ChildInfo) (agenttool.Result, error) {
	if info.Reason == agentturn.ReasonStopped && (info.Cause == agentturn.StopTerminate || info.Cause == agentturn.StopPartialTerminate) {
		if text, ok := lastOutputText(info.Items); ok {
			return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: text}}, nil
		}
	}
	name := "the agent"
	if info.Agent != "" {
		name = fmt.Sprintf("agent %q", info.Agent)
	}
	if info.Cause != "" {
		return agenttool.Result{}, fmt.Errorf("%s stopped (%s) without a final answer", name, info.Cause)
	}
	return agenttool.Result{}, errors.New(name + " finished without a final answer")
}

// lastOutputText returns the text of the last function call output.
func lastOutputText(items agentturn.Transcript) (string, bool) {
	for i := len(items) - 1; i >= 0; i-- {
		if out, ok := items[i].(*openresponses.FunctionCallOutput); ok {
			return out.Output.Text, true
		}
	}
	return "", false
}

var (
	_ agenttool.Tool   = (*agentTool)(nil)
	_ agenttool.Strict = (*agentTool)(nil)
)
