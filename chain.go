package agentturn

import (
	"context"

	"github.com/ChristopherDavenport/openresponses"
)

// The Chain functions join several values for one hook into one. Every
// hook on [Config] is a plain field, so two layers that each want it,
// a memory that re-renders the instructions and a guard that inspects
// the request, silently leave one assignment standing when a product
// follows both their READMEs in turn. A chain says which layers a hook
// runs, in what order, in one place:
//
//	cfg.BeforeModelCall = agentturn.ChainBeforeModelCall(
//		memory.BeforeModelCall(),   // edits the request
//		guard.BeforeModelCall(),    // inspects what will be sent
//	)
//
// The order is the caller's and it matters: a hook that edits the
// request belongs before one that inspects it, so the guard sees what
// the model will. Every chain runs its functions in order, skips nil
// ones, and returns nil when given none, so a chain of nothing is the
// same as no hook. A chain of one behaves exactly as that one function.

// ChainBeforeModelCall runs each hook on the request in order, so each
// sees what the ones before it left. The first error stops the chain
// and is returned, which ends the run with a [ModelBlocked] event as a
// single hook's error does.
func ChainBeforeModelCall(fns ...func(context.Context, *openresponses.Request) error) func(context.Context, *openresponses.Request) error {
	var kept []func(context.Context, *openresponses.Request) error
	for _, fn := range fns {
		if fn != nil {
			kept = append(kept, fn)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return func(ctx context.Context, req *openresponses.Request) error {
		for _, fn := range kept {
			if err := fn(ctx, req); err != nil {
				return err
			}
		}
		return nil
	}
}

// ChainBeforeTurn runs each hook in order and appends what they return,
// in that order, so every layer contributes its items to the turn. The
// first error stops the chain and is returned; the items of the hooks
// that already ran are dropped with the turn.
func ChainBeforeTurn(fns ...func(context.Context, TurnStartInfo) (openresponses.Items, error)) func(context.Context, TurnStartInfo) (openresponses.Items, error) {
	var kept []func(context.Context, TurnStartInfo) (openresponses.Items, error)
	for _, fn := range fns {
		if fn != nil {
			kept = append(kept, fn)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return func(ctx context.Context, info TurnStartInfo) (openresponses.Items, error) {
		var items openresponses.Items
		for _, fn := range kept {
			got, err := fn(ctx, info)
			if err != nil {
				return nil, err
			}
			items = append(items, got...)
		}
		return items, nil
	}
}

// ChainShouldStopAfterTurn runs each hook in order until one stops the
// run: the first true ends the run, and the hooks after it do not run,
// since there is no turn left for them to judge. The first error stops
// the chain and is returned, so an error wrapping [ErrGuard] from any
// hook ends the run as a guard stop with that error on RunEnd.Err. A
// layer that must see every turn, a meter for one, belongs in a
// subscriber rather than here.
func ChainShouldStopAfterTurn(fns ...func(context.Context, TurnInfo) (bool, error)) func(context.Context, TurnInfo) (bool, error) {
	var kept []func(context.Context, TurnInfo) (bool, error)
	for _, fn := range fns {
		if fn != nil {
			kept = append(kept, fn)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return func(ctx context.Context, info TurnInfo) (bool, error) {
		for _, fn := range kept {
			stop, err := fn(ctx, info)
			if err != nil {
				return false, err
			}
			if stop {
				return true, nil
			}
		}
		return false, nil
	}
}

// ChainBeforeToolCall folds the decisions of each hook, in order, into
// one: the strictest action wins, Block over Defer over Allow, which is
// deny over ask over allow as a permission policy reads it. A Block
// ends the chain, since nothing after it can make the call run; a Defer
// does not, so a later layer still sees the call and can refuse it.
// Among decisions of one action the first reason and decider stand;
// a decision that takes the fold to a stricter action brings its own,
// because the reason has to explain the decision that stands and a
// block's reason is what the model reads. The first note stands, and
// Terminate is set when any decision sets it. Arguments a decision rewrites are passed to the
// hooks after it, so each sees what the call will actually run with and
// the last rewrite is what the tool receives. The first error stops the
// chain and fails the turn.
//
// A nil result from every hook is a nil decision, which allows the call
// as a single hook's nil does.
func ChainBeforeToolCall(fns ...func(context.Context, ToolCallInfo) (*ToolDecision, error)) func(context.Context, ToolCallInfo) (*ToolDecision, error) {
	var kept []func(context.Context, ToolCallInfo) (*ToolDecision, error)
	for _, fn := range fns {
		if fn != nil {
			kept = append(kept, fn)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return func(ctx context.Context, info ToolCallInfo) (*ToolDecision, error) {
		var out *ToolDecision
		for _, fn := range kept {
			d, err := fn(ctx, info)
			if err != nil {
				return nil, err
			}
			if d == nil {
				continue
			}
			if out == nil {
				c := *d
				out = &c
			} else {
				out.merge(d)
			}
			if out.Args != nil {
				info.Args = out.Args
			}
			if out.Action == Block {
				return out, nil
			}
		}
		return out, nil
	}
}

// strictness ranks the actions as a permission policy reads them: deny
// over ask over allow. The constants are in another order, since Allow
// has to be the zero value.
func strictness(a ToolAction) int {
	switch a {
	case Block:
		return 2
	case Defer:
		return 1
	}
	return 0
}

// merge folds next into d under the chain's rules.
func (d *ToolDecision) merge(next *ToolDecision) {
	if strictness(next.Action) > strictness(d.Action) {
		// The stricter decision stands, and the reason that explains it
		// stands with it: a block's reason is what the model reads as
		// the error output.
		d.Action, d.Reason, d.By = next.Action, next.Reason, next.By
	}
	if d.Reason == "" {
		d.Reason = next.Reason
	}
	if d.By == "" {
		d.By = next.By
	}
	if d.Note == "" {
		d.Note = next.Note
	}
	if next.Args != nil {
		d.Args = next.Args
	}
	d.Terminate = d.Terminate || next.Terminate
}

// ChainOutputGuard runs each guard on the message in order, each seeing
// what the ones before it left: a guard that returns a replacement
// hands that replacement to the next. The last replacement is what
// reaches the transcript, and a chain in which no guard replaced
// anything keeps the model's own message. The first error stops the
// chain and fails the turn.
func ChainOutputGuard(fns ...func(context.Context, OutputInfo) (*openresponses.Message, error)) func(context.Context, OutputInfo) (*openresponses.Message, error) {
	var kept []func(context.Context, OutputInfo) (*openresponses.Message, error)
	for _, fn := range fns {
		if fn != nil {
			kept = append(kept, fn)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return func(ctx context.Context, info OutputInfo) (*openresponses.Message, error) {
		var replaced *openresponses.Message
		for _, fn := range kept {
			out, err := fn(ctx, info)
			if err != nil {
				return nil, err
			}
			if out != nil {
				replaced = out
				info.Message = out
			}
		}
		return replaced, nil
	}
}
