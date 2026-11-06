// Package handoff is the in-process handoff: a transfer tool per
// handoff an agent offers, an input filter per handoff, and the loop
// that switches one agentturn.Agent from sender to receiver until a
// run ends without a transfer. It is the host side of RFC 0001's
// Composition, "as itself, under new settings": the loop learns no
// handoff concept, and every product with more than one agent would
// otherwise write this loop again.
//
//	team, err := handoff.New(
//		handoff.Agent{Config: triage, Handoffs: []handoff.To{{Agent: "Refund agent", Filter: handoff.RemoveAllTools}}},
//		handoff.Agent{Config: refunds},
//	)
//	a := agentturn.New(team.Config("Triage agent"))
//	end, path, err := team.Run(ctx, a, openresponses.UserText("I was double charged"))
//
// A host whose tools pass through a filter or a wrapper, a policy's,
// sets [Team.Offer], so the transfers pass through it too.
//
// [Team.Run] carries the rules a handoff must meet: the receiver
// continues under a trigger of kind "handoff" whose ref names the
// sender; the handoffs one input causes are bounded
// ([Team.MaxHandoffs]); a filter trims once, between runs, and leaves a
// transcript the loop can continue; and a receiver with no question or
// record channel of its own inherits the sender's. [Team.Resume] does
// the same after a run that paused on a deferred call is answered.
//
// Served through a front, the team is given in three parts:
//
//	responses.New(team.Config("Triage agent"),
//		responses.WithTransfers(team.Route()), // the next request starts where the last transfer left it
//		responses.WithHandoff(team.Handoff),   // a transfer within a request is relayed to the receiver
//		responses.WithHandoffTrim(team.Trim))  // with the handoff's filter applied
//
// and front/a2a takes the same options. Route alone starts the next
// request or task under the receiver, by the rules
// front/responses.Handoffs states for which call handed off, but
// neither relays a transfer within one nor trims. A front's trim holds
// for the receiver's later requests and tasks too: the conversation a
// front keeps, or a caller sends back, holds the history whole, since
// the next request finds its agent by the transfer in it, and the
// front trims it at each handoff again
// (front/responses.TrimHandedOff) before the receiver sees it. The fronts bound the handoffs one request or
// task follows themselves (WithMaxHandoffs).
package handoff

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/front/responses"
	"github.com/ChristopherDavenport/openresponses"
)

// ErrTooManyHandoffs is returned by [Team.Run] and [Team.Resume] when a
// run ends on a transfer after the team has followed
// [Team.MaxHandoffs] of them for one input: two agents that hand the
// conversation back and forth stop there. The last transfer is taken,
// unrun: the agent is switched to its receiver, with the handoff's
// trim, as a front's WithTransfers starts the next input with the
// receiver the transcript names, so the next prompt goes to the same
// agent either way. A delivery waiting on the agent is the exception:
// the receiver takes it in one run, whose own transfer, if it ends on
// one, is not followed. A run that ends on a terminating result no
// handoff follows, with a delivery waiting, is continued so the agent
// takes it. It is front/responses' ErrTooManyHandoffs, which the
// fronts return at their own bound.
var ErrTooManyHandoffs = responses.ErrTooManyHandoffs

// To is one handoff an agent offers.
type To struct {
	// Agent is the receiver's Config.Name.
	Agent string
	// Filter trims the history the receiver sees, once, between the
	// sender's run and the receiver's: [RemoveAllTools], say. nil keeps
	// the history whole.
	Filter func(agentturn.Transcript) agentturn.Transcript
	// Enabled says whether the handoff is offered on a request, from
	// the run's context; nil means always.
	Enabled func(context.Context) bool
}

// Agent is one member of a team: its configuration and the handoffs it
// offers.
type Agent struct {
	Config   agentturn.Config
	Handoffs []To
}

// Team holds the agents with their transfer tools added. Build one
// with [New].
type Team struct {
	// MaxHandoffs bounds the handoffs one [Team.Run] follows; 0 means
	// [responses.DefaultMaxHandoffs], the fronts' default too.
	MaxHandoffs int
	// Offer, when set, is how the host offers the team's tools: it is
	// applied on every request to each agent's whole list, its own
	// tools of the moment and the transfers it offers, and returns the
	// list the request carries, so a filter that removes a tool from
	// the offer, a policy's, and a wrapper around the agent's tools
	// apply to the transfers too (RFC 0001 Composition, as itself, under
	// new settings; #307). agent is the agent's Config.Name.
	//
	// A wrapper keeps a transfer's name and its result as they are:
	// the team knows a transfer by its name ([Team.Route]) and a
	// handoff by the transfer's own output text after the call
	// (front/responses.Handoffs). A wrapper that renames the tool, or
	// changes the text, as an audit suffix would, makes the call run
	// and not hand off, as a call a policy blocked or a tool that failed
	// does not, and nothing tells the two apart afterwards. A wrapper
	// that refuses the call is the way to stop a transfer.
	//
	// A tool of the moment that takes a transfer's name, a remote MCP
	// server's, which [New] cannot see in the static Config.Tools, fails
	// the turn by the loop's own check that a request's tools have
	// distinct names; Offer need not check it.
	//
	// What Offer removes is gone from the request and from the tools a
	// call resolves against, but Config.Tools of what [Team.Config]
	// returns still lists every transfer: read the tools of a request
	// through Config.ResolveTools.
	//
	// Set it before the team's configurations are used: [Team.Config],
	// [Team.Route] and the switch read it when they hand one out.
	Offer func(ctx context.Context, agent string, tools []agenttool.Tool) []agenttool.Tool

	configs map[string]agentturn.Config
	// tools maps a transfer tool's name to its receiver's name.
	tools map[string]string
	// filters holds each handoff's filter, by sender and receiver.
	filters map[[2]string]func(agentturn.Transcript) agentturn.Transcript
}

// Dest is a transfer result's Details: where the conversation goes,
// out of the model's sight.
type Dest struct {
	To string
}

// ToolName is the transfer tool's name for an agent, as the Agents SDK
// derives it: "transfer_to_" and the name lowered, each character that
// is not a letter or a digit an underscore, so "Billing agent" is
// offered as transfer_to_billing_agent.
func ToolName(agent string) string {
	var b strings.Builder
	b.WriteString("transfer_to_")
	for _, r := range agent {
		if r < unicode.MaxASCII && (unicode.IsLetter(r) || unicode.IsDigit(r)) {
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

// output is the text a transfer tool answers its call with, the Agents
// SDK's: which assistant has the conversation now.
func output(agent string) string {
	b, _ := json.Marshal(map[string]string{"assistant": agent})
	return string(b)
}

// New builds a team. Each agent's configuration gains a terminating
// transfer tool per handoff it offers, named [ToolName] of the
// receiver, described from the receiver's Config.Description, whose
// result carries a [Dest] in its Details. It refuses an agent with no
// name or a name another has, two names with one transfer tool name, a
// handoff to an agent the team does not hold, to the agent itself or
// twice to one agent, and a transfer whose name one of the sender's
// own tools has.
func New(agents ...Agent) (*Team, error) {
	t := &Team{configs: map[string]agentturn.Config{}, tools: map[string]string{}, filters: map[[2]string]func(agentturn.Transcript) agentturn.Transcript{}}
	for _, a := range agents {
		name := a.Config.Name
		if name == "" {
			return nil, errors.New("handoff: an agent has no Config.Name")
		}
		if _, ok := t.configs[name]; ok {
			return nil, fmt.Errorf("handoff: two agents are named %q", name)
		}
		if other, ok := t.tools[ToolName(name)]; ok {
			return nil, fmt.Errorf("handoff: agents %q and %q share the transfer tool %s", other, name, ToolName(name))
		}
		t.configs[name] = a.Config
		t.tools[ToolName(name)] = name
	}
	for _, a := range agents {
		cfg := a.Config
		var transfers []agenttool.Tool
		var enabled []func(context.Context) bool
		seen := map[string]bool{}
		for _, h := range a.Handoffs {
			to, ok := t.configs[h.Agent]
			switch {
			case !ok:
				return nil, fmt.Errorf("handoff: %q hands to an unknown agent %q", cfg.Name, h.Agent)
			case h.Agent == cfg.Name:
				return nil, fmt.Errorf("handoff: %q hands to itself", cfg.Name)
			case seen[h.Agent]:
				return nil, fmt.Errorf("handoff: %q hands to %q twice", cfg.Name, h.Agent)
			}
			seen[h.Agent] = true
			for _, own := range cfg.Tools {
				if own.Name() == ToolName(h.Agent) {
					return nil, fmt.Errorf("handoff: %q has a tool named %s, the transfer to %q", cfg.Name, own.Name(), h.Agent)
				}
			}
			transfers = append(transfers, transfer(to))
			enabled = append(enabled, h.Enabled)
			if h.Filter != nil {
				t.filters[[2]string{cfg.Name, h.Agent}] = h.Filter
			}
		}
		t.configs[cfg.Name] = offer(cfg, transfers, enabled)
	}
	return t, nil
}

// offer returns cfg with transfers among its tools. A transfer whose
// Enabled is set, or a configuration whose tools come from a provider,
// is offered through a ToolProvider that asks each request; Tools
// lists every transfer either way, for whoever reads the tools whole,
// [Team.Offer]'s removals not applied.
func offer(cfg agentturn.Config, transfers []agenttool.Tool, enabled []func(context.Context) bool) agentturn.Config {
	if len(transfers) == 0 {
		return cfg
	}
	own := cfg
	cfg.Tools = append(append([]agenttool.Tool(nil), cfg.Tools...), transfers...)
	gated := own.ToolProvider != nil
	for _, e := range enabled {
		gated = gated || e != nil
	}
	if !gated {
		return cfg
	}
	cfg.ToolProvider = func(ctx context.Context) []agenttool.Tool {
		tools := append([]agenttool.Tool(nil), own.ResolveTools(ctx)...)
		for i, tr := range transfers {
			if enabled[i] == nil || enabled[i](ctx) {
				tools = append(tools, tr)
			}
		}
		return tools
	}
	return cfg
}

// transfer is the tool that hands the conversation to to.
func transfer(to agentturn.Config) agenttool.Tool {
	desc := "Handoff to the " + to.Name + " agent to handle the request."
	if to.Description != "" {
		desc += " " + to.Description
	}
	text := output(to.Name)
	return agenttool.NewFunc(ToolName(to.Name), desc, json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
		func(context.Context, agenttool.Call) (agenttool.Result, error) {
			r := agenttool.Text(text)
			r.Terminate, r.Details = true, Dest{To: to.Name}
			return r, nil
		})
}

// Config returns the configuration of the agent named name, its
// transfer tools included; the zero Config when the team holds none.
func (t *Team) Config(name string) agentturn.Config { return t.config(name) }

// config is the configuration of the agent named name as the team
// hands it out: with its tools offered through [Team.Offer] when that
// is set.
func (t *Team) config(name string) agentturn.Config {
	cfg, ok := t.configs[name]
	if !ok || t.Offer == nil {
		return cfg
	}
	offer, whole := t.Offer, cfg
	cfg.ToolProvider = func(ctx context.Context) []agenttool.Tool {
		return offer(ctx, name, whole.ResolveTools(ctx))
	}
	return cfg
}

// Route says where a transfer call of the team hands the conversation:
// the receiver's configuration, transfer tools included, and the text
// the transfer tool answers with. It is what front/responses' and
// front/a2a's WithTransfers take, so a served conversation starts where
// the last transfer left it, the transfer found by
// front/responses.Handoffs' rules.
func (t *Team) Route() responses.Route {
	return func(call *openresponses.FunctionCall) (agentturn.Config, string, bool) {
		name, ok := t.tools[call.Name]
		if !ok {
			return agentturn.Config{}, "", false
		}
		return t.config(name), output(name), true
	}
}

// Run prompts a with items and follows every handoff: when a run stops
// on a terminating result (agentturn.StopTerminate or
// StopPartialTerminate) whose batch handed off, the last transfer of
// it by [Team.Route], it trims the transcript once with [Team.Trim]
// and Agent.SetTranscript, switches a to the receiver with
// Agent.SetConfig, and continues under
// agentturn.Trigger{Kind: "handoff", Ref: sender}. It returns the last
// run's end and the names of the agents that had the conversation, in
// order.
//
// A receiver with no ToolElicitor or ToolRecorder of its own is given
// the sender's. After [Team.MaxHandoffs] handoffs a run that ends on
// another returns [ErrTooManyHandoffs], that handoff taken but its
// receiver not run.
func (t *Team) Run(ctx context.Context, a *agentturn.Agent, items ...openresponses.Item) (*agentturn.RunEnd, []string, error) {
	path := []string{a.Config().Name}
	end, err := a.Prompt(ctx, items...)
	return t.follow(ctx, a, path, end, err)
}

// Resume answers a's pending calls with Agent.Resume and follows the
// handoffs the resumed run ends on, as [Team.Run] does, so a transfer
// the model makes after a run paused on a deferred call is taken too.
func (t *Team) Resume(ctx context.Context, a *agentturn.Agent, answers ...agentturn.Answer) (*agentturn.RunEnd, []string, error) {
	path := []string{a.Config().Name}
	end, err := a.Resume(ctx, answers...)
	return t.follow(ctx, a, path, end, err)
}

// follow takes the handoffs end and the runs after it end on.
func (t *Team) follow(ctx context.Context, a *agentturn.Agent, path []string, end *agentturn.RunEnd, err error) (*agentturn.RunEnd, []string, error) {
	limit := t.MaxHandoffs
	if limit <= 0 {
		limit = responses.DefaultMaxHandoffs
	}
	for n := 0; err == nil && terminated(end); n++ {
		to, ok := t.Handoff(ctx, end, nil)
		if !ok {
			if a.State().Steering == 0 {
				break
			}
			// A delivery the terminating stop left queued, which the
			// agent did not start a run for (agentturn.ErrTerminated),
			// is the agent's to take: with no handoff to follow, the
			// team runs it now rather than at the next input (#290).
			end, err = a.Continue(ctx)
			n--
			continue
		}
		from := a.Config()
		if _, filtered := t.filters[[2]string{from.Name, to.Name}]; filtered {
			if err := a.SetTranscript(t.Trim(ctx, from, to, a.State().Transcript)); err != nil {
				return end, path, err
			}
		}
		if err := a.SetConfig(inherit(to, from)); err != nil {
			return end, path, err
		}
		path = append(path, to.Name)
		var note openresponses.Item
		if !agentturn.CanContinue(a.State().Transcript) {
			// The trim left a model output last: the note is the
			// receiver's input, so the loop appends it and a recorder
			// writes it (#276).
			note = responses.HandoffNote(ToolName(to.Name), from, to)
		}
		handed := agentturn.ContextWithTrigger(ctx, agentturn.Trigger{Kind: "handoff", Ref: from.Name})
		if n == limit {
			// Taken, not run: the receiver has the conversation, as
			// the transcript says, and answers the next input, unless
			// a delivery is waiting, which it takes now in one run
			// whose own transfer is not followed (#290).
			bounded := fmt.Errorf("%w: %d followed, and %s would hand to %s again", ErrTooManyHandoffs, limit, from.Name, to.Name)
			if a.State().Steering == 0 {
				return end, path, bounded
			}
			if note != nil {
				end, err = a.Prompt(handed, note)
			} else {
				end, err = a.Continue(handed)
			}
			return end, path, errors.Join(bounded, err)
		}
		if note != nil {
			end, err = a.Prompt(handed, note)
		} else {
			end, err = a.Continue(handed)
		}
	}
	return end, path, err
}

// Handoff returns the receiver of the last transfer of the team that
// handed the conversation off in end's items, by [Team.Route], and
// false when none did. It is what front/responses' and front/a2a's
// WithHandoff take, so a team served through a front relays the
// receiver's run within the request or task, as [Team.Run] continues
// it in process.
func (t *Team) Handoff(_ context.Context, end *agentturn.RunEnd, _ []*agentturn.ToolEnd) (agentturn.Config, bool) {
	if end == nil {
		return agentturn.Config{}, false
	}
	hs := responses.Handoffs(end.Items, t.Route())
	if len(hs) == 0 {
		return agentturn.Config{}, false
	}
	return hs[len(hs)-1].To, true
}

// Trim applies the filter of the handoff from from to to, when it
// offers one, to tr, and returns tr unchanged otherwise. A trimmed
// transcript the loop cannot continue, a model output left last as a
// transfer's preamble is once its call is filtered out, is continued
// with responses.HandoffNote as the receiver's input, which names the
// transfer, the sender and the receiver: [Team.Run] and the fronts
// deliver it as the receiver's run's input rather than Trim adding it,
// so the loop appends it and a recorder writes it, once, and a later
// trim of a history that holds it adds none (#276). It is what the
// fronts' WithHandoffTrim take, so a team served through a front trims
// as [Team.Run] does.
func (t *Team) Trim(_ context.Context, from, to agentturn.Config, tr agentturn.Transcript) agentturn.Transcript {
	f := t.filters[[2]string{from.Name, to.Name}]
	if f == nil {
		return tr
	}
	return f(append(agentturn.Transcript(nil), tr...))
}

// inherit returns next with from's question and record channels where
// next has none of its own.
func inherit(next, from agentturn.Config) agentturn.Config {
	if next.ToolElicitor == nil {
		next.ToolElicitor = from.ToolElicitor
	}
	if next.ToolRecorder == nil {
		next.ToolRecorder = from.ToolRecorder
	}
	return next
}

// terminated reports whether a terminating tool result stopped the run.
func terminated(end *agentturn.RunEnd) bool {
	return end != nil && end.Reason == agentturn.ReasonStopped &&
		(end.Cause == agentturn.StopTerminate || end.Cause == agentturn.StopPartialTerminate)
}

// RemoveAllTools is the Agents SDK's handoff_filters.remove_all_tools:
// the history without function calls and their outputs.
func RemoveAllTools(t agentturn.Transcript) agentturn.Transcript {
	var out agentturn.Transcript
	for _, it := range t {
		switch it.(type) {
		case *openresponses.FunctionCall, *openresponses.FunctionCallOutput:
			continue
		}
		out = append(out, it)
	}
	return out
}
