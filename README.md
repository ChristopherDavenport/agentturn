# agentturn

A composable agent loop in Go over
[Open Responses](https://www.openresponses.org): one loop, one transcript
type, one tool contract, hooks and queues. Everything else is a front
that feeds prompts in and consumes events out, or a subscriber.

- The transcript is `openresponses.Items`. What a session stores, what
  the model receives and what a front renders are the same bytes. An
  item a harness adds for the model and not for the user, an interrupt
  report or an advisory, is wrapped in `agentturn.Hidden` where it is
  appended: the model still reads it, and its item events and its
  session entry say a renderer should not show it.
- The model is any `openresponses.Streamer`: a remote server through
  `Client.AsAdapter`, a local adapter, or another agent served by
  `front/responses`.
- The tool contract is its own library,
  [agenttool](https://github.com/ChristopherDavenport/agenttool), so a
  tool is written once and runs under any loop. The root module depends
  on `openresponses`, `agenttool` and the standard library; adapters
  with heavier dependencies are nested modules.

## Install

```sh
go get github.com/ChristopherDavenport/agentturn
```

Go 1.25 or later.

## A tool, a loop, an agent

```go
type ReadFileArgs struct {
	Path     string `json:"path" desc:"Absolute path to read"`
	MaxBytes int    `json:"max_bytes,omitempty" desc:"Stop after this many bytes"`
}

var ReadFile = agenttool.New("read_file", "Read a file from disk",
	func(ctx context.Context, a ReadFileArgs) (string, error) {
		b, err := os.ReadFile(a.Path)
		return string(b), err
	})

cfg := agentturn.Config{
	Name:         "reader",
	Description:  "Answers questions about files on disk.",
	Model:        openresponses.NewClient(baseURL, openresponses.WithAPIKey(key)).AsAdapter(),
	ModelName:    "gpt-5",
	Instructions: "Be brief.",
	Tools:        []agenttool.Tool{ReadFile},
}

// Low-level: an iterator of events. The loop runs ahead of the consumer
// and the run_end is always the last event.
for ev := range agentturn.Run(ctx, nil, openresponses.Items{openresponses.UserText("What is in go.mod?")}, cfg) {
	switch e := ev.(type) {
	case *agentturn.ItemUpdate:
		if d, ok := e.Stream.(*openresponses.OutputTextDeltaEvent); ok {
			fmt.Print(d.Delta)
		}
	case *agentturn.RunEnd:
		// e.Reason is done, stopped, input_required, aborted or error;
		// a cancelled ctx ends the run with aborted and context.Canceled
		// on e.Err rather than panicking or hanging.
		if e.Err != nil {
			log.Fatal(e.Err)
		}
	}
}

// Stateful: queues, subscribers with barriers, abort, idle.
a := agentturn.New(cfg)
a.Subscribe(func(ctx context.Context, ev agentturn.Event) error {
	// every event is a barrier: the loop waits for this to return
	return nil
})
end, err := a.Prompt(ctx, openresponses.UserText("What is in go.mod?"))
```

`Prompt` returns the `RunEnd`; the error is set only when the run could
not start or ended with `error`. A print front correlates `tool_end`,
which arrives in completion order, with `tool_start` by call ID rather
than by position:

```go
started := map[string]string{}
a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
	switch e := ev.(type) {
	case *agentturn.ToolStart:
		started[e.CallID] = e.Name + " " + string(e.Args)
		fmt.Println("▶", started[e.CallID])
	case *agentturn.ToolEnd:
		fmt.Printf("  [%s] %s\n", started[e.CallID], e.Result.Output.Text)
	}
	return nil
})
```

A run is one `Prompt` or `Continue` until the agent goes idle; a turn is
one model call plus the tool executions it requested. The events of a
run, in order:

| event | carries |
|---|---|
| `run_start` | run ID |
| `turn_start` | the exact `openresponses.Request` sent |
| `model_retry` | a transient model failure about to be retried under `Config.Retry`: attempt, error, delay |
| `item_start`, `item_update`, `item_end` | an item entering the transcript; `item_update` wraps the wire `StreamEvent` verbatim |
| `response_end` | the folded `Response` with usage, before any tool of the turn runs |
| `tool_start`, `tool_update`, `tool_end` | one tool call from preflight to result, `tool_end` in completion order |
| `turn_end` | the folded `Response` with usage, and the tool results |
| `run_end` | the items added this run and the reason: done, stopped, input_required, aborted, error |

## Tools

Tools come from `agenttool`: `agenttool.New[Args, Out]` turns a typed
function into a tool, reflecting and validating the schema from the
argument struct, and `agenttool.Executor` runs a batch. The loop only
knows the `agenttool.Tool` interface; see that repository for the
contract, the schema generator and the MCP adapters.

A batch of calls runs in parallel, bounded, unless the config or a tool
asks for sequential execution. `Config.BeforeToolCall` is the policy
seam: block, rewrite arguments, terminate the run, or defer the call to
the caller. A deferred call ends the run with `input_required` and the
pending calls listed; `Agent.Resume` takes an answer per call and
continues, which is how a front asks a human before a tool runs. An
answer is an output the front produced, usually a refusal, or an
approval, which runs the call inside the loop with its tool events,
hooks and execution mode:

```go
cfg.BeforeToolCall = func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
	if info.Call.Name == "delete_file" {
		return &agentturn.ToolDecision{Action: agentturn.Defer}, nil
	}
	return nil, nil
}
end, err := a.Prompt(ctx, openresponses.UserText("Clean up the build directory."))
if err == nil && end.Reason == agentturn.ReasonInputRequired {
	// ask the human about end.Pending, then answer every call
	var answers []agentturn.Answer
	for _, call := range end.Pending {
		if approved(call) {
			answers = append(answers, agentturn.Approve(call.CallID).WithBy(agentsession.ByHuman))
		} else {
			answers = append(answers, agentturn.Output(openresponses.NewFunctionCallOutput(call.CallID, "denied by the user")).WithBy(agentsession.ByHuman))
		}
	}
	end, err = a.Resume(ctx, answers...)
}
```

`Answer.WithBy` says who decided, in the session format's terms
(`human`, `policy`, `agent`), and `WithNote` what they said; a recorder
writes both on the decision. There is no default, since a policy engine
answers through `Resume` as often as a person does, so an answer that
names nobody is recorded as an anonymous decision.

The same path repairs a run that was aborted mid-batch: the cut-off
calls are on `end.Pending`, and an agent built with
`agentturn.WithTranscript` from a stored session marks them pending
again. A front whose user has moved on answers them on the way to the
next message instead: a `Prompt` that opens with an output for each
pending call is accepted, and the model sees the outputs and the
message in one call. `AfterToolCall` overrides results;
`ShouldStopAfterTurn` ends a run early.

Every other request member comes from `Config.Request`, the base the
loop builds each turn's request on: `tool_choice`, `max_output_tokens`,
`include: reasoning.encrypted_content` for reasoning models, and so on.
`BeforeModelCall` sees the finished request before it is sent.

`Config.Retry` retries a model call that failed before delivering
anything: a 429 or 5xx, a dropped stream, a refused connection. The
retry happens inside the turn, honours `Retry-After`, reports itself
as a `model_retry` event, and is cut short by `Abort`. An attempt that
already delivered an item is never retried, since the transcript may
hold part of it.

```go
cfg.Retry = agentturn.Retry{MaxAttempts: 4}
```

## Composition

The loop never learns a sub-agent concept. It knows a `Model` and a list
of `Tool`, and every composition is one of those two things. A protocol
earns a place only with a serve side and a consume side:

| protocol | serve | consume |
|---|---|---|
| Open Responses | `front/responses` | `openresponses.Client.AsAdapter` |
| MCP | `agenttool/mcpserver` | `agenttool/mcpclient` |
| A2A | `front/a2a` | `tools/a2a` |
| in-process | `Agent` | `tools/agent` |

The MCP row lives in `agenttool` because MCP is about tools, not about
the loop: `mcpserver` serves any `agenttool.Tool`, and `mcpclient`
produces them. An agent stands in three places inside another system:

- **As a model.** `front/responses` makes a loop an
  `openresponses.Adapter`; `openresponses.NewHandler` serves it; another
  loop points its `Model` at it through `Client.AsAdapter`.
- **As a tool.** `tools/agent` wraps a `Config` as a `Tool`: a child run
  on a fresh transcript, progress through `Call.OnUpdate`, the final
  text as the output and a `ChildInfo` in `Result.Details`.
- **As a peer.** `front/a2a` exposes a loop to A2A callers; `tools/a2a`
  wraps a remote A2A agent as a `Tool`.

`Config.Name` and `Config.Description` are the single source for how an
agent presents itself in every one of these.

## Modules

| path | module | depends on |
|---|---|---|
| `.` (`agentturn`), `front/responses`, `compact`, `tools/agent` | root | `openresponses`, `agenttool` |
| `front/a2a`, `tools/a2a` | nested | `github.com/a2aproject/a2a-go` |
| `session` | nested | `github.com/ChristopherDavenport/agentsession` |

Every module shares the root's version and is tagged at the same
commit, the nested ones with their directory as the prefix
(`front/a2a/v0.0.2`); each is fetched with `go get` like any module.

`compact` is the reference `Transform`: when the transcript exceeds a
token budget it folds the older part and splices the result in front
of the recent tail, caching it by prefix so repeated turns cost
nothing. `New` folds through the model's `Compact` endpoint;
`NewLocal` asks an ordinary model call for a summary and splices it in
as a message, for the servers that do not implement compaction.

```go
c := compact.New(model, compact.WithBudget(60_000))
cfg.Transform = c.Transform

l := compact.NewLocal(model, compact.WithBudget(60_000), compact.WithModel("gpt-5-mini"))
cfg.Transform = l.Transform
```

`WithOnFold` reports every fold, applied or failed, with the index at
which the transcript was split, so a recorder can write it.

`session` subscribes an `Agent` to an `agentsession` store: items on
`item_end`, the `response` entry with its request hash on
`response_end`, config entries as settings change, tool list changes
as deltas, a compaction entry for every fold the compact transform
reports, and a session of its own for every child run it observes,
linked from the parent. Because every event is a barrier, a turn's
tool preflight waits for the assistant items to be durable.

```go
rec, s, err := session.Start(ctx, store, agentsession.Header{CWD: cwd})
defer rec.Attach(agent)()
specialist := agent.New(childCfg, agent.WithObserver(rec.Observe))
c := compact.NewLocal(model, compact.WithOnFold(rec.Fold))
```

## Design

The plan is `docs/plans/agent-layer.md`. Invariants the tests hold:

- The transcript after a run is a valid Open Responses input: every
  `function_call` is answered before the next user message, or the run
  ended with the unanswered calls on `RunEnd.Pending`, whether a hook
  deferred them or an abort or failure cut them off. `Agent.Resume`
  answers them; `Prompt` and `Continue` refuse until it has.
- Exactly one `run_end` per run, nothing after it.
- `item_end` for an assistant item follows `output_item.done`; partial
  items never reach subscribers as `item_end`.
- Abort cancels the model stream and running tools through the context;
  the transcript keeps only completed items. Every event the abort
  leaves behind, the `tool_end` of each cut-off call and the
  `run_end`, still reaches subscribers, with a context whose
  cancellation is lifted.
- Events for one run are delivered from one goroutine.

## License

MIT. See `LICENSE`.
