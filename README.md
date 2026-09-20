# agentturn

A composable agent loop in Go over
[Open Responses](https://www.openresponses.org): one loop, one transcript
type, one tool contract, hooks and queues. Everything else is a front
that feeds prompts in and consumes events out, or a subscriber.

- The transcript is `openresponses.Items`. What a session stores, what
  the model receives and what a front renders are the same bytes.
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

// Low-level: an iterator of events. The loop runs ahead of the consumer.
for ev, err := range agentturn.Run(ctx, nil, openresponses.Items{openresponses.UserText("What is in go.mod?")}, cfg) {
	if err != nil {
		log.Fatal(err)
	}
	if u, ok := ev.(*agentturn.ItemUpdate); ok {
		if d, ok := u.Stream.(*openresponses.OutputTextDeltaEvent); ok {
			fmt.Print(d.Delta)
		}
	}
}

// Stateful: queues, subscribers with barriers, abort, idle.
a := agentturn.New(cfg)
a.Subscribe(func(ctx context.Context, ev agentturn.Event) error {
	// every event is a barrier: the loop waits for this to return
	return nil
})
err := a.Prompt(ctx, openresponses.UserText("What is in go.mod?"))
```

A run is one `Prompt` or `Continue` until the agent goes idle; a turn is
one model call plus the tool executions it requested. The events of a
run, in order:

| event | carries |
|---|---|
| `run_start` | run ID |
| `turn_start` | the exact `openresponses.Request` sent |
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
pending calls listed; `Agent.Resume` takes their outputs and continues,
which is how a front asks a human before a tool runs. `AfterToolCall`
overrides results; `ShouldStopAfterTurn` ends a run early.

Every other request member comes from `Config.Request`, the base the
loop builds each turn's request on: `tool_choice`, `max_output_tokens`,
`include: reasoning.encrypted_content` for reasoning models, and so on.
`BeforeModelCall` sees the finished request before it is sent.

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
token budget it calls the model's `Compact` and splices the returned
compaction item in front of the recent tail, caching the result.

```go
c := compact.New(model, compact.WithBudget(60_000))
cfg.Transform = c.Transform
```

`session` subscribes an `Agent` to an `agentsession` store: items on
`item_end`, the `response` entry with its request hash on
`response_end`, config entries as settings change, and a `link` entry
for every child run. Because every event is a barrier, a turn's tool
preflight waits for the assistant items to be durable.

```go
rec, s, err := session.Start(ctx, store, agentsession.Header{CWD: cwd})
defer rec.Attach(agent)()
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
  the transcript keeps only completed items.
- Events for one run are delivered from one goroutine.

## License

MIT. See `LICENSE`.
