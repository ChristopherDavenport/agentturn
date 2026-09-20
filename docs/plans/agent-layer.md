# Plan: agent layer

> Update, 2026-09-19: the `tool` package and the two MCP adapters
> described below were extracted into their own repository,
> `github.com/ChristopherDavenport/agenttool` (`agenttool`,
> `agenttool/mcpclient`, `agenttool/mcpserver`), so the contract is
> versioned apart from the loop. Read `tool` below as `agenttool`,
> `tools/mcp` as `mcpclient` and `front/mcp` as `mcpserver`. The
> composition rule is unchanged: the loop knows a `Model` and a list of
> `agenttool.Tool`.
>
> Update, 2026-09-20: the API shapes below were revised after the
> usability review. `Run` and `Continue` yield `iter.Seq[Event]` with
> the `RunEnd` as the single terminal; `Prompt`, `Continue` and
> `Resume` return the `RunEnd` beside the error; `ToolDecision` has an
> `Action` (`Allow`, `Block`, `Defer`) instead of booleans; the
> execution modes are `ExecParallel` and `ExecSequential`; `tools/agent`
> constructs with `New` and reads the parent transcript that the loop
> attaches to every tool call's context; `Agent.SetConfig` and
> `SetTranscript` change a live agent between runs.

A generic, designable agent runtime that sits between `openresponses` (the
model wire) and the fronts that expose an agent (Open Responses endpoint,
A2A executor, TUI, chat channels). It is the Go counterpart of
`@earendil-works/pi-agent-core`: a loop, a transcript, a tool contract,
hooks, and queues. Everything else is a front or a subscriber.

## Goals

- One loop, one transcript type, one event stream. Fronts differ only in
  how they feed prompts in and consume events out.
- The transcript is `openresponses.Items`. No third message model. What
  the session stores, what the model receives, and what a front renders
  are the same bytes.
- Provider-neutral by speaking one protocol. The model is an
  `openresponses.Streamer` or `Adapter`; remote servers come through
  `Client.AsAdapter`, local backends implement the interface, and
  translation for other wire formats lives outside this module.
- Extensible without editing the loop: hooks before and after tool calls
  and after each turn, a context transform before each model call, and
  custom transcript items via the openresponses registry.
- Deterministic and testable offline with the `echo` adapter and
  `streamtest`.

## Non-goals

- Provider catalogs, cost tables and auth resolution. Those belong to
  an adapter or a gateway. Retrying a failed model call is the loop's,
  by amendment (see Retry below): only the loop knows whether the
  failed attempt already delivered items, which is what decides
  whether a retry is safe.
- Persistence. The loop takes a transcript and returns events; the session
  library, `agentsession`, is a subscriber.
- Any transport. Fronts own transports.
- A permission system. `BeforeToolCall` is the seam; policy is the
  caller's.

## Module and packages

Separate module, `github.com/ChristopherDavenport/agentturn`. Depends
on `openresponses` and the standard library only.
Fronts that need heavier dependencies (a2a-go, a TUI) are their own
modules or packages so the core keeps its two dependencies.

```
agentturn/                  loop, Agent, events, hooks, queues, run control
agentturn/tool              Tool contract, registry, argument decoding, batch executor
agentturn/front/responses   the loop as an openresponses.Adapter
agentturn/front/a2a         the A2A bridge (separate module: imports a2a-go)
agentturn/front/mcp         Go tools served over MCP (separate module: imports an MCP SDK)
agentturn/tools/...         built-in tools, one package each, opt-in
agentturn/tools/agent       an agent as a Tool: in-process child runs
agentturn/tools/mcp         remote MCP tools as Tool values (separate module)
agentturn/tools/a2a         a remote A2A agent as a Tool (separate module: imports a2a-go)
agentturn/session           agentsession subscriber (separate module: imports agentsession)
```

`tool` depends on `openresponses` only and never imports the root
`agentturn` package, so typed tools are usable from a loop that is not
this one.

## Core types

```go
// Model is anything that can stream an Open Responses response.
type Model = openresponses.Streamer

// Transcript is the conversation as the model sees it, plus app-only
// items that Filter removes before each call.
type Transcript = openresponses.Items

type Config struct {
    Name          string               // how the agent names itself when composed
    Description   string               // one paragraph; tool description, A2A card
    Model         Model
    ModelName     string
    Instructions  string
    Tools         []tool.Tool
    Reasoning     openresponses.ReasoningConfig
    Text          openresponses.TextConfig
    ToolExecution ExecutionMode        // Parallel (default) or Sequential
    Filter        func(Transcript) openresponses.Items   // drop app-only items
    Transform     func(ctx, Transcript) (Transcript, error) // prune, compact
    BeforeToolCall func(ctx, ToolCallInfo) (*ToolDecision, error)
    AfterToolCall  func(ctx, ToolResultInfo) (*ToolOverride, error)
    ShouldStopAfterTurn func(ctx, TurnInfo) (bool, error)
    RequestExtra  map[string]any       // passthrough to Request.Extra
}
```

Turn and run vocabulary follows pi: a run is `Prompt` or `Continue` until
the agent goes idle; a turn is one model call plus the tool executions it
requested.

### Low-level loop

```go
func Run(ctx context.Context, t Transcript, prompts openresponses.Items, cfg Config) iter.Seq2[Event, error]
func Continue(ctx context.Context, t Transcript, cfg Config) iter.Seq2[Event, error]
```

Observational: events are yielded in order but the loop does not wait for
consumers between phases. `Continue` requires the last item to be a user
message or a function call output.

### Agent

```go
type Agent struct { /* state, queues, subscribers */ }

func New(cfg Config, opts ...Option) *Agent
func (a *Agent) Prompt(ctx, items ...openresponses.Item) error
func (a *Agent) Continue(ctx) error
func (a *Agent) Steer(item openresponses.Item)     // injected after the current tool batch
func (a *Agent) FollowUp(item openresponses.Item)  // injected when the run would otherwise end
func (a *Agent) Subscribe(fn func(ctx, Event) error) (unsubscribe func())
func (a *Agent) Abort()
func (a *Agent) WaitForIdle(ctx) error
func (a *Agent) State() State                       // snapshot
```

Barrier semantics, as in pi's `Agent` class: subscribers are awaited in
registration order, and the assistant `item_end` and `turn_end` events are
barriers. Tool preflight for a turn does not start until every subscriber
has returned for the assistant item. `WaitForIdle` and `Prompt` settle
only after `run_end` subscribers finish. The low-level `Run` has no
barriers.

### Events

```go
type Event interface{ EventType() string }

run_start        { RunID }
turn_start       { RunID, Turn, Request openresponses.Request }   // the exact request sent
item_start       { Item }                    // user, assistant, function_call_output
item_update      { Item, Stream openresponses.StreamEvent }   // assistant only; wraps the wire event
item_end         { Item }
tool_start       { CallID, Name, Args json.RawMessage }
tool_update      { CallID, Partial tool.Result }
tool_end         { CallID, Result tool.Result, Err error, Blocked bool }
turn_end         { Response *openresponses.Response, ToolResults []tool.Result }
run_end          { Items added this run, Reason }   // Reason: done, stopped, aborted, error
```

Model streaming is not re-modelled. `item_update` carries the
`openresponses.StreamEvent` verbatim so a front can switch on the same
concrete types it would use against a remote server, and the folded
`Response` from `Accumulator` arrives on `turn_end` with usage.

### Tool contract

```go
type Tool interface {
    Name() string
    Description() string
    Parameters() json.RawMessage      // JSON schema
    Execute(ctx context.Context, call Call) (Result, error)
}

type Call struct {
    ID        string          // call_id
    Args      json.RawMessage
    OnUpdate  func(Result)    // optional progress
}

type Result struct {
    Output    openresponses.FunctionCallOutputData   // what the model sees
    Details   any             // app-only, never sent to the model
    Terminate bool            // hint: skip the follow-up model call
}

// Optional interfaces
type Sequential interface{ Sequential() bool }    // force sequential batch
type Strict interface{ Strict() bool }            // strict schema flag on the FunctionTool
```

Errors returned from `Execute` become error outputs the model sees; do
not encode errors as normal content.

### Typed tools

Python and TypeScript harnesses apply the model's arguments straight to a
function. The Go equivalent is a struct as the argument type and generics
to erase it: reflection over the struct gives the schema at registration
time, the standard decoder gives the typed value at call time.

```go
type ReadFileArgs struct {
    Path     string `json:"path" desc:"Absolute path to read"`
    MaxBytes int    `json:"max_bytes,omitempty" desc:"Stop after this many bytes"`
}

var ReadFile = tool.New("read_file", "Read a file from disk",
    func(ctx context.Context, a ReadFileArgs) (string, error) { ... })
```

`tool.New[Args, Out](name, desc, fn)` returns a `Tool`. It emits the
`openresponses.FunctionTool` for the request, decodes `Call.Args` into
`Args`, and marshals `Out` into `FunctionCallOutputData`: a `string`
passes through as text, `openresponses.Contents` goes out as parts so
image-returning tools need no second constructor, and anything else
marshals to JSON. Options on `New` set `Strict`, `Sequential`, and a
progress callback.

Schema generation is standard library only. A reflection generator
covers structs, `json` tags, a `desc` tag, an `enum` tag, scalars,
slices, maps, and nested structs. Writing it by hand rather than pulling
in a schema library keeps the module at two dependencies and lets strict
mode match the provider rule exactly: every field required,
`additionalProperties: false`, and optional fields expressed as pointer
types that become nullable.

Decode failures are model-visible errors, not panics. Bad arguments
become an error output so the model can retry. Unknown fields are
ignored rather than rejected; a strict schema already prevents them.

The `tool` package imports `openresponses` and nothing upward. The test
of the boundary: a caller writes a tool with `tool.New`, hands it to
their own loop, and never imports the root `agentturn` package.

### MCP

The native contract is the centre and MCP is an edge. An MCP tool is a
name, a description, an input schema and a call over a transport, which
is a subset of `Tool`, so MCP is two adapters in their own modules:
`front/mcp` exposes a set of Go tools as an MCP server, and `tools/mcp`
wraps a remote server's tools as `Tool` values. Neither is a dependency
of the core, and a harness that only writes Go tools never imports
either.

Both adapters use the official Go SDK,
`github.com/modelcontextprotocol/go-sdk`, and nothing else beyond
`openresponses` and the `tool` package. The SDK owns transports (stdio,
streamable HTTP) and protocol negotiation; the adapters own the mapping.

`tools/mcp` is the consumer side:

```go
// Connect opens one session and returns its tools. The caller closes it.
func Connect(ctx context.Context, t mcp.Transport, opts ...Option) (*Server, error)
func (s *Server) Tools() []tool.Tool
func (s *Server) Close() error
```

- Each remote tool becomes a `Tool`: `name` is the tool name, optionally
  prefixed with a server label (`WithPrefix("fs")` gives `fs__read`) so
  two servers with a `read` tool do not collide in one request; the
  remote `description` passes through; `inputSchema` is the JSON schema,
  verbatim, so a strict flag is never set on it because the schema was
  not generated under strict rules.
- `Execute` calls the tool with `Call.Args` as the argument object. A
  result with only text content becomes `Output.Text`; a result with
  image or resource content becomes `Output.Parts` using the matching
  `openresponses` content types; `isError` becomes a returned error so
  the loop produces the error output the model sees. Structured content
  is appended as JSON text when present.
- Remote tools are never `Sequential` unless the caller opts a name in
  with `WithSequential`.
- `Tools()` is a snapshot. The `Server` subscribes to the SDK's
  tool-list-changed notification and refreshes it; the loop reads
  `Config.Tools` each turn, so a caller who wants live updates passes
  `s.Tools` as a function through an option rather than a fixed slice.
  Whether `Config.Tools` should itself accept a provider function is
  listed under open questions.
- Progress notifications map to `Call.OnUpdate` when the caller set one.

`front/mcp` is the producer side and is the inverse mapping: a `Tool`'s
name, description and `Parameters()` become the MCP tool definition,
and the SDK's tool handler decodes the call, runs `Execute` and maps
`Output` back to MCP content, with a returned error setting `isError`.
`BeforeToolCall` and `AfterToolCall` do not run here; policy belongs to
whoever hosts the loop, and this front hosts only tools.

### Batch execution

Default parallel:

1. Preflight every call sequentially: decode arguments, run
   `BeforeToolCall`. A block produces an error output and can set
   `Terminate`.
2. Execute allowed calls concurrently with `errgroup`, bounded by a
   configurable limit. If any tool in the batch is `Sequential`, the whole
   batch runs sequentially.
3. Emit `tool_end` in completion order; append `function_call_output`
   items and emit their `item_start`/`item_end` in assistant source order.
4. The loop stops early only when every finalized result in the batch set
   `Terminate`.

### Request construction

Each turn builds an `openresponses.Request` from config plus the filtered,
transformed transcript. `Store` is false. `PreviousResponseID` is never
used by the loop; history is always inlined, so the loop works against
servers without a store and so the session library sees the full input.
Run and turn IDs travel on the events, not in request `Metadata`, so
the settings a session recorder sees change only when config does.

### Retry

Amendment, 2026-09-20 (issue #22). `Config.Retry` is a policy of
`MaxAttempts`, `Backoff(attempt, err)` and `Retryable(err)`, off by
default. It applies to the model call only, inside the turn: the same
request is sent again after the delay, a `model_retry` event carries
the attempt number, the error and the delay, and the turn's
`turn_start` and `response_end` are delivered once, so a recorder sees
one call. An attempt is retried only when nothing of it reached
subscribers and the server did not answer with a terminal response;
an attempt that delivered an item is final, because the transcript or
a recorder may already hold part of it. The defaults retry 408, 409,
429, 5xx, truncated streams and transport failures, honour
`Retry-After`, and otherwise double from 500ms to a 30s cap. Abort
cuts a delay short.

### Custom items

App-only entries in the transcript (notifications, UI markers, branch
summaries) are slug-prefixed items registered with
`openresponses.RegisterItem`, for example `agentturn:note`. The default
`Filter` drops every item whose type has a slug prefix unless it is
listed as model-visible. Because the registry keeps unknown items
verbatim, sessions written by newer builds still load.

### Compaction

`Transform` runs before each model call and may return a shorter
transcript. The reference implementation calls the adapter's `Compact`
when the input exceeds a token budget and splices the returned
`Compaction` item in. Local summarisation is a second implementation
behind the same hook.

## Invariants

- The transcript after a run is always a valid Open Responses input:
  every `function_call` has exactly one `function_call_output` before the
  next user message, or the run ended with the unanswered calls on
  `RunEnd.Pending`: deferred to the caller (input-required, a front's
  concern) or cut off by an abort or a failure. Either way the caller
  answers them through `Resume` before the next prompt.
- Exactly one `run_end` per run, nothing after it.
- `item_end` for an assistant item is emitted only after
  `output_item.done` from the stream; partial items never reach
  subscribers as `item_end`.
- Abort cancels the model stream and running tools through the context;
  a run aborted mid-turn ends with `Reason: aborted` and the transcript
  contains only completed items. Every event the abort leaves behind,
  the `tool_end` of each cut-off call and the `run_end`, is delivered,
  and an `Agent` delivers it with the cancellation lifted, so a
  subscriber that writes durable state can (issue #23).
- Every `tool_start` has its `tool_end`, whether the call ran, was
  blocked, deferred, or cut off before or during execution.
- Events for one run are delivered from one goroutine.

## Fronts

- `front/responses`: implements `openresponses.Adapter`. `CreateStream`
  runs one turn per request when the caller runs its own tools, or a full
  run when the agent owns the tools, re-emitting assistant items through
  an `Emitter`. `Compact` delegates to the model.
- `front/a2a`: the A2A bridge. Owns a conversation store
  keyed by A2A context ID, an artifact writer that coalesces text deltas,
  the local-versus-caller tool split with input-required as the boundary,
  and a cancel registry. Only package that imports a2a-go.
- TUI and chat channels: consume `Subscribe`, feed `Prompt`, `Steer`,
  `FollowUp`.

## Composition

The aim is composable agents: a coding agent, a chat bot and a fleet of
specialists are the same loop arranged differently, and any of them can
be exposed over A2A or used as a part of another. The loop never learns
a sub-agent concept. It knows a `Model` and a list of `Tool`, and every
composition is one of those two things.

### Three roles

An agent can stand in three places inside another system.

- **As a model.** `front/responses` makes a loop an
  `openresponses.Adapter`; a `Handler` serves it; another loop points
  its `Model` at it through `Client.AsAdapter`. Composition over a
  network with no second protocol.
- **As a tool.** `tools/agent` wraps a `Config` as a `Tool`. `Execute`
  runs a child loop on a fresh transcript, streams the child's events
  out through `Call.OnUpdate`, and returns the final assistant text as
  `Output`. This is the sub-agent pattern in pi and Claude Code.
- **As a peer.** `front/a2a` exposes a loop to A2A callers; `tools/a2a`
  wraps a remote A2A agent as a `Tool`, mapping a task to one call and
  input-required to a returned error the model can answer.

### Both directions for every protocol

A protocol earns a place only with a serve side and a consume side, so
an agent behind any protocol can be a component of any other agent.

| protocol | serve | consume |
|---|---|---|
| Open Responses | `front/responses` | `openresponses.Client.AsAdapter` |
| MCP | `front/mcp` | `tools/mcp` |
| A2A | `front/a2a` | `tools/a2a` |
| in-process | `Agent` | `tools/agent` |

### Self-description

`Config.Name` and `Config.Description` are the single source for how an
agent presents itself when composed. `tools/agent` uses them as the tool
name and description; `front/a2a` derives the agent card from them plus
the tool list as skills; `front/mcp` uses them for the server info. An
agent is described once.

### `tools/agent`

```go
// Tool wraps an agent as a tool. Args defaults to {"input": string}.
func Tool(cfg agentturn.Config, opts ...Option) tool.Tool

// Options
WithArgs[T any]()                 // typed arguments, rendered as the first user message
WithTranscript(func(parent Transcript) Transcript)   // seed the child from the parent
WithObserver(func(ctx, Event))     // every event of the child run, for session recording
```

- The child gets its own run, transcript and event stream. Parent hooks
  do not run inside it; the child's own `Config` carries its hooks.
- `Result.Details` carries a `ChildInfo` with the child's run ID and
  the items it added, so a session subscriber on the parent writes a
  `link` entry with `rel: subsession` and the spawning `call_id`, as the
  session format specifies.
- The observer sees the child's events from the tool's goroutine, with
  the parent's tool-call context: the loop attaches its run ID to the
  context of everything a run calls, so the observer knows which run
  the child belongs to, and `tools/agent` adds the child's `Config`.
  That is how one observer serves every level of nesting (amendment,
  2026-09-20, issue #24).
- Abort on the parent cancels the child through the context. The
  observer's context has the cancellation lifted, as an `Agent` lifts
  it for subscribers, so what the abort leaves behind is still written.
- Nesting is unbounded and each level is the same code, so a specialist
  that itself delegates needs nothing new.

### What a product adds

pi-style coding agent: the loop, `tools/` for shell and files, a TUI
front, the reference compaction transform, steering, and `agentsession`
as a subscriber. OpenClaw-style assistant: the same core with channel
fronts and a gateway that owns which agents are reachable. Two modules
neither the core nor this plan provides and that compose in through
existing seams:

- A permission policy through `BeforeToolCall`. Policy is the host's,
  as the non-goals say; a policy module is a `BeforeToolCall` value and
  nothing else.
- Skills or prompt context through `Transform`, which already sees the
  whole transcript before each model call.

A product is a wiring of modules, and every module is usable without
the product.

### Session

`agentturn/session` is a nested module that subscribes an `Agent` to an
`agentsession` store. It lives here rather than in `agentsession` so the
session library never depends on this module's event or result types.
It imports `agentsession` and the root package and nothing else. The
package is named `session`, not `agentsession`, because a consumer
always imports both side by side, to open a store and to attach it, and
should not have to alias one of them.

- Appends assistant items on `item_end`, user items and function call
  outputs on their `item_end`, and the `response` entry on `turn_end`,
  following the writing discipline in the session plan.
- Computes `request_hash` from the `Request` carried on `turn_start`
  using the RFC's definition (JCS canonical form, SHA-256). The
  implementation is a copy of `agentsession.RequestHash` tested against
  the same golden vectors, so the two never drift.
- On `tool_end` whose `Details` is a `ChildInfo`, writes a `link` entry
  with `rel: subsession` and the spawning `call_id`. The child's own
  session is written live through `Recorder.Observe`, registered as the
  child tool's observer: created on the child's `run_start` under the
  session of the run on the context, filled with the child's config,
  items, responses and folds as they happen (issue #24). A child that
  was not observed is written from `ChildInfo.Items`, items only.
- On a fold reported by `compact.WithOnFold` through `Recorder.Fold`,
  writes the compaction entry, naming as `first_kept` the entry of the
  item at the fold's split index; the recorder keeps the entry ID of
  every item it wrote, aligned with the working transcript, and
  `Resume` seeds that from the context at the leaf. A failed fold is a
  custom entry in `agentturn:compaction_failed` (issue #27).
- Returns from the subscriber only after the append is durable under the
  store's sync policy, so the barrier holds: tool preflight for a turn
  waits on the write.

## Milestones

1. `tool` package: contract, `tool.New` typed wrapper, standard-library
   schema generation with strict-mode output, batch executor with
   parallel and sequential modes. Table tests, including golden schemas
   for the generator and a test that the package does not import the root package.
2. `Run` and `Continue` against the `echo` adapter: events, request
   construction, filter, transform. `streamtest` for the wire side.
3. `Agent`: queues, subscribers with barriers, abort, idle. Tests that
   assert barrier ordering with a slow subscriber.
4. Hooks: before, after, stop-after-turn, terminate semantics.
5. `front/responses` and a compliance run against it.
6. `front/a2a` as its own module with the invariants from the bridge
   design tested against a2a-go's in-memory handler.
7. Reference compaction transform using `Compact`.
8. `tools/mcp` as its own module, tested against an in-process SDK server
   over the in-memory transport: name prefixing, text and image results,
   `isError`, and list-changed refresh. `front/mcp` after it, round-trip
   tested by serving `tool.New` tools and consuming them with `tools/mcp`.
9. `tools/agent`: child run, progress through `OnUpdate`, `ChildInfo` in
   `Details`, abort propagation, three levels of nesting against `echo`.
   Then `tools/a2a` as its own module, round-trip tested against
   `front/a2a` in memory.
10. `session` as its own module: every event maps to the right entry,
    a slow append delays tool preflight, a child run produces a `link`,
    and the stored path rebuilds a request that hashes to the recorded
    `request_hash`. Milestones 1 through 9 do not depend on
    `agentsession`; this one is the integration point.

## Open questions

Revisited 2026-09-20 against the code. Each question is marked
resolved, with the answer the code gives, or deferred, with what would
reopen it.

Resolved:

- `front/a2a` lives in this repo, as a nested module with its own
  `go.mod`, alongside `tools/a2a`. One version per repository, tagged
  in lockstep with the root.
- `Filter` stays a function in `Config`, typed `Transcript` to
  `Transcript`, with `DefaultFilter` and `VisibleFilter` as the
  ready-made values. No wrapper type; `Transcript` remains an alias of
  `openresponses.Items` so the transcript and the wire share bytes.
- The concurrency limit defaults to `agenttool.DefaultMaxParallel`,
  which is 8; `Config.MaxParallelTools` overrides it.
- The schema generator lives in the `agenttool` root package
  (`Reflect`, `SchemaOf`, `SchemaFor`), not in a sub-package, and is
  importable on its own for `text.format` schemas since the whole
  contract moved to its own module.
- The MCP adapters shipped in the first release together with
  `front/a2a`, and then moved to `agenttool` as `mcpclient` and
  `mcpserver` because MCP is about tools, not the loop.
- `Config.ToolProvider` supplies the tools for each turn in place of
  `Config.Tools`; the loop resolves it once per turn through
  `ResolveTools`, before the model call, and the same list serves the
  turn's batch so a call resolves against what the model was offered.
  A provider that snapshots a remote list lags a change still in
  flight by one turn; a provider that must not waits inside itself,
  bounded by its context, as `mcpclient.Remote.Await` allows (issue
  #28). `front/a2a` and the responses front use the same resolution.
- A deferred call is answered through `Agent.Resume` with an `Answer`:
  an output, or an approval that runs the call inside the loop with
  `BeforeToolCall` skipped and everything else as in a turn (issue
  #25). A `Prompt` that opens with the outputs of every pending call
  answers them on the way to the next message (issue #26).
- `mcpclient` is strictly a tool adapter: no resources, no prompts. A
  resource or prompt reaches the model as a tool that returns it, or
  through the host's `Transform`.
- A child in `tools/agent` requires its own `Model`; the loop
  validates `Config.Model` on every run and a nil model refuses to
  start. Sharing is one assignment at the call site, and requiring it
  keeps a child's configuration complete on its own.
- The child's full transcript returns through `ChildInfo.Items`, so
  a session recorder that did not observe the child can still write it
  as a linked subsession, items only. The full record, config and
  responses included, comes through the observer written live (issue
  #24). A host that wants less keeps only what it needs; the loop does
  not summarise.
- `front/a2a` advertises one default skill from `Config.Description`
  plus one skill per tool, resolved through `ToolProvider` when set.
- Retries, formerly a non-goal, are the loop's: see Retry above.

Deferred:

- Whether steering should cancel in-flight tools. Today `Steer` queues
  items for after the current batch, as pi does; a tool that runs long
  is cut only by `Abort`. Reopen if a front needs "stop that tool but
  keep the run", which would need a per-call cancel on `ToolStart` and
  a defined output for the cut call.
