# Changelog

All user-visible changes to this library. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
uses [Semantic Versioning](https://semver.org/); before v1.0.0 minor
versions may break the API.

## Unreleased

- **Fixed**: an abort or a failure during a tool batch left a
  `function_call` without an output in the transcript, so `Continue`
  refused it while `Prompt` sent it. `RunEnd.Pending` now lists every
  call of the run without an output, the deferred ones and the cut-off
  ones alike, and `Agent` treats them the same: `Prompt` and
  `Continue` return `ErrInputRequired` until `Resume` has answered
  them. `WithTranscript` derives the pending calls from the seeded
  transcript, so a session aborted mid-batch resumes through the same
  path. (#1)
- **Breaking**: the loop no longer stamps `agentturn_run_id` and
  `agentturn_turn` into request metadata. The IDs are on every event,
  and the keys made the settings differ on every turn, so a `session`
  recorder wrote a config delta before every response. A session now
  holds one config entry until the configuration actually changes.
  (#2)
- `session`: a model call that fails before any stream event, or a
  run aborted mid-stream, is recorded as a response entry with status
  `failed`, the error and the request hash, so the record shows the
  call was made. The in-flight hash is cleared on every `run_end`, so
  a recorder reused for a later run cannot attribute that run's first
  response to the failed call. (#4)
- `session.Resume` reopens a session and starts the recorder from the
  settings at its leaf, so a resumed session gets a delta or nothing
  rather than a full copy of the instructions and every tool schema.
  (#3)
- `session.Start` names child sessions after the header's harness
  unless `WithHarness` says otherwise, and `Recorder.Store` returns the
  store so a caller need not hold three values for one session. (#5)
- **Fixed**: `front/responses` single-turn mode built its request by
  hand and dropped every `Config.Request` member, so an agent with
  `include: reasoning.encrypted_content` kept its reasoning in full-run
  mode and lost it when the caller passed tools. Both modes now start
  from `Config.BaseRequest` and run `BeforeModelCall`. The new
  `Config.ResolveTools` is the provider fallback the loop uses, for
  fronts that build their own request. (#8)
- **Fixed**: `front/a2a` reported a failed event write as a cancelled
  task. The write error is now returned to the SDK, which fails the
  task, as the `AgentExecutor` contract requires. `Execute` is split
  into the relay, the persist step and the final status; the zero
  `Executor` is usable. (#9)
- **Breaking**: `front/a2a.MetaPendingCalls` carries the pending call
  IDs as a JSON array of strings (a `[]any` in process, the only form
  the SDK stores) instead of an in-process `int` count. `AgentCard`
  takes a context and a version, and advertises the tools of a
  `ToolProvider` as skills. (#10)
- **Breaking**: `Agent.Prompt`, `Continue` and `Resume` return the
  `*RunEnd` beside the error, so a caller can tell done from stopped
  from paused without a second call. The error is set only when the
  run could not start or ended with `ReasonError`; an abort comes back
  as a `RunEnd` with `ReasonAborted` and the context error on it, and
  `ErrAborted` is gone. `Resume` with nothing pending returns
  `ErrNotPending`, `Steer` and `FollowUp` take any number of items,
  and the zero `Agent` is idle for `WaitForIdle`. (#11)
- **Breaking**: `Run` and `Continue` yield `iter.Seq[Event]`. The
  `RunEnd` is the single terminal: always the last event, with the
  failure on `Err` when `Reason` is `ReasonError`. A run refused before
  it starts is one `RunEnd` with `ReasonError` and no other event. The
  loop runs ahead of the consumer by up to `EventBuffer` events.
  `CanContinue` is exported. (#12)
- **Breaking**: `ToolDecision` has an `Action` of `Allow`, `Block` or
  `Defer` instead of two booleans that could both be set. `Terminate`
  and `Args` are unchanged. (#13)
- **Breaking**: names aligned across sibling packages: `tools/agent`
  constructs with `New` like every other package; `tools/a2a.Details`
  is `TaskInfo`, the counterpart of `ChildInfo`; both packages'
  `InputRequiredError` match `agentturn.ErrInputRequired` under
  `errors.Is`; the execution modes are `ExecParallel` and
  `ExecSequential` so `Sequential` no longer collides with
  `agenttool.Sequential`; `front/a2a.Text` is unexported; `Filter`
  returns `Transcript` like `Transform`, and `doc.go` states the rule
  for `Transcript` versus `openresponses.Items`. (#14)
- **Breaking**: the loop attaches its working transcript to the context
  of every hook and tool call, and `agentturn.TranscriptFromContext`
  reads it. `tools/agent.WithTranscript` uses it, so a host no longer
  has to remember `WithParentTranscript`, which is gone with
  `ParentTranscript`. `WithArgs`, `WithStrictArgs` and `New` document
  when they panic; `WithStrictArgs` reflects the schema once; `Execute`
  states that `ChildInfo` rides on every error path. (#15)
- `Agent.SetConfig` and `Agent.SetTranscript` change a live agent
  between runs, refusing with `ErrRunning` during one. `SetTranscript`
  derives the pending calls from the new transcript, so a branch switch
  pairs with `agentsession.Session.Branch` on the store side. (#20)
- README: the first-contact example handles the `RunEnd` and
  cancellation, a print front keyed on call ID, and the
  `input_required` round trip through `Defer` and `Resume`. (#6, #19)
- `compact.NewLocal` folds the transcript through an ordinary model
  call for servers without a compaction endpoint: the older items and
  a summary prompt go to the model, the summary comes back as a user
  message in front of the kept tail, a later fold starts from the
  previous summary, and the prefix cache and `Last` work as for `New`.
  `WithSummaryPrompt` and `WithSummaryItem` adjust it. **Breaking**:
  `Transform.Last` returns an `openresponses.Item`, and `WithFilter`
  takes a `Transcript` to `Transcript` function. The transform no
  longer holds its lock across the model call, and an unhashable
  prefix is no longer remembered as matching everything. (#21)
- Depends on `agenttool` v0.0.2 and `agentsession` v0.0.2.

## v0.0.2 - 2026-09-19

- One version per repository. Every nested module's `go.mod` requires
  the released root, and `front/a2a` for `tools/a2a`, next to a
  `replace` that builds against the tree, so `go get` works for
  consumers and the checkout needs no workspace. `make release
  VERSION=` sets the requirements, dates the changelog, and tags the
  root and every nested module at one commit; the release workflow
  publishes nested tags too.

## v0.0.1 - 2026-09-19

First release. The root module is the loop, `front/responses`,
`compact` and `tools/agent`. The nested modules `front/a2a`,
`tools/a2a` and `session` are not tagged separately yet: they build
against the root through a local `replace`, so a consumer needs the
repository checked out beside their own until a later release tags
them.

- `make check` now includes `tidy-check`, which fails when `go mod tidy`
  would change any module's `go.mod` or `go.sum`; CI uses the same target.
- **Breaking**: the tool contract moved to its own module,
  `github.com/ChristopherDavenport/agenttool`, together with the MCP
  adapters: `agentturn/tool` is now `agenttool`, `front/mcp` is
  `agenttool/mcpserver` and `tools/mcp` is `agenttool/mcpclient`.
  `Config.Tools` is `[]agenttool.Tool`. The self-description helper
  `ServerFor` did not move; build the server with
  `mcpserver.NewServer(cfg.Name, version, cfg.Tools...)`. `make interop`
  moved with the adapters.

- `Config.Request` is the base of every request the loop sends, so
  `tool_choice`, `parallel_tool_calls`, `max_output_tokens`,
  `temperature`, `truncation`, `include`, `safety_identifier`,
  `prompt_cache_key` and the rest reach the model; the loop owns input,
  tools, store, stream and previous_response_id. `Config.BeforeModelCall`
  sees the built request before it is sent. `Config.BaseRequest` exposes
  the starting point, which the session recorder uses for the initial
  config entry.
- `front/a2a` builds its input-required boundary on `ToolDecision.Defer`
  and `RunEnd.Pending` instead of stub tools and a stop hook. In a
  mixed batch the agent's own tools run and only the caller's calls are
  handed back. A follow-up that does not answer exactly the pending
  calls leaves the task input-required with the calls repeated and the
  rule stated, rather than failing it.
- `front/responses` expresses a deferred run the way Open Responses can:
  the response completes with the pending function_call items last in
  its output, and the caller sends the outputs back with the
  conversation. `tools/agent` returns an `*agent.InputRequiredError`
  when the child deferred calls, with the pending calls on `ChildInfo`
  so a host can continue the child.
- `tool.New` validates arguments against the reflected schema before
  decoding: missing required properties, wrong types, values outside an
  enum, and unexpected properties under a strict schema are returned as
  a `*tool.ValidationError` the model can retry on. `tool.Reflect`
  returns the schema tree and `Schema.Validate` / `ValidateJSON` check a
  value against it; `WithoutValidation` opts out.
- Pause and resume: `ToolDecision.Defer` hands a call to the caller. The
  other calls of the batch run, no output is appended for the deferred
  one, `tool_end` reports `Deferred`, and the run ends with
  `ReasonInputRequired` and `RunEnd.Pending`. `Agent.Resume` appends the
  outputs and continues; `Prompt` and `Continue` return
  `ErrInputRequired` while calls are pending. With the low-level loop,
  append the outputs and call `Continue`.

- `tool`: the tool contract (`Tool`, `Call`, `Result`, `Sequential`,
  `Strict`), `tool.New` for typed tools with a standard-library JSON
  Schema generator and strict mode, `Func` for untyped tools, `Set`,
  and `Executor` for parallel and sequential batches with progress.
- Root package: `Run` and `Continue` over any `openresponses.Streamer`,
  the event stream (`run_start` through `run_end`), request
  construction with `store: false` and inlined history, `Filter` and
  `Transform`, and the `BeforeToolCall`, `AfterToolCall` and
  `ShouldStopAfterTurn` hooks with block, rewrite, override and
  terminate semantics.
- `Agent`: queues (`Steer`, `FollowUp`), subscribers with barrier
  delivery, `Abort`, `WaitForIdle` and `State`.
- `front/responses`: the loop as an `openresponses.Adapter`. A request
  with function tools runs one turn and hands the calls back; a request
  without runs the agent to completion. `WithToolItems` includes the
  executed calls in the output; `Compact` delegates to the model.
- `compact`: the reference `Transform`. Over a token budget it calls the
  model's `Compact`, splices the compaction in front of the recent
  tail, and caches by prefix so repeated turns cost nothing.
- `tools/agent`: an agent as a tool. A child run on a fresh or seeded
  transcript, progress through `Call.OnUpdate`, `ChildInfo` in
  `Result.Details`, abort through the context, unbounded nesting.
- `tools/mcp` and `front/mcp` (nested modules on
  `modelcontextprotocol/go-sdk` v1.8.0): remote MCP tools as `Tool`
  values with prefixing, content and error mapping, progress and
  list-changed refresh; and Go tools served over MCP with the inverse
  mapping.
- `front/a2a` and `tools/a2a` (nested modules on `a2a-go` v0.3.15): the
  A2A executor with a conversation store keyed by context ID, coalesced
  artifact chunks, caller-owned tools through input-required and a
  cancel registry; and a remote A2A agent as a `Tool`.
- `front/mcp` validates call arguments against the tool's schema before
  running it, as the reference servers do, so a missing required
  property or a wrong type is refused with `isError`. Schemas in drafts
  the validator does not support are served without validation.
- `session` (nested module on `agentsession` v0.0.1): a `Recorder`
  that subscribes an `Agent` to a store. Items are appended on
  `item_end`, the response entry with its request hash on
  `response_end`, config entries as the settings change, app-only items
  as custom entries, and a tools/agent child run as a linked subsession.
  The module carries its own request hash, tested against
  agentsession's golden vectors.
- New `response_end` event: the folded response with usage, delivered
  after its items and before any tool of the turn runs. `item_start`,
  `item_update` and `item_end` carry the response ID of a streamed item.
- `make interop` runs the MCP adapters against the upstream reference
  server and the MCP Inspector over stdio; `front/mcp/examples/stdio` is
  the server it drives.
- `Makefile` and CI mirror `openresponses`: `make check` runs gofmt,
  vet, the dependency boundary, staticcheck, govulncheck and race tests
  across every module.

