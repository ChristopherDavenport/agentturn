# Changelog

All user-visible changes to this library. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
uses [Semantic Versioning](https://semver.org/); before v1.0.0 minor
versions may break the API.

## v0.0.8 - 2026-09-23

- **Fixed**: a fold that pinned items, `compact.WithPin`, is written to
  the compaction entry's `pinned` member, which the context algorithm
  places after the summary as the request carries it. The recorder put
  them in the `fold` extension member instead, which `BuildContext`
  never reads, so the context rebuilt from the record was missing them
  and the recorder declined a request hash it could not stand behind.
  The calls after such a fold now keep their hashes,
  `Session.RequestContext` rebuilds the input that was sent, and the
  pinned items are in the context `Resume` and `Continue` seed from, so
  the pin predicate still has something to match after a restart. The
  v0.0.7 entry below says those calls are recorded without a hash; that
  was the behaviour until this release. (#78)

  **Breaking, in the fold member.** `session.FoldCall.Pinned` is gone:
  the pinned items are the format's now, and the member names the
  fold's own model call and nothing else. A session written by v0.0.7
  with a pinned fold still carries them under `fold.pinned`, still has
  no request hashes on the calls after the fold, and is not repaired by
  reading it with this release — nothing about that file is wrong, it
  is short the member the context algorithm reads.

- **Breaking, in what is recorded.** A run whose segment holds no
  response of its own is written as `stopped` only when it answered a
  call an earlier run's model call made and left no call on the path
  without an output, and as `aborted` otherwise. v0.0.7 wrote `stopped`
  for every responseless segment, which disagrees with
  `agentsession.ComputeReason` for a run that answered nothing or that
  answered only part of what was pending, and so failed `Run.Verify`.
  The loop cannot produce either of those shapes — `Resume` refuses a
  partial answer — so this reaches a host driving `agentturn.Run`
  itself through the public `Recorder.Handle`. (#78)

## v0.0.7 - 2026-09-23

- Requires `agentsession` v0.0.7, `agenttool` v0.0.7 and `openresponses`
  v0.0.12, up from v0.0.5, v0.0.5 and v0.0.9.
- **Breaking, in what is recorded.** `session` writes `stopped` rather
  than `aborted` for a run whose segment has no response of its own — a
  resume whose approved batch terminated, or a refusal on Resume.
  agentsession v0.0.6 took those shapes out of the cascade's `aborted`
  step, so `stopped` is now what the segment reads and what
  `Run.Verify` accepts; writing `aborted` was the accommodation the old
  cascade forced, and it now fails verification.

  **Sessions written before v0.0.7 are affected, and they are not
  corrupt.** One of them that holds a terminating resume or a refusal
  on resume carries `"reason": "aborted"` on that run entry, which was
  the right value under RFC 0001 draft 0.2. Draft 0.3 moved the answer,
  so reading the same file with `agentsession` v0.0.6 or later fails
  `Run.Verify`, `VerifyRecords` and the `agentsession verify` CLI with
  *run end disagrees with its segment: … wrote aborted, segment reads
  stopped*. Nothing else about the file changed and no item, response
  or hash is wrong; it is the reader's rule that moved under it.
  Sessions written from v0.0.7 on carry `stopped` and verify. There is
  no migration for the old ones: rewriting the run entry's reason in
  place is the only fix, and whether that is worth doing is a judgement
  about the archive, not about the file. (#78)
- The `session` tests accept `agentsession.ErrNoHash` from `Verify`,
  which agentsession v0.0.6 added for a response that recorded no
  request hash. The recorder legitimately writes none whenever a layer
  edits the request outside the transcript, which is the case several
  of those tests construct.

- `Queued` reports an item `Agent.Steer` or `Agent.FollowUp` accepted
  into a queue, with which queue it went into and the run that was in
  flight, so a host writing what it accepted can tell an item it was
  handed from one a run produced, where before the two were the same
  user message with nothing to separate them. The report is not the
  accept: the item is queued when the call returns, and the goroutine
  that owns delivery reports it at its next event, before anything that
  item produces. Steer and FollowUp keep their signatures and never
  wait on delivery, so steering from inside a subscriber is safe, and a
  host that must not lose an input writes it before it accepts it. The
  queues, `State.Steered` and `State.Queued` are unchanged. (#67)

- `compact.WithPin(fn)` keeps the items fn reports through a fold:
  whatever part of the folded prefix they were in, they follow the
  summary in the request, in their order, so a stream rule's reminder
  or a policy notice survives compaction as itself rather than as
  whatever the summary model made of it. `WithKeepLast` keeps a window
  at the end and nothing kept a member of the part that is folded.
  `Fold.Pinned` reports them and `session` names them in the
  compaction entry's `fold` member; because a compaction entry says
  only where the kept tail starts, the calls after a fold that pinned
  anything are recorded without a request hash rather than with one
  that would not verify. (#78)

- `Retry.Revise` may change the request the next attempt sends: another
  model, a lower effort. `ModelRetry` carries that request, and
  `session` settles on it, so a fallback chain lives in the loop rather
  than under it as a `Streamer` over its legs, where no event described
  the switch and the config on the path named the model that did not
  answer. The retry's settings replace the failed attempt's rather than
  adding to them, since an attempt that never answered wrote nothing.
  (#77)

- `agentturn.Invoke(ctx, name, args)` runs one of the turn's tools from
  inside another as if the model had asked for it under the call in
  flight: `BeforeToolCall` decides, `tool_start` and `tool_end` carry
  the new `Parent` naming the call that made it, `AfterToolCall`
  applies, and `session` writes a custom entry in the
  `agentturn:nested_call` namespace before and after, since a nested
  call has no function_call item for a dispatch to name. A tool that
  let its code reach the agent's other tools through an `agenttool.Set`
  of its own ran them past the policy, the events and the record
  alike. A nested call appends nothing to the transcript, and a hook
  that defers one refuses it, since there is nobody to ask while a tool
  is running. Delivery is serialised, so a subscriber is never called
  from two goroutines at once. (#76)

- **Fixed**: `Retry` committed an attempt as soon as any output item
  opened, and a reasoning model opens its reasoning item before its
  text, so a 503 between the summary and the first token was final and
  left a transcript ending in a reasoning item, which `Continue`
  refuses and a strict server rejects. An attempt commits when a
  message or a function call opens; an item completed before that is
  held, reaching subscribers as item_start and item_update so a front
  still renders thinking live, and is appended when the attempt commits
  or dropped when it ends without committing. The transcript never ends
  in a bare reasoning item. (#71)

- `Agent.AbortCause(err)` cuts a run with a reason, and the loop reads
  `context.Cause` wherever it read the bare context error, so a host
  that cancels the run's context itself with `context.WithCancelCause`
  is heard too. `RunEnd.Err` carries the cause and a recorder writes it
  as the run's end, so a harness that cuts a stream for a rule, an
  advisor, a coordinator or a user pressing Esc can count them apart
  instead of recording four "context canceled". `Abort` is
  `AbortCause(nil)` and keeps its meaning. (#70)

- `ChainBeforeModelCall`, `ChainBeforeTurn`, `ChainShouldStopAfterTurn`,
  `ChainBeforeToolCall` and `ChainOutputGuard` join several values for
  one hook into one. Every hook is a plain field, so a product that
  follows two layers' READMEs in turn keeps the second assignment and
  loses the first with no error and no sign, which is how a memory
  block silently freezes at the value it had when the process started.
  The semantics are stated: in order, the first error stops the chain,
  the first stop ends the run, BeforeTurn concatenates, BeforeToolCall
  folds deny over ask over allow with the first reason of an action
  kept, and each output guard sees what the one before it left.
  `Config`'s doc comments name the layers that contest each field. (#64)

- `tools/agent`: the child runs as an `agentturn.Agent` rather than
  inside the low-level `Run`, and `WithSpawn(fn)` hands it to the host,
  keyed by the call, before the run starts: a host can steer a running
  child, abort one without cutting its siblings or the parent, and
  prompt it again once the tool has returned, which is what a hub that
  fans work out to subagents does. Nothing an observer sees changes,
  except that every event is now a barrier, as it is for the parent, so
  a child's dispatch is durable before its tool runs. The observer
  stays subscribed after the call, so a later run the host starts is
  recorded into the same child session. (#72)
- `tools/agent`: `WithRunContext(fn)` gives the child run a context of
  the host's making, derived from the call's; `ContextWithConfig` and
  `ContextWithRetry` are exported for a host that runs a child agent
  itself and observes it with the same function. (#72, #73)
- **Fixed**: a child session was filed under no working directory, so
  every child landed in a store's `default` bucket and no listing
  scoped to a directory ever showed one, although the child ran in its
  parent's process and its parent's directory. The child header
  inherits the parent's `CWD`. (#66)
- `session`: `Recorder.ChildContext`, for `agent.WithRunContext`, puts
  the ID of the session a child run will be written to on the context
  the child is given, and `SessionIDFromContext` reads it, so a layer
  that attributes its writes to a session, a memory journal for one,
  names the child's session rather than the parent's.
  `ContextWithSessionID` is the same for a host's own runs. (#63)
- **Fixed**: a second run under one call always reset the child
  session's leaf, which a revived subagent is not: it answers from its
  own context, and the new root rebuilt one item of the seven its
  request carried, with no hash. A second run continues the session at
  its leaf; a host that means a retry from a clean start says so with
  `agent.ContextWithRetry`. (#73)

- `Answer.By`, set with `Answer.WithBy`, names who decided an answer to
  a pending call, in the session format's terms: `human` for a person
  at a prompt, `policy` for a rule that answered on its own, `agent`
  for another model. It rides on the `ToolDecision` the loop
  synthesises for an approval and, for an output the caller wrote,
  which raises no tool_start, on the run's context, where the recorder
  finds it with `agentturn.DeciderFromContext`; `Agent.Resume` attaches
  it and `ContextWithDeciders` is there for a host driving the
  low-level `Run`. There is no default: a policy engine answers through
  Resume as often as a person does, so an answer that names nobody is
  recorded as an anonymous decision rather than guessed at. (#65)
- `agentturn.Hidden(item)` marks an item as part of the model's context
  that a renderer should hide: a stream rule's interrupt report, an
  advisory, a notice a keyword added. It is a wrapper the loop strips
  as it appends, so the transcript, the request and every type switch
  see the item itself; `ItemStart.Hidden` and `ItemEnd.Hidden` carry
  the mark, and `session` writes the entry with the format's `visible`
  false. `Unhide` unwraps one. It works wherever the loop appends a
  caller's item: `Prompt`, `Steer`, `FollowUp`, the prompts of `Run`
  and what `BeforeTurn` returns. (#75)
- **Fixed**: `session` recorded a held call without the reason the hook
  gave, although the reject arm two lines away wrote one, so a session
  said three calls were held and nothing said which rule raised the
  prompt. A hold carries the decision's reason; the model still sees
  nothing for a deferred call. `ToolDecision.Reason` documents that it
  is the reason of a decision whatever the action, not only the message
  a block shows the model. (#62)

- **Fixed**: `session` wrote the reject decision that carries an output
  the caller supplied only for a call it was holding, so a call seeded
  from a branch whose dispatch and hold are on the path it left ended
  with an output and no dispatch, which `VerifyRecords` reads as a
  broken promise. The decision is written for any call the path holds
  no dispatch and no reject for, which is what a reader takes an output
  the caller wrote to mean; a held call is unchanged. (#68)
- **Fixed**: `session` wrote a stream cut after a completed item as a
  failed response with no response ID, so the context algorithm found
  no items to strip and read the items the cut call produced as its
  input, and the recorded hash mismatched. Every interrupted run of a
  model that completes an item before the cut, a reasoning model for
  one, was a `verify` failure. The failed response carries the ID of
  the response the stream named, learned from every item event it
  raised, so an interrupted turn names the response it was cut out of
  whether or not the attempt ever appended anything. (#69)
- `session`: `Recorder.Rebase` with the empty entry ID is the reset a
  product's `/clear` makes: the session's leaf is reset so the next
  append starts a new root, and the recorder forgets the settings, the
  items and the calls of the branch it left, so the new root opens with
  a full config entry and its responses carry hashes again. Before
  this, a reset leaf could not be told to the recorder at all, and the
  root it wrote rebuilt with no model and no instructions. (#74)

## v0.0.6 - 2026-09-20

- Depends on `agenttool` v0.0.5, and `session` writes a tool's side
  data: a `Result.Details` value that implements `agenttool.Recordable`
  is written on `tool_end` as a custom entry in the namespace it names,
  between the call's dispatch and its output, so a tool keeps what its
  output does not carry, the full bytes of a truncated result for one,
  without the recorder knowing its type. (#53, the library half)
- Depends on `agentsession` v0.0.5, the release that implements RFC
  0001 draft 0.2, and `session` writes the record entries the draft
  added, so a session written by this recorder is resumable from the
  file alone: which calls reached their tools, which are held on a
  decision, why each run started and how it ended. `agentsession
  verify` passes, record checks included, on every session the test
  suite writes. (#31, #34, #44, #48, #50, #38, #47, #52)
- `session`: `Start` promises `run`, `dispatch` and `decision` in the
  header's `records` unless the caller set them. Every run is
  bracketed by a `run` entry: the start carries the loop's source,
  input or resume, and the `Trigger` on the context as `ref`; the end
  carries the reason in the format's terms and the run's calls left
  without an output. Every call handed to its tool gets a `dispatch`
  on `tool_start`, durable before the tool runs under the barrier. A
  blocked call is a `reject` decision with its reason; a deferred call
  a `hold`; an approval of a held call, or a hook that rewrote the
  arguments, a `proceed` carrying the arguments the tool ran with, so
  the record says what ran while the function_call item stays as the
  model wrote it; an output the caller wrote for a held call is
  preceded by the `reject` it is. `Resume` seeds the pending calls
  from the path so their records anchor after a restart. (#31, #34,
  #44)
- `session`: a child session's ID is derived from the parent's and the
  call's as the format recommends, its header names the call in
  `spawned_by` and promises the same records, the link from the parent
  is written when the child starts, at dispatch, and a retry of the
  same call continues the child session from a new root. tools/agent
  puts the call on the observer's context for this.
- `session`: `WithEnv` takes a function the recorder calls once per
  run for the environment, written as an `env` entry when it differs
  from the last one written or found on the path by `Resume`. The
  recorder gathers nothing itself. (#52)
- `session`: a call `BeforeModelCall` refused is recorded as a failed
  response carrying the hook's error and the hash of the request that
  was built, distinct from a call that was made and failed, through
  the new `ModelBlocked` event. (#38)
- `session`: a compaction entry carries a `fold` member naming the
  fold's own model call by response ID, model and request hash, so a
  replay can recognise the fold's call; `compact.Fold` carries the
  fold's `Request` and `ResponseID`. The hash is of the fold's request,
  which no path rebuilds, and is never written as a response. (#47)
- **Breaking**: `RunEnd.Pending`, `State.Pending` and
  `agent.ChildInfo.Pending` are `[]PendingCall`, each call with why it
  has no output: `PendingDeferred`, `PendingAborted` for a call cut off
  in flight, whose tool may have run, and `PendingUnknown` for a call
  found unanswered in a seeded transcript. `PendingCalls` returns the
  calls alone. (#34)
- **Fixed**: an abort inside a tool batch discarded the outputs of the
  calls that had finished, so on resume a call that ran was
  indistinguishable from one that never did. The outputs of the calls
  that finished are appended, in the batch's order, before the run
  ends; only the calls the abort cut off are pending. (#48)
- **Fixed**: a message appended after an unanswered function call hid
  it from the pending set, so `Continue` and `Prompt` accepted a
  transcript a strict server rejects. A call with no output anywhere
  later in the transcript is pending, whatever messages intervene.
  (#50)
- `session`: `Recorder.Rebase` moves the session's leaf and reseeds the
  recorder from the context there, so a branch with a live recorder
  records the deltas and the fold alignment of the branch it continues
  rather than the one it left; it appends nothing and refuses while a
  run is active with `ErrRunActive`. (#49)
- `session`: a recorder attached to an agent takes the agent's
  configuration again at every run, so `Agent.SetConfig` reaches the
  record as a config delta and as the filter in force; `WithFilter`
  serves `Handle` without an agent and a configuration whose Filter is
  nil. (#41)
- `session`: `Continue` rolls a session over into a successor through
  `agentsession.Continue` and returns a recorder seeded from it. The
  package documents that a link is never a context edge: a subsession
  is self-contained, a change of settings within a conversation is a
  config entry, and a rollover is a successor. (#40)
- `session`: `Recorder.Annotate` appends a custom entry at the current
  leaf of the session of the run on the context, and one made from a
  subscriber during turn_start lands before that turn's config entry
  whatever the registration order: the recorder holds the settle it
  computed on turn_start until the turn's next event. (#51)
- `session`: a response carries a request hash only when the recorder
  can stand behind it, when the request's input is what the stored
  path rebuilds from the items it wrote, the fold it last recorded and
  the filter in force. A request a Transform or a BeforeModelCall
  changed in a way the record does not describe, or that carries items
  the recorder never wrote, is written without a hash, and
  `Session.Verify` reports it as unverified rather than as a mismatch.
  (#35, the recorder half)
- `RunEnd.Cause` says what stopped a run with `ReasonStopped`:
  `StopMaxTurns`, `StopHook`, `StopGuard`, `StopTerminate`,
  `StopPartialTerminate` or `StopRefused`; the recorder writes it as
  the run end's `ref`. A `ShouldStopAfterTurn` hook that returns an
  error wrapping `ErrGuard` ends the run as a policy stop, with the
  error on `RunEnd.Err`, rather than as a failure, and `TurnInfo.Final`
  says the turn called no tools. (#37)
- **Breaking**: a batch in which some results set `Terminate` and
  others do not now ends the run, with `StopPartialTerminate`, instead
  of continuing as if nothing had asked to stop; the other calls still
  run and their outputs still land. A batch whose every result
  terminates stops as before, with `StopTerminate`. (#36)
- `Config.OutputGuard` runs on each assistant message as the stream
  completes it, before it is appended, delivered as item_end or
  recorded, and may replace it with a placeholder; function calls and
  every other output item never reach it. (#37)
- `Config.BeforeTurn` returns items the loop appends to the transcript
  at the start of each turn, with their item events, so per-turn
  context is a fact about the path and a recorded session rebuilds the
  request it was part of. (#35, the loop half)
- `ToolCallInfo` carries `Batch`, every call of the turn in the model's
  order, and `Index`, so a policy that defers one call can hold the
  rest of the batch. (#42)
- `Refuse` answers a pending call and ends the run with `StopRefused`
  instead of calling the model, for a refusal that should end the turn;
  `Answer.Terminate` is behind it. The outputs are still appended. (#43)
- `Answer.Note` and `ToolDecision.Note` carry what the user or the
  policy said with the result: appended after the batch's outputs as a
  user or a developer message, so the model reads the result and the
  note together in the same turn; `Answer.WithNote` attaches one. The
  steer queue is drained after an approved batch, as after any batch.
  (#45)
- **Fixed**: a tool list from `ToolProvider` skipped the duplicate-name
  check that `Config.Tools` gets before a run. The provided list is
  validated once per turn and a bad one fails the turn naming it. (#46)
- **Fixed**: the low-level `Run` and `Continue` accepted a transcript
  with an unanswered function call, which `Agent.Prompt` refuses; they
  now fail with `ErrInputRequired` unless the prompts' leading outputs
  answer it. (#39)
- `tools/agent`: a child that ends without a final assistant message
  no longer returns an empty output. By default the parent's model sees
  an error naming the cause, or the last tool output when a terminating
  tool answered on the child's behalf; `WithNoAnswer` sets what it sees.
  `ChildInfo.Cause` carries the child's stop cause. (#30)
- `tools/agent`: the snapshot a `WithTranscript` seed receives has every
  in-flight call of the batch answered with a placeholder output, the
  child's own naming the agent, so a seed that keeps the conversation
  hands the child a valid input; the parent's snapshot is unchanged.
  `New` panics on a `Config.Name` a provider rejects as a tool name
  unless `WithToolName` gives one. (#39)
- `State.Steered` and `State.Queued` are the queued items themselves,
  copies of the steer and follow-up queues, so a host that promised a
  sender it has an item can persist it and queue it again after a
  restart. `Steer`, `FollowUp`, `Abort`, `SetConfig` and
  `SetTranscript` document that the queues live in memory and survive
  everything but the process. (#32)
- `TurnStart.Inputs` names the items appended since the previous turn's
  response, or since the run started, so a front routes a response to
  the messages it answers without counting item events between turns.
  (#33)
- `AfterToolCall` documents that an override is invisible to every
  subscriber and recorder, so a cap on tool output belongs in the tool
  or in a Transform, not there. (#53)
- `Steer` and `FollowUp` document that a subscriber steering in
  reaction to an event its own item produces feeds the run forever, and
  where to steer from instead. (#54)
- `RunStart` carries `Source`, input or resume, and the `Trigger` a
  caller attached to the context with `ContextWithTrigger`; the loop
  learns nothing from it. (#31)
- `ToolStart` carries the `ToolDecision` a hook returned for the call,
  or the caller's arguments for an approval, and `ToolDecision.By`
  names who decided, for the record. (#44)
- `ModelBlocked` is emitted when `BeforeModelCall` refuses a request,
  with the request as built and the hook's error, in place of the
  turn_start the call would have had. (#38)

## v0.0.5 - 2026-09-19

- **Fixed**: an abort delivered every event after it, the `tool_end` of
  the cut-off call included, through the cancelled context, so a
  session recorder's child session and link were lost and a subscriber
  error during the abort vanished into `ReasonAborted`. `Agent` now
  delivers every event with the cancellation lifted, as it did for
  `run_end` alone; a subscriber that fails for a reason of its own
  during an abort has its error wrapped with the context error on
  `RunEnd.Err`; a call cut off during preflight gets its `tool_end`
  like one cut off while running; the low-level `Run` no longer drops
  events at random once its context is cancelled; and `tools/agent`
  calls its observer with the cancellation lifted too. (#23)
- `tools/agent` + `session`: a child run is recorded as a full session
  written live. `agent.WithObserver(rec.Observe)` creates the child's
  session on its `run_start`, with `parent_session` naming the session
  of the run that called the tool, writes its configuration, items,
  responses and folds as they happen, and the parent's `tool_end`
  writes the link. A child of a child is linked from the child's
  session through the same observer. The loop attaches its run ID to
  the context of everything a run calls, read with
  `agentturn.RunIDFromContext`, and `agent.ConfigFromContext` gives the
  observer the child's configuration. A child whose run was not
  observed is still written from `ChildInfo.Items`, as before. (#24)
- **Breaking**: `Agent.Resume` takes `Answer` values, built with
  `Output`, `Approve` or `ApproveWith`, so an approved deferred call
  runs inside the loop: `BeforeToolCall` is skipped, the tool events
  fire with turn 0, `Sequential` and `MaxParallelTools` apply,
  `AfterToolCall` runs, and the outputs are appended before the model
  is called. `Resume(ctx, agentturn.Output(out))` is the old call. (#25)
- `Agent.Prompt` accepts a `function_call_output` for each pending call
  ahead of the message, appending them through the event stream, so a
  front whose user has moved on after an abort answers the cut-off
  calls in the same model call as the next prompt. A leading output for
  a call that is not pending is `ErrNotPending`. (#26)
- `compact.WithOnFold` reports every fold, applied or failed, with the
  index at which the transcript was split, the output, the summary
  item, the token estimate and the usage; a reporter's error fails the
  turn. `session.Recorder.Fold` is the reporter: it writes the
  compaction entry naming the entry of the first kept item, or a custom
  entry in `agentturn:compaction_failed` for a fold that failed or was
  aborted, so the record shows the attempt. `session.Resume` seeds the
  item alignment from the context at the leaf. (#27)
- `Config.ToolProvider` documents that it is read once per turn, before
  the model call, that a snapshot provider offers a change still in
  flight one turn late, and how to wait for it with a bound. (#28)
- `session`: a tool list change is recorded as `tools_added` and
  `tools_removed` rather than a full replace, unless the delta would
  not replay the request's tool order or would be larger than the
  replacement. (#29)
- Depends on `agenttool` v0.0.4 and `agentsession` v0.0.4.

## v0.0.4 - 2026-09-19

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
- Loop readability: the tool batch is `preflightAll`, `execute` and
  `collect`, the stream loop body is `streamEvent`, the per-call state
  is `callState`, and every error the package produces begins with
  `agentturn:`. `Agent` delivers `run_end` with a context that is not
  cancelled, even after `Abort`, so a recorder can write on it. The
  doc no longer claims a panic in a hook produces a `RunEnd`;
  `Config.Reasoning`, `Config.Text` and `TurnInfo.Transcript` are
  documented. (#16)
- `session`: `mu` documents what it guards, the RFC 8785 canonical
  form is `canonicalJSON` and distinct from `Canonical`, the number
  formatter is tested against the RFC 8785 Appendix B values and
  fuzzed for round-tripping, member names are UTF-16 encoded once per
  object, and the dead response counter is gone. (#17)
- A2A: `%w` chains are kept through `ErrInvalidParams`, the duplicated
  part conversions say which counterpart to change with them,
  `answeredCalls` backs both `unanswered` and `stripUnanswered`, an
  artifact replace no longer aliases the SDK's parts slice, progress
  text grows chunk by chunk instead of re-joining every artifact, data
  part errors say what failed to encode, and the `tools/a2a` argument
  schema is reflected once at init. **Breaking**: `front/a2a`'s
  `NewMemoryStore` and `MemoryStore.Len` are removed; the zero
  `MemoryStore` is ready to use. (#18)
- `Config.Retry` retries a model call that failed before delivering
  anything, inside the turn: `MaxAttempts`, `Backoff` and `Retryable`,
  with defaults that retry 408, 409, 429, 5xx, truncated streams and
  transport failures, honour `Retry-After`, and double from 500ms to a
  30s cap. A `model_retry` event reports each retry; `turn_start` and
  `response_end` are delivered once per turn. An attempt that already
  delivered an item is final, and `Abort` cuts a delay short. The
  plan's non-goal on retries is amended. (#22)
- Depends on `agenttool` v0.0.3 and `agentsession` v0.0.3. A tool
  that panics now ends its call with an `agenttool.PanicError`: the
  model sees one line as the error output and `ToolEnd.Err` carries
  the stack. The A2A front builds its caller-tool stubs with
  `agenttool.NewFunc`, and the session recorder writes config extras
  through `ConfigEntry.SetExtra` and `ClearExtra`.

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

