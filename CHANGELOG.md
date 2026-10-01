# Changelog

All user-visible changes to this library. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
uses [Semantic Versioning](https://semver.org/); before v1.0.0 minor
versions may break the API.

## Unreleased

- Requires `agenttool` v0.0.12, up from v0.0.11, and `agentsession`
  v0.0.16, up from v0.0.15. The format is unchanged. A `cas` store a
  session recorder writes through is migrated to v0.0.16's per-session
  log on its first writing open, after which agentsession v0.0.15 and
  earlier cannot read it; keep a copy of the store if rolling back may
  be needed.
- A failed fold reports what its model calls did, as a successful one
  does: `compact.Fold` carries the last attempt's `Request`,
  `ResponseID` and `Usage` when the fold failed too, and gains
  `OutputTypes`, the item types the last call answered, and
  `Attempts`, the number of calls. `session.FailedFold` writes them as
  `attempts`, `request_hash`, `response_id`, `model`, `usage` and
  `output_types`, all omitted when empty, naming the request by hash
  and model as a compaction entry's `fold` member does rather than
  keeping the folded prefix again. A record of a fold that failed now
  says whether the model thought and answered nothing, called a tool,
  or never answered, without a replay. The format is unchanged. (#177)
- `compact.NewLocal` no longer applies a summary that is not smaller
  than what it folds. A summary whose estimate is not below that of
  the items it replaces is asked once more, as a summary with no text
  is, and a second oversized one fails the fold with
  `compact.ErrSummaryTooLarge` (`compact: summary is larger than what
  it folds`) rather than growing the request the fold was meant to
  shrink. The summary request's `MaxOutputTokens` defaults to half the
  budget, set before `WithRequest` runs so a caller can change or
  clear it, so a runaway summary is cut by the server rather than paid
  for. (#178)
- **An `OutputGuard` error wrapping `ErrGuard` is a guard stop.** The
  message the guard was given is not appended and has no `item_end`,
  and the run ends `ReasonStopped` with `StopGuard` and the error on
  `RunEnd.Err`, as from the other three guard hooks. It failed the
  run, so `front/responses` answered with `server_error` carrying the
  guard's text and `front/a2a` failed the task with it. Both fronts now
  refuse: `front/responses` ends the response incomplete with
  `content_filter`, in a full run and in a single turn, and empties the
  withheld message, whose deltas had gone out, so its done events and
  the response hold none of its text; `front/a2a` rejects the task
  with `RefusedText`. Any other `OutputGuard` error still fails the
  run. `StopGuard`, `ErrGuard`, `OutputGuard` and `ChainOutputGuard`
  say so (#181).
- **`front/a2a` keeps withheld text out of a task's artifacts.** Under
  a configuration with an `OutputGuard` the executor writes a message's
  artifact whole at `item_end`, from the message the guard left, rather
  than streaming its deltas. A task keeps its artifacts, so the text a
  guard replaced was returned by every `tasks/get` for as long as the
  task was stored. Without an `OutputGuard` the text streams as before
  (#179).
- **The receiver's run after a handoff names it.** Both fronts
  continue the configuration `WithHandoff` returns under
  `agentturn.ContextWithTrigger(ctx, agentturn.Trigger{Kind: "handoff",
  Ref: <the sender's Config.Name>})`, so the receiver's `BeforeTurn`
  context and its `run_start`, and so the record, say why it ran, as
  for a host that continues the receiver in process. Under a front it
  read like a user input (#182).
- **`WithStart` in `front/responses` and `front/a2a`** picks the
  configuration a request or a task starts under from the conversation
  so far, the new message last; false keeps the front's own. A handoff
  lasted only for the request or task that made it, and the
  conversation's next message went back to the sender, which saw a
  conversation it had handed off and handed it off again. Neither
  front keeps state between requests: the host finds the last handoff
  in the transcript, which `front/a2a`'s store holds and which
  `front/responses`' caller sends back when the adapter is built with
  `WithToolItems` (#180).
- **A call's ID is decided as it opens.** The loop gives a function
  call its ID at `output_item.added`, the model's when it names no
  other call and one of its own when it is empty or taken, so the
  call's `ItemStart`, every `ItemUpdate` and its `ItemEnd` carry one
  ID, where v0.0.12 renamed the call only when it completed and its
  `ItemStart` and `ItemUpdate` carried the model's. A call whose first
  event is its `output_item.done` is decided then. `ItemEnd.ModelCallID`
  still holds the model's ID for a call the loop renamed. `front/responses`
  no longer changes a call's `call_id` at `output_item.done`: the client
  sees one ID from `output_item.added` through the call's output, and
  a call the model gave no ID no longer opens under an ID the emitter
  minted that no output carries (#184).
- A call whose ID repeats one of a call the attempt holds is renamed.
  A stream that sends no `output_item.added` for its calls keeps them
  out of the transcript until its response arrives, and v0.0.12 checked
  a call's ID against the transcript alone, so two calls `c1` in one
  such response kept one ID and the session recorder failed the run.
  A call's ID is now taken by every call of the attempt, held or not
  (#183).
- **The recorder compares settings by value.** A config delta is
  written when the settings a run sends differ from those in force as
  JSON values, where it compared their encoded bytes: a `cas` store,
  which keeps a body's canonical bytes, hands a tool's `parameters`
  back with their keys sorted, so a recorder resumed from one wrote
  `tools_added` with every tool on its first run in each process. The
  extra members compare the same way (#176).

## v0.0.12 - 2026-09-29

- Requires `agenttool` v0.0.11, up from v0.0.10, and `agentsession`
  v0.0.15, up from v0.0.11. The session recorder writes
  `agentsession/0.9`, and v0.0.11 of agentsession refuses a 0.9 file.
  Under v0.0.15's reading of 0.9 a call ID names one call in the whole
  session, on any branch: `session.CallIDs`, and so `AgentOptions` and
  `Recorder.Rebase`, a rebase to `""` included, reserve every call ID
  in the session rather than those on the path, and
  `Recorder.ChildContext` reserves those of a child session reopened
  under its call. A writer seeded from a path knows every call on it,
  those with an output included, so a decision, a dispatch or a
  second output after a call's output is refused with
  `ErrCallCompleted` rather than written, failing the run, and a
  dispatch for a call the path holds no function call entry for is
  refused, so its tool does not run unrecorded. A function call the
  filter writes as a custom entry takes no decision or dispatch,
  whose `target` must name a function call. The recorder writes a run end's
  `pending` list with the earlier runs' calls the run wrote a decision
  or a dispatch for, so a resume whose BeforeToolCall defers a call
  that never started ends `input_required` with that call pending; a
  block of a call that may have run is an `answer`, where it was a
  `reject`, which the format now refuses after a `dispatch`; and a call
  a `reject` or an `answer` ended gets no further decision.
  `session.Pending` reads a call rejected before its output from
  agentsession's `CallRejected`.
- **A call ID names one call.** A function call the model gives no
  call ID, or one a call in the transcript already has, takes an ID of
  the loop's own when it completes, the model's with a random suffix:
  its `ItemEnd`, the transcript, its output, the response the turn
  runs and every later request carry it, and only its `ItemStart` and
  `ItemUpdate` carry the model's. The model's ID is kept in letters,
  digits, `_` and `-`, the alphabet Anthropic takes, with anything else
  replaced by `_`, and cut so the new ID is at most 64 characters. `ContextWithReservedCallIDs` reserves IDs for the
  runs started with a context, and `Agent.SetTranscript` keeps the IDs
  of the transcript it replaces reserved. A provider that numbered its calls per response had a later
  call's output matched to an earlier call, and agentsession v0.0.13
  refuses the repeated call, which failed the run.
- **The recorder writes the omitted list as keeps.** Under
  `session.WithInstructionsParts`, a config delta that changes
  `instructions_omitted` now writes it as `Settings.OmittedDelta`
  returns it, each run of parts unchanged in the list in force a
  `{"keep":n}`, where it wrote the list whole: under a memory at its
  budget a save or a forget moved one part and rewrote hundreds. A
  replace still carries the list whole, since a keep in a replace
  counts over nothing and `Append` refuses it, and a resumed or folded
  recorder counts from the list the context resolves. A settings
  change is written as a replace only when it is smaller with the
  list whole than a delta with what moved in it. Needs format 0.9.
  (agentsession #115)
- The session recorder writes a tool that joins the list anywhere but
  its end as a config delta rather than a full replace: `tools_added`
  names every tool from the first one out of place, which replay
  removes by name and appends, so an MCP server that lists a new tool
  mid-list costs that tool and the ones after it, not every tool and
  every instruction part. A tool moved to the end is added alone. The
  size check still writes a replace when that is smaller. (#162)
- `compact.WithRequest` edits the summary request `compact.NewLocal`
  sends, after the transform has set its model, input and store. The
  request carried nothing a caller passed but the model name, so a
  thinking model left at its server's default reasoning thought
  through the fold and now and then ended with a function call; a
  caller whose requests set `effort: none` can now say the same for
  the summary. A summary response with no text is asked once more
  before the fold fails with `compact: summary response has no text`,
  and the fold reports the call that answered. (#161)
- A record a child's background job writes after the child's run ended
  is filed in the child's session, under the job's call, rather than at
  the root with no `call_id`: `session.Recorder.Annotate` and
  `RecordFunc` reopen the child session `SessionIDFromContext` names
  when it is one of the recorder's children and no run of it is being
  written. `RunContext`'s doc no longer says the call is absent from
  it: a child run's context carries the call that started the child,
  so a job in a child names its own with `agenttool.WithCall`. (#168)
- Breaking: a guard that stops a run before the agent answers reaches
  a front's caller as a refusal rather than a failure. `front/responses`
  ends the response `incomplete` with `incomplete_details.reason`
  `content_filter`, collected and streamed, where it failed with a 500
  `server_error` a web tier retries; a single turn that
  `BeforeModelCall` refuses as a guard does the same. `front/a2a` ends
  the task `rejected` with the fixed `RefusedText`, where it failed the
  task, which an orchestrator re-plans. Neither sends the guard's error,
  whose text named the rule a caller could phrase around; the host has
  it on `RunEnd.Err` and in the record. (#164)
- `front/responses.WithHandoff` and `front/a2a.WithHandoff` take a
  handoff within the request that made it. A run that stops on
  `StopTerminate` or `StopPartialTerminate` asks the function, given
  the run's end and the `ToolEnd` events of the batch that stopped it,
  whose `Details` name the destination, for the receiver's
  configuration, and the transcript continues under it in
  the same response or task, the receiver's items relayed as the
  sender's were. `front/a2a` sets it on the task's agent and continues
  that agent, so `WithRecorderFor` records the switch and the
  receiver's run, and a `ToolRecorder` or `ToolElicitor` the new
  configuration leaves nil is kept from the agent's. Without the
  option, a terminating stop with no answer completes with the text of
  the last output of a call whose result set `Terminate`, where
  both fronts completed with no answer: an empty response, or a task
  with no message. (#163)
- Breaking: `Agent.Resume` puts an approval of a call pending as
  `PendingUndispatched` to `Config.BeforeToolCall`, since nothing has
  decided it, and a Block or a Defer applies as it would in a run: a
  deferred call ends the run with `input_required`. A call that never
  started used to run with the hook skipped, so `session.ReplayAnswers`
  followed by a resume ran, after a restart, exactly the calls the
  policy had not yet allowed. A call a record refused and stopped
  before the refusal's output is pending as the new `PendingRejected`
  rather than as never started: it takes only an output, an approval
  returns `ErrCallAnswered`, and `session.ReplayAnswers` answers it
  with the reject's reason. (#159)
- The outputs `Agent.Resume` appends for pending calls carry no
  trigger on their `item_end`, so the session recorder writes no
  `source` on them: their decision says who gave them, and a policy's
  refusal no longer reads as sent by the person who started the run.
  The run's start and the answers' notes keep the trigger. (#160)
- Breaking: `Agent.Resume` runs a keyed call that may have run again
  only under the key of the dispatch it repeats. An answer whose
  `IdempotencyKey` differs, the same arguments under another key,
  returns `ErrAmbiguousCall` unless it carries `WithRunAgain`: a key
  names one operation, and a new key runs the one that may have
  happened as a new one. New arguments still need a new key. A call
  whose dispatch carried no key, from a file written before format
  0.8, is refused under any key the answer names. v0.0.11's note that
  `Answer.IdempotencyKey` "supplies one after a restart" meant that
  key, and no other. (#165)
- An approval of a call that may have run carries the rule that let
  it run again as its reason when it gives none, the new
  `RunAgainSafeReason` or `RunAgainKeyedReason`, as
  `session.ReplayAnswers` spells them, and the session recorder writes
  a `proceed` whenever a call an earlier run dispatched goes to its
  tool again, with the approval's decider, so a person's approval
  with no reason is on the record; one that names nobody is written
  with no decider, where it was `policy`. A driver that gives no reason gets
  `run again`, written before the second dispatch. (#166)
- `session.ReplayAnswers`' doc names `session.AgentOptions` as the
  seed its answers are read against, where it said the context's
  items, under which a never-started call's approval is held to the
  replay rule and refused; the package doc resumes a session with
  both. (#167)
- **Call IDs a record holds stay taken.** The new
  `WithReservedCallIDs` and `Agent.ReserveCallIDs` name call IDs a
  call the model makes must not take although the transcript does not
  hold them; such a call is renamed as a repeat in the transcript is.
  `session.AgentOptions` reserves every call ID on the path, the new
  `session.CallIDs`, and `Recorder.Rebase` reserves them on the agent
  it is attached to. An agent seeded with a session's context after a
  fold did not see the calls the fold left out, so a provider that
  numbers its calls per response reused one, agentsession refused it,
  and the run failed.
- `ItemEnd.ModelCallID` keeps the ID the model gave a call the loop
  renamed, and the session recorder writes it beside the item in an
  `agentturn:model_call_id` member, the new
  `session.ModelCallIDMember`, as the format asks of a writer that
  replaces an ID.
- The Open Responses front relays a renamed call under the loop's ID,
  the one its output names; it carried the model's, so a caller saw a
  call and an output that did not match.

## v0.0.11 - 2026-09-29

- Requires `agenttool` v0.0.10, up from v0.0.9, and `agentsession`
  v0.0.11, up from v0.0.9. The session recorder writes
  `agentsession/0.8`: a 0.5, 0.6 or 0.7 session reads as it stands,
  and v0.0.10 of agentsession refuses a 0.8 file. Two rules read an
  earlier file differently, as agentsession's RFC states: the omitted
  instruction parts stay in force past the entry that wrote them, and
  a call held after its dispatch is held, not in flight.
- The omitted instruction parts under `session.WithInstructionsParts`
  are written only when they differ from the list in force, rather
  than on every config entry, since format 0.8 keeps the list in force
  until a config changes it. A memory with 474 omissions paid 37 KB
  for the list on every write, more than the joined string the parts
  were meant to beat; a 9 byte patch under that list now writes a
  320 byte delta. A list that empties is written as `[]`, which 0.7
  had no way to say, a replace carries the list whenever it is
  non-empty, and a recorder resumed on a path, or after a compaction,
  whose checkpoint carries it, starts from the list in force. (#147)
- An instructions delta under `session.WithInstructionsParts` writes
  each run of unchanged parts as one `keep` rather than naming every
  part by hash, so a memory write under many layers costs the part
  that moved.
- A tool served by agenttool's `mcpserver` finds an elicitor that asks
  the MCP client on its call's context; an agent run inside one puts
  its tools' questions there when `Config.ToolElicitor` is nil, and
  `Recorder.Elicitor` documents how to record them.
- The session recorder compares an env entry's workspace by the
  format's rule, member by member in canonical form, so a workspace
  whose members its host encodes with their keys in another order no
  longer writes a new env entry every run.
- The response entry of a model call that `Config.Retry` tried again
  carries `attempts`, the calls it took with the one that answered, so
  an exporter's `llm_call_count` counts the retries without knowing the
  `agentturn:model_retry` namespace; so does the failed response
  `run_end` writes for a call whose retries ran out. A call that took
  one leaves the member off. An abort during a retry's backoff counts
  the attempt that was due, since the loop reports the retry before
  the delay and the recorder cannot tell that abort from one before
  the attempt streamed. The `model_retry` custom entries stay,
  since they carry what a count cannot: each failure, its delay, its
  model and whether `Retry.Revise` changed the request. (#117)
- A guard that stops a run before the agent answers is reported as a
  refusal by every consumer in this repository again. Since v0.0.10
  made a guard at `BeforeTurn` or `BeforeModelCall` a stop rather than
  a failure, `front/responses` completed an empty response, `front/a2a`
  completed the task with no message, and `tools/agent` returned an
  error that named neither the agent nor the guard. A full run over
  `front/responses` now fails with the guard's error, as a single turn
  already did; `front/a2a` fails the task with it; and `tools/agent`
  returns `agent "<name>": <error>`, which wraps `agentturn.ErrGuard`,
  with the child's `ChildInfo`, and does not ask `WithNoAnswer`.
  `RunEnd.Answer` is the one test the three share: the run answered
  when its items end in an assistant message with text. Text before a
  call is a preamble, not an answer, so a guard that refuses a tool's
  output on the next turn fails the call rather than completing with
  "Let me check.". The rule holds for every guard stop, so a
  `ShouldStopAfterTurn` guard after a turn that only called tools is a
  refusal too, where it used to complete; a stop hook (`StopHook`) or
  the turn budget there still goes to `WithNoAnswer`. A guard that
  stops the run after an answer completes with it, or with the
  `OutputGuard` replacement when it has text; a replacement with no
  text is no answer, and the guard's error is reported. `tools/agent`
  takes its answer from `RunEnd.Answer` for every reason, so a child
  that stopped on its turn budget after a preamble reports no answer
  instead of returning the preamble. `ChildInfo` gains `Agent`, the
  child's name, and the default `WithNoAnswer` error names the agent
  as its documentation said it did. (#138)
- RFC 0001 describes a guard's refusal at `BeforeModelCall` as the
  loop does it: `model_blocked` is raised, then the run stops with cause
  `guard`, and the recorder writes an `agentturn:model_blocked` custom
  entry rather than a failed response. Turn phase 2 gives the guard case
  of `BeforeTurn`. The session writer's comment on a run with no
  response of its own says a guard stop before the first model call
  reads as aborted. (#140)
- The session recorder's run start entry carries `config_base`
  (`session.ConfigBaseMember`), the request hash of the configuration's
  base request, when the recorder knows the configuration. The first
  run of a recorder seeded from a stored path, by `session.Resume`,
  `session.Start` on a header with a `Base`, `Recorder.Rebase`,
  `session.Continue` or a child session reopened under the same call,
  compares with the base the path's last run start recorded, as a
  later run compares with the one before, and settles a change at
  `run_start`. A handoff that crosses a process then files the
  receiver's `BeforeTurn` items under the receiver's configuration, as
  #109 did within one, and a configuration that did not change writes
  nothing there, even when a hook edits the request. A path that
  recorded no base is compared by its settings at the leaf: a product
  whose `BeforeModelCall` edits the request pays a delta at `run_start`
  and the edited one at `turn_start` on its first resume, and
  instructions on the path composed of parts are not compared with a
  base the host's parts do not join, so they are not replaced by a
  string that was never sent. (#139)
- Breaking: `session.WithInstructionsParts` takes
  `func(ctx, req)` and is called for every session the recorder
  writes, a child run's included, with `ctx` naming that session for
  `session.SessionIDFromContext` whether or not the host put the
  child's ID on the run's context. A child agent with layers of its
  own records them as parts rather than the joined string; a function
  that returns no parts for a session leaves it the string, as before.
  A host moves to it by adding the parameter. A function that ignores
  the context is now asked for every child session too: a child whose
  instructions are the root's gets the root's parts and omitted list.
  (#142)
- The loop mints an idempotency key for every call it hands to a tool,
  the run's ID and the call's, and puts it on `agenttool.Call`, so a
  tool that claims `ReplayKeyed` deduplicates under the reference
  harness without a host wrapper. `ToolDispatch` and a pending call
  that may have run carry it, an approval through `Agent.Resume` runs
  the call again with it, and `Answer.IdempotencyKey` supplies one
  after a restart. The session recorder writes it on the `dispatch` as
  `idempotency_key`, which format 0.8 defines, and `session.Pending`
  reads back the key of a call's last dispatch with the arguments
  that dispatch ran with, the pair a run of it again repeats. A call a
  tool makes through `Invoke` gets a fresh key each time, derived from
  nothing, which RFC 0001 lists as open. (#145)
- The session recorder writes a second `dispatch` for a call an earlier
  run dispatched and a resume runs again, durably before the tool runs,
  carrying the key of the dispatch it repeats, so the record holds one
  per hand-off. An output the caller writes for such a call,
  `agentturn.OutcomeUnknown` for one, is preceded by an `answer`
  decision, format 0.8's verdict for a call ended without running
  again, rather than a `proceed`, since the call did not go on toward
  its tool, or a `reject`, since it may have run. The answer carries
  `Answer.By` and `Answer.Reason`, which `Agent.Resume` puts on the
  run's context for an output with `agentturn.ContextWithReasons`
  beside `ContextWithDeciders`; `OutcomeUnknown` carries
  `agentturn.OutcomeUnknownReason`, and `session.ReplayAnswers` says
  which rule declined the call: "not run again: replay unknown", "not
  run again: keyed without a key" or "not run again: no tool". A call
  on a path without dispatch records, which may have run, is answered
  the same way. A call run again gets a `proceed` carrying the
  approval's new `Answer.Reason`. (#144)
- Breaking: `Agent.Resume` applies agenttool's rule for running a call
  again to an approval of a call that may have run, pending as
  `PendingAborted` or, since the loop cannot say, as `PendingUnknown`:
  it runs the call when the tool's replay is safe, or keyed with a key
  known, and otherwise returns `ErrAmbiguousCall` and runs nothing. A
  tool without `WithReplay`, which reads as `ReplayUnknown`, can no
  longer be approved after an abort cut it mid-call, nor after a
  restart that seeds its call as unknown; answer
  it with `agentturn.OutcomeUnknown`, or approve it with
  `Answer.WithRunAgain`, the only way past the rule, whose proceed
  carries `agentturn.RunAgainReason`. A keyed call approved with other
  arguments than the dispatch it repeats ran with is refused unless the
  answer carries a key of its own. A call run again runs with the
  arguments of that dispatch, a decision's rewrite included, which
  `PendingCall.Args` carries. A call cut before it was handed to its
  tool is now pending as `PendingUndispatched`, not `PendingAborted`,
  since it did not run, a deferred call approved and cut so included,
  and one that may have run keeps its reason, key and arguments rather
  than reading `PendingUnknown`. `agentturn.WithPending` seeds an agent
  with what a record says of its pending calls, and `Agent.SetPending`
  a live one after `SetTranscript`, which now keeps what the agent knew
  of a call pending in both transcripts, so a held call stays held
  across a rebase; `session.AgentOptions`
  gives it and the context's items for a stored session in one call,
  `session.Pending` reads the calls, and `session.ReplayAnswers`
  returns the rule's answer for each call pending at the leaf that is
  not held. (#143)
- Breaking: `agentturn.Trigger` gains `Extra`, the caller's richer
  facts about a firing, such as when it was due or which attempt it
  is, which the session recorder writes as members of the trigger
  object beside kind, ref and source, where format 0.8 puts them: on
  the run start, on a queued input's `queued` entry, written again
  after a run end or a resume, and on the item that drains it, so a
  03:00 firing queued behind a busy run keeps its slot.
  `Recorder.Requeue` hands them back to the agent, as
  `json.RawMessage` values. A name the format defines there, `kind`,
  `ref` or `source`, or a value that does not encode as JSON, is
  refused with `agentturn.ErrTriggerExtra` where the trigger enters,
  whether or not a recorder is attached: the agent's `Prompt`,
  `Continue`, `Resume` and `Deliver`, and the loop's `Run` and
  `Continue`, refuse the run before it starts, and `Agent.Queue`,
  which now returns an error, and `Recorder.Queue` queue nothing,
  rather than failing the next, unrelated run. `Trigger.Validate`
  makes the same check. `Trigger` holds a map now and no longer
  compares with `==`. The items a run was prompted with carry its
  trigger on `ItemEnd.Trigger`, and the recorder writes it as their
  `source`, as it already did for a queued input. A child run of
  `tools/agent` no longer inherits the trigger of the parent's run,
  whose `Extra` its recorder would otherwise write on the child's run
  start, nor who answered the parent's pending calls and why;
  `WithRunContext` can give it a trigger of its own. (#146)
- Behaviour change: an item steered during a turn after which the run
  stops, on `MaxTurns`, `ShouldStopAfterTurn`, a terminating result or
  a call that needs input, is no longer appended to the run that
  stopped. It stays queued, and the next run takes it after its
  prompt: a `Prompt("second")` after a `MaxTurns` stop now sends
  `second` before the item steered in the last turn, where the item
  used to come first. A host that wants the item answered first calls
  `Agent.Continue`. `MaxTurns` is now checked at the end of the turn
  rather than at the top of the next. (#148)
- `Agent.Deliver` hands an input that arrives on its own time, a
  detached child's answer, to the model: it returns joined once a
  request of the run in flight has followed the drain that took it,
  and when that run will not call the model again it starts a run for
  it once the run in flight has ended. Called from inside the run, by
  its tool, hook or subscriber or a child run one of its tools made,
  it does not wait. A steer made while a run
  delivered its `run_end`, when `State().Running` still read true,
  waited for the user's next prompt. The run marks itself past its last drain as it
  decides to stop, or in the same step as the drain that finds the
  queues empty, and a `Queued` report made after it names no run. An
  agent mints a run's ID as it starts the run, so `State().RunID` and
  a `Queued` report made before `run_start` name that run rather than
  the one before. `tools/agent.WithDetach` documents delivering with
  it. (#148)
- `agentturn.RunContext` carries `Config.ToolRecorder`, so a record a
  tool's background job writes with `agenttool.WriteRecord` reaches the
  host rather than being dropped without an error. The call is not on
  it: a job carries its call with `agenttool.WithCall` for the record
  to name it. The tool elicitor stays off, with the rest of what
  belongs to the batch. (#149)
- A panic while an `Agent` starts a run, from a nil or misbehaving
  tool in the configuration, no longer leaves the agent's lock held:
  a caller that recovers it can fix the configuration and prompt
  again, where every later call on the agent used to block for good.
  `Prompt`, `Continue`, `Resume` and `Deliver` all release it.
- `front/a2a.WithRecorderFor` records the runs a served agent makes.
  The executor drives each task on an `agentturn.Agent` seeded with the
  stored conversation, rather than on `agentturn.Run`, and hands that
  agent, with the task's context ID, to the function before the run
  starts; a host opens that conversation's session there, points
  `Config.ToolRecorder` at it with `SetConfig`, attaches the recorder
  and returns a context carrying the session ID. What it subscribes
  sees every event in step with the run, so a call's dispatch is
  written before the tool runs and a tool's record lands under its
  call, and the loop goes at the pace of those writes. A subscriber
  error fails the task, or cancels it when the run was being aborted.
  A record that cannot be opened, or a nil context, fails the send:
  the caller gets the error and no task exists. `agentturn.Run` runs
  ahead of its consumer, which a recorder cannot follow. The
  transcript still comes from the `ConversationStore`, which drops the
  calls an aborted run left unanswered while the session keeps them.
  (#141)
- `front/a2a` runs one task per context ID at a time. A message on a
  context ID with a task in flight is refused at once with
  `ErrConversationBusy`, wrapped with `a2a.ErrInvalidRequest`, rather
  than running on the transcript the first task has not saved, which
  lost one of the two turns; a served agent whose tool sends to its own
  conversation gets that error instead of waiting on itself. (#141)
- Two pending states from format 0.8. A call a record holds after its
  dispatch is pending as `PendingDeferred` with the new
  `PendingCall.Dispatched` set, the key and the arguments of that
  dispatch: it waits on the caller and may have run, so
  `Agent.Resume` holds an approval of it to the replay rule as it
  does an aborted call's, which `PendingCall.MayHaveRun` reports, and
  the recorder writes the approval as a `proceed` and a second
  `dispatch`, and an output as an `answer`. A call an `answer` ended
  before a crash wrote its output is pending as the new
  `PendingAnswered`: `Agent.Resume` refuses an approval of it with
  `ErrCallAnswered`, the recorder writes its output with no decision,
  since the format allows nothing else after an answer, and
  `session.ReplayAnswers` gives `OutcomeUnknown` as that output.
  `session.Pending` read both as other states: the first as a plain
  hold approved without the rule, the second as unknown, whose
  approval the store refused. (#143)
- Breaking: `session.DispatchKey` and `session.IdempotencyKeyMember`
  are gone. agentsession reads the key into
  `DispatchEntry.IdempotencyKey`, so a dispatch no longer holds it in
  `Unknown`, and `Call.IdempotencyKey` with `Call.DispatchedArgs` read
  the pair a run again repeats.

## v0.0.10 - 2026-09-28

- Requires `agenttool` v0.0.9 and `agentsession` v0.0.9, up from
  v0.0.8. The session recorder writes `agentsession/0.6`, which adds
  optional members only; a 0.5 session reads as it stands, and v0.0.8
  of agentsession refuses a 0.6 file, as a 0.x reader refuses a later
  minor. A store now honours a header's `base` at `Create`, so a
  forked session can be written through any of them.
- `session.WithInstructionsParts(fn)` gives the recorder the parts a
  request's instructions are composed of and the parts the host left
  out, so config entries carry `instructions_parts` and
  `instructions_omitted` rather than one string. A change to one layer
  is a delta naming that part, the others by hash: a 13 byte memory
  write under two 4000 byte layers is a delta of a few hundred bytes
  where it repeated the whole prompt. `fn` is called with the request
  about to be sent, after `BeforeModelCall`, so a product whose hooks
  rewrite the instructions takes its parts from what they left; parts
  that do not join to the instructions sent are dropped and the string
  is written, as without the option, so a product's composition never
  fails the run; so are parts the format refuses, one with no ID or two
  sharing one, and an omitted part with no ID is dropped. Omitted
  parts are written on every config entry while there are any, and on
  an entry of their own when only they changed; the format cannot say
  the list emptied, so a reader keeps the last one written. A session
  recorded before the option was set has the joined string on its
  path, and its first entry under the option carries every part's
  text, once. It applies to the recorder's own session. Without the option nothing
  changes. RFC 0001 draft 0.2 resolves its open question on
  instructions as parts. (#114, #121, #129, #90)
- **Breaking, in what is recorded.** The recorder writes more of what
  the loop knows, and every addition is an entry or a member a reader
  written against 0.5 ignores; nothing already written changes its
  hash, and no `ref` changes except a guard's.
  - An allowed call whose decision gave a reason, the grant that
    allowed it, is a `proceed` decision carrying the reason. A hook
    that returns nil or no reason writes nothing, as before. (#130)
  - A guard's stop writes the run end's `ref` as the cause followed by
    the guard's error, `guard: <error>`, where it was `guard` alone and
    the error was lost. The other causes are unchanged. (#110)
  - Every failed model attempt that is tried again is a custom entry in
    `agentturn:model_retry` (`session.ModelRetryNS`): the attempt, the
    error, the delay, the failed attempt's model and whether
    `Retry.Revise` changed the request. It precedes the response of the
    attempt that answered and holds no item and no setting, so an
    exporter can count a turn's calls; agentsession's exporter reading
    it for `llm_call_count` is filed there. (#117)
  - A configuration that changed since the last run, through
    `Agent.SetConfig`, is settled at `run_start`, before any item of the
    run, so what the new configuration's `BeforeTurn` appends is filed
    under it. A configuration that did not change writes nothing there,
    even when a hook edits the request every turn, and `turn_start`
    still settles the request as sent. The first run of a resumed
    recorder has no last configuration to compare with and settles at
    `turn_start`, as before. (#109)
  - `WithEnv` compares entries with the members the library does not
    define, so a container restart named in one is written; an
    unchanged environment still writes nothing. (#128)
  - The run start carries the trigger in its parts as `trigger`, beside
    the joined `ref`, which is unchanged. `agentturn.Trigger` gains
    `Source`, the layer that took the input, which the joined string
    leaves out. (agentsession #82)
  - A custom entry a tool's record, a recordable details value or a
    nested call writes carries `call_id`: the call on the tool's
    context, or for a nested call the call whose tool made it, and
    nothing when the session does not hold the call, as for a child
    run's context, which carries its parent's call. The records of a
    parallel batch now say whose each is. (agentsession #87)
- `Recorder.Annotate` returns the ID of the entry it wrote, and
  `Recorder.EntryOf(ctx, item)` returns the entry of an item the
  recorder wrote, so a checkpoint does not depend on subscriber order.
  **Breaking**: `Annotate` returned only an error; a caller that
  ignored the ID writes `_, err :=`. (#124)
- `Config.ToolElicitor` installs an `agenttool.Elicitor` on every tool
  call's context, and `session.Recorder.Elicitor(by, fn)` wraps one so
  a question a tool asks the user mid-call, an MCP server's elicitation
  through mcpclient among them, is written as a custom entry in
  `agentturn:elicitation` under the call, with the message, the schema
  or URL, the action, the content and who answered, before the answer
  returns to the tool. A nil `fn` answers `cancel`, since nobody was
  asked. (agenttool #48)
- A call whose `tool_dispatch` a subscriber refused is pending as
  `PendingUndispatched`, since the loop knows its tool never ran, where
  it read `aborted`, "may have run". The docs of `ToolDispatch`,
  `Recorder.Attach` and RFC 0001's `tool_dispatch` row say a subscriber
  that vetoes a dispatch is registered before the recorder. A `reject`
  that withdraws a written dispatch waits on agentsession's RFC and is
  an open question. (#119)
- RFC 0001's open question on a `proceed` with no `dispatch` is
  resolved by agentsession #78: the format admits it.
- **Breaking**: an error wrapping `ErrGuard` from `BeforeTurn` or
  `BeforeModelCall` stops the run with `ReasonStopped` and `StopGuard`,
  the error on `RunEnd.Err`, as it does from `ShouldStopAfterTurn`; it
  ended the run with `ReasonError`, so a cost limit either overshot by a
  call or was filed as a failure. The turn has no `turn_start`; a stop
  from `BeforeModelCall` still raises `ModelBlocked` with the refused
  request, which the recorder writes as a custom entry in
  `agentturn:model_blocked` carrying the error and the request hash,
  since a failed response would make the record read the stop as a
  failure. Any other error is unchanged. (#116)
- **Breaking**: an item steered while the agent is idle joins the next
  run before its first model call, after the prompt, where it waited
  for the first batch and reached the model one call late. A run that
  begins with approved calls still drains after their batch, and a
  refusal on `Resume` leaves the queue for the run after it. (#123)
- `Agent.Queue(ctx, mode, items...)` is `Steer` or `FollowUp` with the
  trigger on `ctx` carried on each item's `Queued` report, which gains
  `Trigger`: an input that joins a run has a provenance of its own. (#67)
- **Breaking, in what is recorded.** The recorder writes the inbox and
  closes the runs it inherits. (#67, #120, #94, #118)
  - Every `Queued` report is a `queued` entry, with the mode and the
    trigger, before the item is appended, and the item entry that
    appends it names it in `queued_from` with the trigger as `source`.
    A run end closes the entries of the inputs it did not append, so
    the recorder writes them again after the end, since the agent
    still holds them. `Start` promises `queued` in the header by
    default. After a kill, `Session.PendingQueued` lists what was
    accepted and not appended, and `Recorder.Requeue(ctx, agent)` hands
    it back to the agent without writing it a second time; an input
    nobody takes up is closed by the next run end and not written again.
    An input accepted while the agent is idle is reported, and written,
    at the next run's start; `Recorder.Queue(ctx, agent, mode, items...)`
    writes it first and then queues it, for a host that must not lose
    one between runs.
  - `Resume` closes a run left open at the leaf, which a crash cut off,
    with reason `error` and ref `cut off: closed on resume`, before it
    returns, and queues the inputs that run owed again after the end,
    as agentsession #86 settled: the writer that continues the path
    owns the run. `Rebase` into a run closes it `interrupted` with ref
    `rewind to <entry>`, leaving what that run owed behind, and writes
    on the new branch what the agent holds, unless its entry is still
    pending there; a rebase between runs with nothing held appends
    nothing.
  - `Start` on a header with a `Base` seeds the recorder from the
    context at the base, as `Resume` does at the leaf, so an agent
    seeded with it records requests that carry hashes, and closes a run
    the base is inside, `interrupted`, with ref `fork at <entry>`, and
    the inputs the prefix owes after a run's end are the fork's to take
    up with `Requeue`. Every
    store honours a base since agentsession v0.0.9. (#118)
- The batch of calls approved through `Agent.Resume` keeps turn 0,
  which the tool events' docs now state: it runs before the run's first
  model call, and a synthetic turn would either write a failed
  response into the record or count against `MaxTurns`. (#113)
- RFC 0001 resolves its open questions on durable queues (#67) and a
  run the process died inside (#94), and says who closes an open run.
- **Fixed**: `front/responses` relays what `OutputGuard` left. In a
  full run a message the guard replaced after its deltas went out is
  carried as the replacement on its `output_text.done`,
  `content_part.done` and `output_item.done` events and in the
  response, where the relay found the replacement did not extend the
  streamed text and closed the message with the original; the deltas
  cannot be taken back, so a client renders on done. A request with
  the caller's tools, which calls the model once itself, now runs the
  guard on each assistant message as the loop does, with no run ID and
  turn 1; it ran none. A replacement of another shape than what was
  streamed, a refusal over text or two parts over one, keeps the
  stream well formed: the streamed parts are emptied at their indices
  and the replacement's other parts are written after them. A message
  the guard kept streams exactly as before. (#108)
- `agentturn.RunContext(ctx)` returns, on the context of a tool call, a
  hook, the transform or the model call, the run's context: the values of the context the run was
  started with and the run ID, cancelled by `Agent.Abort`,
  `AbortCause` or the cancellation of the prompt's context, with the
  cause, and not when the batch or the run ends by itself. Background
  work that must outlive its call derives from it, where the call's
  own context ended with the batch and nothing else was on it. The
  low-level `Run` and `Continue` cut it when the caller's context ends
  or the consumer breaks out before the run's end. (#125)
- `agentturn.Steered(ctx)` returns a channel a steer into the tool's run
  closes, during its batch or before it and not yet drained, so a tool
  that only waits returns early; the steered item joins the run after
  the batch as before, the loop does not cut the tool, and the next
  batch listens afresh. nil outside an agent's run. (#126)
- `tools/agent.WithDetach(fn)` makes a call return once the child's run
  has started, with an output saying it is working and a `ChildInfo`
  naming the run, and hands the run's end to `fn`; the child runs on
  `RunContext`, so an abort of the parent's run cuts it and the parent
  ending does not. The session recorder writes a detached child's
  session through its end, after the call's `tool_end`, and releases
  its writer then. (#127)
- `tools/agent` carries `ContextWithRetry` from a later `Prompt` of a
  spawned child to its observer, so a recorder starts that child's
  session afresh; it never saw the mark. (#122)
- `PendingCall.Tool` is the tool a pending call resolved to in the run
  that made it, nil for a call no tool has or one found in a seeded
  transcript, and `ToolEnd.Reason` is the decision's reason for a
  blocked or deferred call, so a front asking the user can show what
  it asks about and why. Neither changes the record. (#112)
- `ChainTransform(fns...)` runs transforms in order, each on a copy of
  what the one before returned, so a product's own shaping runs before
  the compact transform. A request a product transform changed has no
  hash, as with one transform. `compact.Fold.First` is the first item
  a fold kept, and `Recorder.Fold` finds its entry by that item rather
  than by the fold's index, which counts the transcript the fold was
  given. A fold whose first kept item the recorder wrote nowhere, after
  a transform that replaced items, or in two places, one item value
  appended twice, is written as a custom entry in
  `agentturn:compaction_unplaced` rather than a compaction naming the
  wrong entry, and the run goes on. (#115)
- `OutputInfo.Output` holds the items of the response that precede the
  message, and the guard's docs say that whether a message is the
  answer is unknown when it runs, since a model may speak before a
  function call; a guard that needs finality reads `TurnInfo.Final`.
  (#111)

## v0.0.9 - 2026-09-28

- Requires `agenttool` v0.0.8 and `agentsession` v0.0.8, up from
  v0.0.7. The session recorder writes `agentsession/0.5`, whose entry
  IDs are envelope hashes; sessions written under 0.4 read by
  migrating in memory, as that library says.
- **`tool_dispatch`** reports the moment a call is handed to its tool:
  after it has taken a slot in the bound and its turn in a serial
  batch or a resource chain, and before the tool runs. `tool_start`
  keeps its place, when the call is decided, in the model's order for
  the whole batch. The event is raised from the executor's `OnStart`
  on the call's own goroutine, serialised with the run's events, and a
  subscriber that fails on it stops the call before its tool runs and
  ends the run with reason `error`, as any delivery failure does; the
  call is then pending as `aborted` with no side effect behind it.
  (#93)
- **Breaking, in what is recorded.** `session` writes the `dispatch`
  entry from `tool_dispatch`, per call, rather than from `tool_start`,
  per batch. A call cut off before it reached its tool, waiting for a
  slot or its turn, now has no dispatch and reads as never started; it
  read as in flight before, so a resume after a kill treated every
  call of the batch as possibly run. A call the loop refused itself, a
  name no tool has or arguments that are not an object, is recorded
  as a `reject` decision by `policy` carrying the error the model saw,
  where it was written with a dispatch it never earned. (#93)
- `Config.ToolRecorder` is `agenttool.Executor.Recorder` for every
  batch, so a tool that writes a record while it runs with
  `agenttool.WriteRecord` reaches the host under the loop; it was a
  no-op, since the loop installed nothing. `session.Recorder.RecordFunc`
  is the value to set it to: each record lands as a custom entry in
  the record's namespace, at the leaf of the run on the context,
  durably before the tool goes on. (#97)
- **Fixed**: a hook or a subscriber that fails while a tool batch is
  in flight no longer leaves the running calls without their
  `tool_end`. The batch is settled as an abort settles it: every call
  that has not ended gets a `tool_end` carrying the failure, the
  outputs of the calls that finished with a result of their own are
  appended, and the rest are pending as `aborted`; a call whose
  after-call hook failed has no result and is cut. The failure stops
  the batch's tools through their context, with the failure as the
  cause, and the executor is drained before anything is ended, as
  under an abort, so a call that returns its own result on the way out
  is settled through the after-call hook and appended, or cut when the
  hook itself was what failed, and a tool's `tool_end` follows its
  return. An abort that a subscriber or the
  after-call hook fails inside now ends every cut call before the
  failure is reported, the remaining cut calls skipping the hook, and
  a nested call whose hook or delivery failed gets its `tool_end`
  before the error returns to the tool. A `tool_end` counts as raised
  when the loop delivers it, so a subscriber that fails on one never
  sees the call ended twice. (#101)
- **Fixed**: `ToolDecision.Terminate` on an allowed call that runs
  now ends the run with `StopTerminate`, or `StopPartialTerminate`
  when the rest of the batch did not agree, as the doc comment
  promised; it was honoured only for a call settled in preflight. An
  `AfterToolCall` override does not clear the hint. (#102)
- **Fixed**: a wire `error` event the model sends before its answer
  opens is a failed attempt like a cut stream, retried under
  `Config.Retry` with the wire error itself offered to the policy. It
  was treated as a committed attempt and never retried, so a 503
  reported that way defeated the retry and the fallback chain. An
  error event after the answer opened is still final. (#103)
- `docs/rfcs/0001-agent-loop.md` states the loop's contract as draft
  0.1, structured as agentsession's and agenttool's RFC 0001: the
  transcript, the request procedure, the phases of a turn, the batch
  and its decisions, the run end with its reasons and causes, pending
  calls and the resume, cancellation, the event catalogue with its
  order and invariants, delivery, the queues, the hooks and their
  chains, nested calls, composition, what each event gives the
  record, the Go binding as a table, and conformance. The open
  questions name the issues that track each gap. Checking the draft
  against the code found the three defects fixed above. (#81)

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

