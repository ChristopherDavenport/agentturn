# RFC 0001: Agent Loop

Status: draft 0.2
Author: Christopher Davenport
Discussion: to be opened against this repository. The Go module at its
root is the reference loop; agenttool RFC 0001 is the tool contract it
executes and agentsession RFC 0001 is the record it writes.

## Summary

An agent loop takes a transcript and a configuration, calls a model,
runs the tools the model asked for, and calls the model again until the
model answers without asking for anything, a hook stops it, a tool
answers on the model's behalf, a call is handed to the caller, the
caller cancels it, or something fails. It reports every step as an
**event**, and the last event says how the run ended.

This document states what a loop owes the three parties around it. To
the **model**, it owes a request built one way from the transcript and
the settings, so a recorded session rebuilds the request that was sent.
To the **tools**, it owes the batch rules of agenttool RFC 0001 and a
decision seam before each call, so a policy blocks, rewrites or defers
a call without editing the loop. To the **observer** — a front, a
session recorder, a parent agent — it owes an event sequence with fixed
order and fixed invariants: one terminal per run, every `tool_start`
paired with a `tool_end`, an item announced complete only when it is,
and every event after a cancellation still delivered, so what the
record holds is what happened.

The transcript is Open Responses items and nothing else. The loop knows
a model and a list of tools and never learns a sub-agent concept; every
composition is one of those two things, and the same loop serves as a
model, a tool, a peer over a protocol, or itself under new settings.

## Motivation

The rules below exist in doc comments on exported Go identifiers
(#81). Checking this draft against the code found three that the code
did not keep (#101, #102, #103); they are fixed, and the rules here
are the ones the tests now hold. A second
implementation, in Go or elsewhere, has nothing to conform to and is
reverse-engineered from comments, and two of them diverge exactly where
the comments are load-bearing: one delivers `run_end` as soon as the
context is cancelled rather than after the `tool_end` of the cut-off
call, so its sessions resume against a transcript missing a tool
output; one honours a tool's terminate hint on the first result that
sets it, so the model never sees the read that ran beside a search;
one delivers post-abort events with the cancelled context, so the
recorder's last write fails and nothing reports it. All three write
session files that agentsession's reader treats differently from the
reference loop's, and a replay across the two fails in a way that reads
as model nondeterminism.

agentsession ships RFC 0001 and a fixture corpus; agenttool ships RFC
0001 and a schema corpus. Between them, the loop is the piece that
takes a tool's result and puts it in the record, and it is the piece
whose semantics were written down nowhere. This document is the third
of the set: the tool contract ends where a batch is handed to an
executor, the session format begins where an entry is appended, and
the loop is everything in between.

It changes no behaviour of the Go module at draft 0.1. Where the module
falls short of what is written here, or where a rule is contested, the
shortfall is under open questions with the issue that tracks it.

## Goals

- **One transcript.** The conversation is a list of Open Responses
  items. What a session stores, what the model receives and what a
  front renders are the same bytes. There is no third message model.
- **One request.** Each turn's request is built from the transcript and
  the settings by a stated procedure, with the members the loop owns
  fixed, so a recorder rebuilds it and a replay resends it.
- **One event stream.** Every step of a run is an event with a stated
  position in the sequence, and a set of invariants holds over every
  run however it ends.
- **Hooks, not edits.** Policy, context injection, compaction, output
  guarding and early stopping are seams the loop calls at stated
  moments, each with a stated contract, so a product composes them
  without touching the loop.
- **Resumable.** A run that ends with calls unanswered says which and
  why, and the caller answers them through one path whether a hook
  deferred them or a cancellation cut them off.
- **Recordable.** Every event a session recorder needs carries what it
  needs when it needs it, so the record is written as the run happens
  and is durable before the side effect it precedes.
- **Composable.** The loop knows a model and a list of tools. An agent
  stands inside another as one of those, and nothing in the loop knows
  the difference.
- **Bindable.** The contract is language-neutral; the Go module is a
  binding of it, given here as a table.

## Non-goals

- **Tool semantics.** What a tool is, how its arguments are validated,
  how a batch is ordered, bounded and serialised, and what a record is
  belong to agenttool RFC 0001. This document uses those rules and adds
  the decision seam in front of them.
- **The record's shape.** What an entry is and how a path rebuilds a
  request belong to agentsession RFC 0001. This document says which
  event carries what a writer of that format needs, and when.
- **A permission system.** The decision hook is the seam; who decides,
  and by what rule, is the host's.
- **Transports.** A front owns its transport. This document defines
  what a front feeds in and consumes out.
- **Provider catalogues, cost and auth.** The model is anything that
  streams an Open Responses response.
- **Persistence of the loop's own state.** The queues live in memory
  and the transcript is the caller's; a session library is a
  subscriber.

## Terminology

The key words MUST, MUST NOT, SHOULD and MAY are to be interpreted as
in RFC 2119.

- **Transcript**: the conversation as a list of Open Responses items,
  in order. The loop appends to it and never edits what is there.
- **Item**: an Open Responses item, at the version the payload names.
- **Request**: an Open Responses request, built by the loop for one
  model call.
- **Settings**: the members of a request other than its input: model,
  instructions, reasoning, text, tools, and the passthrough members.
- **Model**: anything that streams an Open Responses response for a
  request.
- **Tool**: a tool as agenttool RFC 0001 defines it: a definition, a
  set of properties and an execute.
- **Configuration**: the model, the settings, the tools, the hooks and
  the limits a run reads.
- **Run**: one pass of the loop, from an input or a resume until the
  loop stops calling the model. A run has an ID, a source, a sequence
  of turns and exactly one end.
- **Turn**: one model call and the tool executions it requested. Turns
  are numbered from 1 within a run. Everything before the first turn —
  the prompts, a resume's outputs and notes, and a batch the caller
  approved on resume — carries turn 0; an item appended after a turn's
  batch carries that turn's number.
- **Batch**: the function calls of one response, in the model's order,
  or the calls a caller approved on resume.
- **Call**: one function call item and everything that happens to it:
  its decision, its execution and its output.
- **Pending call**: a function call in the transcript with no function
  call output after it.
- **Event**: one step of a run, reported to the consumer.
- **Consumer**: whoever receives the events: the caller of the
  low-level loop, or the subscribers of an agent.
- **Hook**: a function the configuration supplies and the loop calls at
  a stated moment.
- **Front**: whatever feeds prompts in and consumes events out: a
  terminal, a protocol server, a channel bridge, a parent agent.
- **Agent**: the stateful form of the loop: a transcript it keeps, the
  queues, the subscribers and run control.
- **Queue**: items accepted while a run is in flight or the agent is
  idle, to be appended at a stated point of a run.
- **Hidden item**: an item in the model's context that a renderer
  should not show.

## The transcript

The transcript is the only conversation state. The loop reads it to
build each request and appends to it as items arrive; a front renders
from it; a session recorder writes it.

- The transcript MUST be a list of Open Responses items. A loop MUST
  NOT introduce a second item shape or a wrapper the model does not
  see; an item a harness adds for itself and not for the model is an
  extension item, slug-prefixed as the payload allows, and the filter
  below removes it before a call.
- Within a run the loop only appends. It MUST NOT reorder, rewrite or
  remove an item that is in the transcript when the run starts or that
  it has appended. A shorter or edited conversation for one model call
  is the transform's, and the transform's result is the request's
  input and never the transcript.
- A transcript is **valid input** when every function call in it has
  a function call output naming it. The loop checks that one exists
  anywhere in the transcript and does not check that there is only
  one, or that it follows the call. A run MUST leave the
  transcript valid, or end with the unanswered calls listed on its end
  event, and a loop MUST refuse to start a run over a transcript that
  holds an unanswered call unless the run's own input answers it.
  Whether a message appended after a dangling call settles it is not
  the loop's to decide; a strict server says no, and the loop follows
  the strict server.
- The loop MAY be given a transcript to start from and MUST NOT modify
  the caller's copy; the items it appended are on the run's end event
  so a caller holding the low-level iterator can rebuild the result.
- An item the caller supplies MAY be marked **hidden**. The loop
  appends the item itself, unwrapped, so the transcript and the request
  hold the item and only its events say it is hidden. A session
  recorder writes it with the format's display flag off. Hidden is a
  fact about rendering: the model reads the item, a request carries it.
- The **filter** removes the items the model must not see before every
  call. The default drops every item whose type carries a slug prefix
  and every null; a configuration MAY keep listed extension types. An
  item the filter removes is out of the model's context and a recorder
  writes it as a record entry, never as context. Marking such an item
  hidden adds nothing.

## The configuration

A configuration is what a run reads and nothing else is. It carries:

| member | what the loop does with it |
| --- | --- |
| name, description | how the agent presents itself when composed: the tool name and description as a tool, the card as a peer, the server info over a protocol. The loop itself reads neither |
| model | required; a run over a configuration with no model is refused |
| model name, instructions, reasoning, text | the corresponding request members, set on every request when set |
| request base | every other request member — tool choice, parallel tool calls, max output tokens, temperature, truncation, include, cache key, service tier, passthrough extras — copied onto every request. The loop owns input, tools, store, stream, stream options and previous response ID and overwrites them |
| tools, or a tool provider | the tools of the moment, resolved once per turn |
| execution mode, parallel bound | the batch rules of agenttool RFC 0001: sequential, or parallel up to the bound |
| max turns | a limit after which the run stops |
| filter, transform | what the model sees; see the request |
| retry | the policy for a model call that failed before it began |
| the hooks | see [Hooks](#hooks) |

The **tools of the moment** are resolved once per turn, before the
request is built, and the same list serves the turn's batch, so a call
resolves against what the model was offered. A provider that snapshots
a remote list offers a change one turn late when the change is still in
flight as the turn starts; a provider that must not waits inside
itself, bounded by its context. A set of tools MUST have distinct
names; a fixed list is checked once before the run and a provided list
once per turn, and a duplicate refuses the run or fails the run. The
loop never closes a tool: close belongs to the host, as agenttool RFC
0001 says, and a provider that builds tools per turn is responsible for
what it built (#95).

A configuration MAY be replaced between runs. The transcript is kept,
so the next run continues the same conversation under other
instructions, tools, hooks or model. This is the fourth way an agent
composes, [below](#composition), and the one that needs no package.

## A run

### Starting a run

A run begins in one of two ways, and its **source** says which:

- **input**: new items are appended and the model is called. The
  low-level loop takes the items as its prompts, or continues from a
  transcript that already ends with something the model can answer: a
  message that is not the assistant's, or a function call output.
- **resume**: the run begins by answering a call that was pending when
  it started, whether the caller approved the call so the loop runs it
  or supplied its output. A prompt that opens with the outputs of the
  pending calls is a resume that also carries a message.

Before a run starts, the loop checks what it can and refuses when:
there are no prompt items; the transcript cannot be continued; the
configuration has no model; two tools share a name; the transcript
holds an unanswered call that neither it nor the leading outputs of the
prompts answer; an answer names a call that is not pending; or, for an
agent, a run is already active. The two forms refuse differently. The
low-level loop yields one `run_end` carrying reason `error`, the
refusal as its error and no run ID, and no other event. The agent
returns the refusal to the caller and delivers nothing to its
subscribers, since no run started and there is nothing to record.

A run has an ID the loop mints, carried on every event of the run and
attached to the context of everything the run calls — the transform,
the hooks, the model, the tools — so a tool that composes another agent
can say which run it was called from. A **trigger** the caller attaches
to the context, a kind and a reference in the caller's own terms, is
carried on `run_start` and read by nothing in the loop; it is how a
recorder learns what caused the run without the loop learning what a
cron job is.

### The turn

A turn proceeds in these phases, in this order. Each names the events
it raises and the hooks it calls; the events are defined
[below](#events).

1. **Limits.** If the run has taken its turn limit's worth of turns,
   it ends with reason `stopped` and cause `max_turns`, whatever would
   have started the next one: a tool batch to answer or a steered item.
   If the run's context is cancelled, it ends with reason `aborted`.
2. **Before the turn.** The before-turn hook MAY return items, which
   the loop appends to the transcript with their item events as it
   appends any input. They are then facts about the transcript: a
   recorded session rebuilds the request they were part of, which
   injection through the transform cannot give. A hook error ends the
   run with reason `error`.
3. **Tools of the moment** are resolved and, for a provided list,
   validated. A resume's approved batch resolves them the same way and
   skips the validation.
4. **The request** is built as the [next section](#the-request) says.
   The before-model-call hook runs on it last. A hook that refuses
   raises `model_blocked` carrying the request as built and ends the
   run with reason `error`; no call was made, and the event is what
   tells a recorder that from one that was made and failed.
5. **`turn_start`** carries the request exactly as it will be sent and
   the **inputs** of the turn: the items appended since the previous
   turn's response, or since the run started for the first turn. They
   are what this turn's response answers.
6. **The model call**, as the [model call section](#the-model-call)
   says: item events as the stream delivers output items, an attempt
   retried under the retry policy when it failed before it began, and
   `response_end` with the folded response once the stream ends.
   Every completed output item is in the transcript when `response_end`
   is delivered. A model failure ends the run with reason `error`.
7. **The batch**, as the [batch section](#the-batch) says: the
   function calls of the response are decided one at a time in the
   model's order, then executed, then their outputs appended in the
   model's order.
8. **`turn_end`** carries the folded response and the tool results in
   the model's order. It is raised whether or not the batch left calls
   pending.
9. **Pending.** If any call of the batch was deferred, the run ends
   with reason `input_required` and the deferred calls listed. Nothing
   below runs: a deferred call is a question to the caller, and the
   stop hook and the terminate hint are answered after it.
10. **The stop hook** sees the turn and MAY end the run with reason
    `stopped` and cause `hook`, or with cause `guard` and its error on
    the end event, or fail it with reason `error`.
11. **Terminate.** If every result of the batch set the terminate hint,
    the run ends with reason `stopped` and cause `terminate`. If some
    did, it ends with cause `partial_terminate`: the other calls ran
    and their outputs are in the transcript, and the host acts on the
    call that asked to end the run. agenttool RFC 0001 requires a
    harness to report a partial batch as partial and leaves the policy
    to it; this loop's policy is to stop and say so.
12. **Queues.** Items steered in while the turn ran are appended, with
    their item events. If the model called no tools, the follow-up
    queue is drained after them; if nothing was queued, the run ends
    with reason `done`. Otherwise the next turn begins at phase 1.

A batch of zero calls runs phases 7 and 8 trivially: `turn_end` carries
no results, and phase 9 finds nothing pending.

### The request

The request for a turn is built from the transcript and the
configuration by this procedure, and a loop MUST build it this way so
that a recorder holding the transcript and the settings rebuilds it:

1. Start from a copy of the working transcript.
2. Apply the **transform**, which MAY return a shorter or otherwise
   edited list for this call only: prune, compact, inject. It receives
   a copy of the list and MUST NOT mutate the items. The working
   transcript is not replaced.
3. Apply the **filter** to the result, which removes what the model
   must not see.
4. Take the request base, set the loop-owned transport members — store
   false, stream true, no stream options, no previous response ID — and
   apply the model
   name, instructions, reasoning and text over it, the passthrough
   extras merged over its own, and the definitions of the tools of the
   moment.
5. Set the input to the filtered, transformed list.
6. Call the **before-model-call hook** on the finished request. It MAY
   change anything.

History is always inlined and the provider's store is never used, so
the loop works against a server without a store and a session
recorder sees the whole input. The loop adds nothing that varies from
run to run: run and turn IDs ride on the events, never on the request,
so the settings the model sees change only when the configuration
does, and a cached prefix survives a run boundary. That is the writing
discipline of agentsession RFC 0001 applied where the request is made.

Two consequences follow for the record and are stated here because
they are decided here. A change to the input by the transform or the
hook is one the record cannot describe: the path holds the transcript's
items and the request carried others, so a recorder that hashes what it
sent MUST decline to hash rather than write a hash the path cannot
verify (#92). A change to a setting by the hook is a config delta on
the path. A transform that folds the transcript and reports the fold to
the recorder, with what it kept, is the one edit the record can
describe, and it is described as a compaction rather than as a
divergence.

### The model call

The model is called with the request and streams a response. The loop
turns the wire events into item events and appends completed items to
the transcript.

- An output item raises `item_start` as the stream opens it, an
  `item_update` for each wire event that touches it, carrying the wire
  event verbatim and the item as accumulated so far, and `item_end`
  once the stream has completed it. An `item_end` for an assistant
  item MUST follow the wire event that says the item is done; a partial
  item MUST NOT be announced as complete. The item is in the transcript
  when `item_end` is delivered.
- The **output guard** runs on each assistant message as the stream
  completes it, after the wire event that completes it and before the
  message is appended and `item_end` is delivered, and MAY replace the
  message with another. The deltas of the original have already been
  delivered as `item_update`, so a front that must not show withheld
  text renders on `item_end`. Function calls, reasoning and every other
  output item never reach the guard, so a replay still has what it
  needs. A guard error fails the run. The replacement reaches the transcript and `item_end` only: the
  response carried by `response_end`, `turn_end` and the stop hook is
  the wire response as the model produced it, with the original
  message, so a front that must not show withheld text renders from
  the item events and never from the response.
- `response_end` carries the folded response, usage included, as soon
  as the stream ends and before any tool of the turn runs. A response
  that arrived with a failed status is delivered here too, before the
  run ends with the error, so a recorder can write it.

#### Retry

A model call that failed before it began MAY be attempted again under
the configuration's retry policy: a maximum number of attempts, a
backoff, a predicate on the error, and optionally a revision of the
request. A retry happens inside the turn: the request is sent again
after the delay, `model_retry` reports the attempt that failed, its
error, the delay and the request the next attempt will send, and the
turn's `turn_start` and `response_end` are delivered once.

An attempt **commits** when the model begins its answer, which is a
message or a function call item opening; when the server answers with
a failed response; or when a consumer fails while it is delivered. An
error event the server sends before the answer opens does not commit:
it is a failed attempt like a cut stream, and the retry policy sees
the wire error itself; one sent after the answer opened is final. A
committed attempt MUST NOT be retried, because the transcript or a
recorder may already hold part of it. An item the model completes
before the attempt commits — the reasoning summary a reasoning model
writes before its first token — is **held**: it reaches the consumer as
`item_start` and `item_update`, so a front renders it live, and it is
appended with its `item_end` when the answer begins or when the
response arrives. An attempt that ends without its response — a
transport failure, a cut stream, a failed response — drops what it
held, so a failure between the reasoning and the first token is retried
and leaves nothing behind. A front that renders from `item_start` drops
what it was rendering when a `model_retry`, or a `run_end` with an
error or an abort, follows with no `item_end` for it.

Retry is off unless the policy names more than one attempt. The
default predicate retries the transient statuses (408, 409, 429 and
5xx), a stream that ended before its terminal event and transport
failures, and calls everything else, a 4xx in particular, final. The
default backoff honours a Retry-After header, uncapped, and otherwise
doubles from 500ms to a 30s cap. Cancellation cuts a delay short and
ends the run with reason `aborted`.

The revision MAY move the next attempt to another model or another
setting, which is how a fallback chain lives in the loop rather than
under it: the revised request is on the `model_retry` event, a recorder
takes its settings, and the path names the model that answered rather
than the one that did not. A fallback written as a model under the
loop still works and still says nothing.

### The batch

The function calls of a response form a batch. The loop runs it in two
stages: every call is **decided**, one at a time in the model's order,
before any call executes; then the calls that may run are handed to an
executor under the rules of agenttool RFC 0001.

#### Preflight

For each call, in the model's order:

1. The tool is looked up by name among the tools of the moment. The
   arguments are the call's arguments string parsed as JSON, `{}` when
   empty.
2. The **decision hook** sees the call, the tool or none, the
   arguments, the whole batch and the call's index in it, and returns a
   decision or none. None allows the call. A decision has an action —
   **allow**, **block** or **defer** — and MAY carry a reason,
   replacement arguments, a note, a terminate hint and who decided.
   Replacement arguments are what the tool receives; the function call
   item in the transcript keeps the model's. A hook error fails the
   run.
3. `tool_start` is raised with the arguments the tool will receive and
   the decision, in the model's order.
4. A call that will not execute is **settled** at once, with its
   `tool_end`:
   - a **deferred** call ends with the deferred flag, no result and no
     output; nothing ran. The other calls of the batch proceed. The
     call is pending when the run ends, with reason `deferred`.
   - a **blocked** call ends with the blocked flag and an error output
     carrying the decision's reason, `call blocked` when it gave none.
   - a call naming **no tool** ends with the error `unknown tool
     "<name>"`.
   - a call whose arguments are **not a JSON object** ends with the
     error `invalid arguments: expected a JSON object`.

   A settled call's error output is rendered as agenttool RFC 0001
   renders a returned error, `Error: ` and the message, once. A
   decision's terminate hint rides on the result of a call settled
   here, so a policy can refuse a call and end the run in one decision;
   it rides on the result of an allowed call the same way, once the
   tool has run, and an override by the after-call hook does not clear
   it.

A hook that defers one call can defer the rest of the batch and hold
all of it for the answer, since it sees the whole batch and runs
before any call executes. A decision's **note** is text the model sees
with the result: for an allowed call it is appended after the batch's
outputs as a developer message, so the model reads the result and the
note together in the same turn; a blocked call carries its reason
instead.

The decision hook sees one call at a time, a nested call included, so
a policy MAY keep state without a lock of its own.

#### Execution

The calls preflight did not settle are handed to the executor as one
batch, with the configuration's execution mode and bound and each
call's arguments as decided. The executor's rules are agenttool RFC
0001's: results matched by call, completion order free, one sequential
tool serialising the batch, a shared resource serialising its calls,
cancellation waited for. A `tool_dispatch` is raised as each call is
handed to its tool, after it has taken a slot in the bound and its turn
in a serial batch or a resource chain, and before the tool runs; a call
the cut reaches first never reached a tool and raises none. It is
raised on the call's own goroutine, serialised with the run's events,
and the executor waits on it, so a recorder has the dispatch durable
before the side effect. A consumer that fails on it stops the call
before its tool runs and fails the run as any delivery failure does,
with the call pending as `aborted` and no side effect behind it. A
`tool_update` is raised for each progress update a tool sends, and
each call is settled as it completes, in completion order:

1. The **after-call hook** sees the call, its result and its error,
   and MAY replace either. An override replaces the result before
   `tool_end` is delivered and before the output is appended, so no
   consumer and no recorder sees what the tool returned: bytes cut
   here exist nowhere afterwards. It runs for every call but a blocked
   one: a call that settled in preflight with no tool or with bad
   arguments, an executed call, and a call a cancellation cut off,
   which it sees with the cancellation as the error. It does not run
   for a call a failure cut off, since the run has already failed. A
   hook error fails the run.
2. An error, from the tool or the override, becomes the error output
   the model sees, with the result's details and terminate hint kept.
3. `tool_end` is raised with the result as the model will see it, the
   error beside it, and the blocked and deferred flags.

Once every call has settled, the outputs are appended to the transcript
in the **model's order**, whatever order they completed in, each with
its item events, and then the notes of the decisions, in the same
order. A deferred call has no output. A batch that a cancellation or a
failure cuts appends the outputs of the calls that finished and drops
their notes. A call is **finished** when it settled with a result of
its own, from its tool or the after-call hook, and its `tool_end` was
raised; a call whose after-call hook failed has no result and is cut.
A failure stops the batch's tools through their context, with the
failure as the cancellation's cause, and the loop still receives every
result the executor holds, as it does under a cancellation. A call
that returns a result of its own before the cancellation reaches it is
settled through the after-call hook as any other, since the hook is
the point past which nobody sees what a tool returned, and is then
finished and its output appended, or cut if the hook refuses it too.
When the failure was the hook's own, no such result can be judged, and
every call that returns one is cut. One that returns the cancellation
is cut either way. A tool's `tool_end` therefore follows the tool's
return, and the events of a nested call the tool makes on its way out
precede it.

The executor is given the configuration's execution mode and bound,
the harness's tool recorder when it has one, so a tool that writes a
record while it runs, as agenttool RFC 0001 lets it, reaches the host
with the call on the context, and the start callback that raises
`tool_dispatch`.

The context every hook and tool of the batch runs under carries the run
ID, a snapshot of the working transcript as it stood when the batch
started with the batch's calls included, and the invoker for
[nested calls](#nested-calls). A tool that composes another agent seeds
it from the conversation without the host threading anything through.

### Ending a run

Exactly one `run_end` is raised per run and nothing follows it. It
carries the items the run appended, a **reason**, a **cause** when the
reason is `stopped`, an **error** when there is one, and the **pending
calls** with why each is pending.

| reason | meaning | error |
| --- | --- | --- |
| `done` | the model answered with no tool calls and no queued follow-up kept the run going | none |
| `stopped` | the loop chose not to call the model again; `cause` says why | the guard's error when the cause is `guard`, otherwise none |
| `input_required` | the decision hook deferred one or more calls to the caller; they are listed as pending | none |
| `aborted` | the run's context was cancelled | the cancellation's cause, or the plain cancellation when it has none; joined with the error of a consumer or a hook that failed for a reason of its own while the run was being cancelled |
| `error` | the model, a hook or a consumer failed | that failure |

| cause | meaning |
| --- | --- |
| `max_turns` | the turn limit was reached before another model call |
| `hook` | the stop hook ended the run |
| `guard` | the stop hook ended the run with an error marked as a guard's; the error is on the end event |
| `terminate` | every result of the batch set the terminate hint |
| `partial_terminate` | some results of the batch set it and others did not |
| `refused` | an answer on resume asked the run to end without calling the model |

The reason is decided as follows. A phase that ends the run on its own
terms — the turn limit, a deferred call, the stop hook, the terminate
hint, a refusal on resume, the cancellation check at the top of a turn
or inside a retry delay — names its reason and cause, and that stands.
Otherwise, if the run's context is cancelled when the run winds down,
the reason is `aborted`, with the cancellation's cause as the error
and, when the failure that ended the run was not that cancellation, the
failure joined to it. Otherwise the reason is `error` with the failure.
Within a turn the phases run in order, so a deferred call is found
before the stop hook is asked and the stop hook before the terminate
hint is read.

A **pending call** is a function call in the transcript with no output,
in transcript order, each with a reason:

| pending reason | meaning |
| --- | --- |
| `deferred` | the decision hook handed the call to the caller in this run and nothing has answered it. The tool did not run |
| `aborted` | the call was appended by this run and the run was cancelled or failed before its output was appended: while it was in flight, after its `tool_start`, or before it was reached, when the failure came earlier in the batch. The tool may have run to completion, so its side effect may have happened |
| `unknown` | the call was in the transcript the run was given, so the loop cannot say whether it ran. A session recorded with dispatch entries can. A call approved on resume and then cut off reads `unknown` too, since the run did not append it, although the run knows it started |

The pending list is empty for `done` and `stopped`. Whatever ended the
run, the calls without an output are the caller's to answer, and the
transcript is valid input again once each has one.

## Pending calls and resume

A run that ends with pending calls leaves the transcript invalid as
input, and the loop MUST refuse to start another run over it until the
calls are answered. There is one path for answering, whether a hook
deferred the calls or a cancellation cut them off, and it is the
**resume**.

An **answer** names one pending call and is one of:

- an **output** the caller produced: a refusal as text, or the result
  of a call the caller ran itself. It is appended as the call's
  function call output.
- an **approval**, optionally with replacement arguments: the loop runs
  the call itself.

An answer MAY carry a **note**, what the person said when answering,
and **who decided** it in the session format's terms (`human`,
`policy`, `agent`). There is no default decider: a policy engine
answers as often as a person does, so an answer that names nobody is
recorded as an anonymous decision rather than guessed at. An answer MAY
ask the run to **terminate** once every answer is in, for a refusal
that should end the turn so the user can say what to do instead; the
outputs are still appended, so the transcript stays valid.

A resume MUST answer every pending call exactly once and MUST NOT name
a call that is not pending. It then runs as follows, and its source is
`resume`:

1. `run_start`.
2. The outputs of the answers that carry one are appended with their
   item events, then the notes of those answers as user messages.
3. The approved calls run as **one batch before the first turn**, with
   the decision hook skipped because the decision has been made: every
   `tool_start` is raised first, in the order the answers name the
   calls, each carrying a decision holding the caller's arguments, note
   and decider and nothing else; the tool events carry turn 0; the
   execution mode and bound apply; the after-call hook runs; the
   outputs are appended in the order the answers name the calls, not
   in transcript order, and their notes after them as user messages.
4. If no answer asked the run to terminate and every result of the
   approved batch set terminate, the run ends with reason `stopped` and
   cause `terminate`, or `partial_terminate` when some did.
5. When there was an approved batch, items steered in while the caller
   was deciding are appended, as after any batch. A resume with outputs
   alone leaves them for the first turn's batch.
6. If any answer asked the run to terminate, it ends with reason
   `stopped` and cause `refused`, whatever the approved batch's results
   said.
7. Otherwise the first turn begins.

A prompt that opens with a function call output for each pending call
is accepted as a resume that also carries a message: the outputs are
appended ahead of the message, so the next model call sees the answers
and the message together. A leading output for a call that is not
pending, or a prompt that leaves a pending call unanswered, is refused.

An agent given a transcript to start from — a stored session being
resumed — derives its pending calls from that transcript: every
function call without an output is pending with reason `unknown`, and
the agent refuses to run until they are answered. Replacing the
transcript re-derives them the same way: whatever the old transcript
was waiting on is forgotten.

## Cancellation

The run's context is the interrupt. Cancelling it reaches the model
stream and the running tools through their contexts, as agenttool RFC
0001 says a tool is stopped, and the run ends with reason `aborted`.

- A cancellation MAY carry a **cause**: a rule that matched, an advisor
  that raised a blocker, a coordinator that cancelled a job, a user
  pressing a key. The cause is what the run ends with on its error, so
  a recorder writes it as the run's end and a product counts why its
  runs were cut; a cancellation with no cause ends with the plain
  cancellation error. The tools of the run see the plain cancellation
  from their own context either way.
- A batch cut off is settled as follows, and this is the rule the
  motivation's first misreading breaks. Every call that had its
  `tool_start` and has not ended gets its `tool_end` with the
  cancellation as its error, so the two are always paired, and a
  consumer or a hook that fails on one of those does not deprive the
  others of theirs: the call it failed on is ended without the hook,
  the remaining cut calls skip the hook, and the failure is reported
  once every call has ended. The outputs
  of the calls that **finished** before the cut — settled with a result
  of their own rather than the cancellation — are appended in the
  batch's order, so a call that ran to completion is answered in the
  transcript and only the calls the cancellation actually cut off are
  left pending, with reason `aborted`. The executor is waited for, so
  every result it holds is received before the run ends.
- The transcript holds only completed items. An output item the stream
  had opened and not completed is dropped, as under a retry.
- `run_end` follows every `tool_end` the cancellation produced and is
  the last event. A consumer that delivers `run_end` as soon as it sees
  the cancellation, before the cut-off calls have ended, is not
  conforming.
- The queues are untouched: anything steered or queued and not yet
  appended goes to the next run.

Every event a cancellation leaves behind — the `tool_end` of each
cut-off call, the appended outputs, the `run_end` — MUST be delivered
to the consumer, and an agent MUST deliver it with a context whose
cancellation is lifted, so a subscriber that writes durable state has a
context it can use. A subscriber that fails for a reason of its own
while the run is being cancelled ends it with reason `aborted` and its
error joined to the cancellation on the end event, so the failure rides
on the abort rather than vanishing; whether it should also be reported
on its own is open (#83).

## Events

Every event carries the run ID, and every event of a turn carries the
turn number. The catalogue, with the members beyond those two:

| event | members | when |
| --- | --- | --- |
| `run_start` | `source`, `trigger` | first event of a run |
| `turn_start` | `request`, `inputs` | after the request is built and before it is sent |
| `model_retry` | `attempt`, `error`, `delay`, `request` | after an uncommitted attempt failed and before the next is sent |
| `model_blocked` | `request`, `error` | in place of `turn_start` when the before-model-call hook refused; the last event before `run_end` |
| `item_start` | `item`, `response_id`, `hidden` | an item entering the transcript: an input as it is appended, an output item as the stream opens it |
| `item_update` | `item`, `stream`, `response_id` | one wire event of an output item, with the item as accumulated |
| `item_end` | `item`, `response_id`, `hidden` | the item is complete and in the transcript |
| `response_end` | `response` | the stream ended; before any tool of the turn runs |
| `tool_start` | `call_id`, `name`, `args`, `decision`, `parent` | after preflight, in the model's order |
| `tool_dispatch` | `call_id`, `name`, `parent` | the call has been handed to its tool, before the tool runs; after its `tool_start` and before its `tool_end` |
| `tool_update` | `call_id`, `name`, `partial` | a progress update from a running tool |
| `tool_end` | `call_id`, `name`, `result`, `error`, `blocked`, `deferred`, `parent` | the call settled, in completion order |
| `turn_end` | `response`, `tool_results` | after the batch's outputs are appended |
| `run_end` | `items`, `reason`, `cause`, `error`, `pending` | last event of a run |
| `queued` | `item`, `mode`, `hidden` | an item accepted into a queue; belongs to no run |

`response_id` is the provider's response identifier for an item the
model produced and empty for an item the loop appended. `stream` is
the Open Responses wire event verbatim, so a front switches on the same
types it would use against a remote server; model streaming is not
re-modelled. `parent` is the ID of the call whose tool made this one as
a [nested call](#nested-calls) and empty for a call the model made.
`decision` is what the decision hook returned, none when there was no
hook or it returned none, and for an approved call the caller's answer.

### Order

The events of a run appear in this order; `*` is zero or more, `?` at
most one, and `queued` MAY appear between any two events:

```
queued*
run_start
(item_start item_end)*                  prompts; or a resume's outputs and notes
batch?                                  a resume's approved batch, turn 0
(item_start item_end)*                  its outputs and notes, then steered items
turn*:
  (item_start item_end)*                before-turn items
  turn_start | model_blocked            the latter ends the run
  ((item_start item_update*)* model_retry)*  attempts that failed before committing
  (item_start item_update* item_end)*   output items; item_end after the item is done
  response_end
  batch
  turn_end
  (item_start item_end)*                steered items; then follow-ups when no calls
run_end
queued*

batch:
  (tool_start tool_end?)+               preflight, in order; tool_end here only for a
                                        call that will not run
  (tool_dispatch | tool_update | tool_end | nested)*   completion order; a call's
                                        tool_dispatch precedes its tool_end
  (item_start item_end)*                outputs, in order; then notes
nested:
  tool_start (tool_dispatch | tool_update | nested)* tool_end   a call a tool made, parent set
```

### Invariants

A conforming loop holds these over every run, however it ends:

- Exactly one `run_end` per run, and no event of the run follows it.
  A `queued` report belongs to no run and MAY precede `run_start` or
  follow `run_end`.
- Every `tool_start` has exactly one `tool_end` with the same call ID,
  whether the call ran, was blocked, was deferred, or was cut off
  before or during execution by a cancellation or by a failure. A
  batch a hook or a consumer fails inside is settled as a cancelled
  one is: every call that has not ended gets its `tool_end` carrying
  the failure, the outputs of the calls that finished are appended,
  and the rest are pending as `aborted`. A `tool_end` counts as raised
  when the loop delivers it, whether or not every consumer took it, so
  a consumer that fails on one never earns the call a second.
- `item_end` for an output item follows the wire event that completed
  it; a partial item is never announced as complete. The item is in the
  transcript when `item_end` is delivered.
- Outputs are appended in the model's order; `tool_end` arrives in
  completion order. A consumer correlates the two by call ID, never by
  position.
- `response_end` precedes every `tool_start` of its turn, and every
  output item of the response has had its `item_end` before it.
- `turn_start` is delivered once per turn and `response_end` at most
  once, however many attempts the turn took: a turn whose last attempt
  ended without a response has none.
- Every event after a cancellation that the rules above say is raised
  is delivered.
- Events are delivered serially, never concurrently: the events of
  the run and the events a nested call raises from a tool's own
  goroutine pass through one delivery, so a consumer is never entered
  twice at once. Which goroutine delivers an event is not specified.
- A failure truncates the sequence at its phase; the events the
  cancellation section says are raised for a cut batch follow, and then
  `run_end`.

## Delivery

The loop comes in two forms over one event stream.

The **low-level loop** is observational: it yields events in order and
does not wait for the consumer between phases. It MAY run ahead of the
consumer by a bounded number of events, 256 in the reference, and MUST
apply backpressure after that. A consumer that stops reading cancels
the run, and the call that stops reading MUST block until the run has
wound down, so nothing is left running behind a consumer that walked
away. The `run_end` is the single terminal and its error is where a
failure is reported; the low-level loop returns nothing else.

The **agent** delivers every event synchronously to its subscribers, in
registration order, and every event is a **barrier**: the loop does not
move to the next phase until each subscriber has returned. Tool
preflight for a turn therefore does not start until every subscriber
has returned for the response's items, and a prompt settles only after
the `run_end` subscribers finish, which is what lets a session recorder
have an item durable before the tool that reads it runs. One event is
delivered at a time, whichever goroutine raised it. A subscriber that
subscribes during a run takes effect from the next event.

A subscriber that returns an error ends the run with reason `error`,
and the failure has these consequences, stated because a second
implementation would otherwise choose differently:

- the event does not reach the subscribers after the one that failed;
- an error on `run_end` is ignored, since the run has already ended;
- an error on a `queued` report delivered ahead of a run's event ends
  the run before that event is delivered to anyone;
- an error on `model_blocked` replaces the hook's error as the run's;
- an error on `tool_dispatch` stops that call before its tool runs, and
  the run ends as for any delivery failure, with the call pending.

A subscriber MUST NOT start a run from inside a delivery: a prompt, a
continue or a resume made there is refused as one made during a run,
`run_end` included, since the run is active until its delivery
returns. Queueing an item never blocks and is safe from a subscriber.

An agent runs **one run at a time**: a prompt, a continue, a resume, a
configuration change or a transcript change while a run is active is
refused. The agent's **state** is a snapshot: the transcript, whether a
run is active and which turn it is on, the queues' contents, and the
pending calls with their reasons.

## Queues

An agent accepts items outside a run's own input and appends them at a
stated point:

- **steer**: the item is appended after the current batch, before the
  next model call. When the agent is idle it is consumed by the next
  run at the same point: after that run's first batch, or on a resume
  after the approved batch, so a steered item never precedes the
  prompt that starts a run.
- **follow-up**: the item is appended when the run would otherwise
  end, so the agent keeps going instead of going idle. It is drained
  only after a turn in which the model called no tools, after the
  steered items.

Accepting an item MUST NOT block on the delivery barrier, so steering
from inside a subscriber is safe. Each accepted item is reported as a
`queued` event, with which queue it went into and the run that was in
flight, empty when the agent was idle. The report is not the accept:
the item is queued when the call returns, and the goroutine that owns
delivery reports it at its next event, before anything that item
produces. A run in flight reports it before its next event; an idle
agent at the start of the next run. A host that must not lose an input
therefore writes it before queueing it rather than from the event, and
a subscriber's error cannot refuse the item.

The queues live in memory. An item accepted is in no record until a
run appends it or a subscriber writes it, and it survives a
cancellation, a configuration change and a transcript change but not
the process; a host reads the queues back from the agent's state and
queues them again after a restart. Whether the loop should make them
durable through the record is open (#67).

A steered item is appended with its own item events. A subscriber that
steers in reaction to an event the steered item itself produces feeds
the run forever, and nothing reports it: the loop cannot tell a
reaction from a fresh input.

## Hooks

Every hook is one member of the configuration, called at one moment,
with a stated contract. Each is one field, so two layers that want the
same one silently lose an assignment to each other; a binding SHOULD
provide a way to chain each, with the fold rule the table gives.

| hook | when | sees | returns | an error | chain |
| --- | --- | --- | --- | --- | --- |
| before turn | turn phase 2 | the working transcript | items to append as facts | fails the run | items appended in order; the first error drops them all |
| before model call | turn phase 4, last | the finished request | edits it in place | `model_blocked`, then the run fails | in order, each seeing what the last left; the first error stops |
| output guard | as the stream completes an assistant message | the message | a replacement or none | fails the run | in order, each seeing the last's replacement; the last stands |
| decision (before tool call) | preflight, per call, model order; a nested call as a batch of one | the call, the tool, the arguments, the batch and index | a decision or none | fails the run; returned to the tool for a nested call | the strictest action wins, block over defer over allow; a block ends the chain, a defer does not; rewritten arguments pass to the hooks after; the first reason and decider of the standing action, the first note; terminate if any set it |
| after tool call | as each call settles, blocked calls excepted | the call, the result, the error | an override or none | fails the run; returned to the tool for a nested call | — |
| should stop after turn | turn phase 10 | the response, the results, whether the turn was final, the transcript | stop or not; a guard error stops with cause `guard` | fails the run unless marked as a guard's | in order until one stops; the first error stops |
| transform | request step 2 | a copy of the transcript | the input for this call | fails the run | — |
| filter | request step 3 | the transformed list | what the model sees | — | — |

The stop hook tells a policy stop from a failure by marking its error
as a guard's, which the reference binds as a sentinel the error wraps.
Whether the decision should be returned as data instead is open (#82).
A layer that must see every turn, a meter for one, belongs in a
subscriber rather than in the stop hook, which stops at the first hook
that stops.

The hooks run with the run's context, which carries the run ID and,
for the decision and after-call hooks, the batch's transcript snapshot
and the invoker. A hook MUST NOT make a nested call: the decision hook
takes one call at a time and would wait for itself.

## Nested calls

A tool whose own work is to call other tools — a code-execution kernel
with a loopback bridge — MAY **invoke** one of the turn's tools through
the loop rather than holding a tool set of its own, where the policy,
the events and the record would all be absent. The loop runs the call
as if the model had asked for it under the call in flight:

- the decision hook decides it, seeing a batch of one at index 0; a
  hook that defers it refuses it instead, since a nested call cannot be
  handed to the caller: it belongs to a tool that is running, and the
  error reads `a nested call cannot be deferred to the caller: ` and
  the reason. The decision's note and terminate hint are ignored;
- `tool_start` and `tool_end` are raised with **parent** naming the
  call that made it, serialised with the run's own events;
- the after-call hook MAY override the result;
- the result returned to the invoking tool is the one the model would
  have seen, with the error beside it: a tool that failed, a name no
  tool has, arguments that are not an object, or a refusal whose reason
  is the error;
- a failure of the loop's own around the call — the decision hook
  returning an error, a consumer failing on its `tool_start` or its
  `tool_dispatch`, the after-call hook or a consumer failing as it
  settles — is returned to the invoking tool as the error and does not
  end the run; the tool decides what its own result is. A refused
  `tool_dispatch` stops the nested tool as it stops any, and the
  failure is the invoking tool's to answer rather than the run's. A call whose `tool_start` was raised
  gets its `tool_end` carrying the failure, so the pair holds for a
  nested call too. A decision hook error is returned before
  `tool_start`, so that call raises no events at all.

Nothing is appended to the transcript: a nested call is the work of the
call that made it, costs no items, and its terminate hint means nothing
to the loop. A session recorder writes it as a record entry beside the
parent call. The invoker is on the context of every tool of a batch;
outside a loop, invoking fails with a stated error.

## Composition

The loop never learns a sub-agent concept. It knows a model and a list
of tools, and every composition is one of those two things. A protocol
earns a place only with a serve side and a consume side, so an agent
behind any protocol can be a component of any other agent:

| protocol | serve | consume |
| --- | --- | --- |
| Open Responses | the loop as an adapter | a client as a model |
| MCP | agenttool's server | agenttool's client |
| A2A | the A2A front | the A2A tool |
| in-process | the agent | the agent tool |

An agent stands in four places inside another system:

- **As a model.** The loop served as an Open Responses adapter, so
  another loop points its model at it. Composition over a network with
  no second protocol. The request's input is the conversation, history
  is always inlined, and a previous response ID is rejected because the
  loop keeps no store.
- **As a tool.** A configuration wrapped as a tool. Each call runs a
  **child run** on a fresh transcript, or one seeded from the parent's
  by a function the host supplies. The seed the function sees is the
  batch's snapshot with every unanswered call answered by a
  placeholder output — the call being served says the child is
  answering it, its siblings say they are running alongside — so the
  child starts from valid input. The child's accumulated assistant
  text streams out through the call's progress channel, with the
  child's run ID as details; its events reach the observer alone. The
  final assistant text is the output; when there is none, the last
  output of a run that terminated is, and otherwise the call fails
  with an error naming the cause. A child that failed returns its
  error; one that was cancelled returns the cancellation. The child's
  run ID, the items it added, its reason, cause and pending calls are
  the result's details, so a recorder on the parent links the child
  session. The
  child runs with its own configuration and its own hooks; the
  parent's do not run inside it. A child that ends with calls its own
  hook deferred returns an error that says so and names them, so the
  parent's model sees it as an error output and a host can ask once
  whether any sub-agent needs input (#84). A cancellation on the
  parent reaches the child through the context. An **observer** the
  host installs receives every event of the child run, in order, with
  the parent's call context, which is how one recorder serves every
  level of nesting; a **spawn** callback hands the host the child's
  agent, keyed by the call, so a hub can steer it, cancel it without
  cutting its siblings, or prompt it again once the tool has returned.
- **As a peer.** The loop exposed to A2A callers, with its card derived
  from its name, description and tools; a remote A2A agent wrapped as a
  tool, a task mapped to one call and input-required to a returned
  error the model can answer.
- **As itself, under new settings.** The configuration replaced and the
  transcript kept, so the next run continues the same conversation
  under another agent's instructions, tools and model. This is what an
  agent framework calls a handoff, and it needs no package (#96). A
  recorder writes the switch as a config delta, so the settings any
  response was produced under are on its path. A terminating tool
  result is the usual trigger, with the destination in the result's
  details where the model cannot see it. A transcript holds whatever
  the previous model produced, reasoning items included, and a
  reasoning item carries a signature its own provider issued; a
  handoff that changes the provider MUST drop them, and which component
  owns that rule is open (#91).

A configuration's name and description are the single source for how
an agent presents itself in every one of these.

## The record

A session recorder is a subscriber. This section says which event
carries what agentsession RFC 0001 needs and when, so that a recorder
built against this document and a reader built against that one agree.
Nothing here is required of a loop that has no recorder; it is required
of the events.

| event | entry |
| --- | --- |
| `run_start` | `run` start, with the loop's source as the format's and the trigger joined as `kind:ref`, or whichever is set, as `ref`; an `env` entry when the host supplies one and it changed; the full initial `config` from the configuration's base request when nothing has been written yet |
| `turn_start` | a `config` delta when the request's settings differ from the path's; the request hash is computed here and written on the response. When the host names the parts the request's instructions are composed of, and they join to the instructions sent, the entries carry `instructions_parts`, a delta naming the parts that moved and the others by hash, and `instructions_omitted` for what the host left out; parts that do not join are dropped and the string is written, since the record describes what was sent |
| `model_retry` | a `config` delta when the revised request's settings differ |
| `model_blocked` | a failed `response` carrying the hook's error and the request hash, so the call that was refused is told from one that was made and failed |
| `item_end` | an `item`, with the display flag off for a hidden item. Before a caller-supplied output for a call that was neither dispatched nor rejected, a `reject` decision with the output's text as its reason and the decider the caller named, and `policy` as the decider when the loop refused the call itself in this run, for a name no tool has or arguments that are not an object; before one for a call dispatched in an earlier run, a `proceed` with the decider when one was named |
| `response_end` | the `response`, with `request_hash` when the input the loop sent is the input the recorded path rebuilds, and none otherwise |
| `tool_start` | a `decision`: `reject` with the reason for a block, `call blocked` when it gave none; `hold` for a defer, with the reason when given; `proceed` for a call that was held or whose arguments were rewritten, with the arguments. The decider is the decision's, and `policy` for a call nothing was holding whose decision names nobody. A nested call is a record entry instead |
| `tool_dispatch` | a `dispatch`, durable before the event returns, so the tool runs after it or not at all; nothing for a nested call |
| `tool_end` | a recordable details value as a record entry in its namespace; a nested call's record; for a child run that was not observed, its session written from the items it added and its `link`; an observed child's `link` and session are written by the observer from the child's own events, starting at its `run_start` |
| `turn_end` | nothing of its own |
| `run_end` | first, when a `turn_start` had no `response_end`, a failed `response` carrying the run's error; then `run` end with the reason mapped onto the format's cascade: `done`, `input_required` and `error` as themselves, the error's text as `ref`; `aborted` as `interrupted` with the error's text, since the host asked; `stopped` as `stopped` when the last response made calls, as `done` when it made none, and for a run with no response of its own as `stopped` when it answered a pending call and left none, `aborted` otherwise, with the cause as `ref` |
| a fold the transform reports | a `compaction` naming what was kept and what was pinned, or a record entry for a fold that failed |
| `queued` | nothing today; the format has `queued`, and writing it is open (#67). The loop's follow-up mode is spelled `follow_up` and the format's `followup`; a writer maps the one onto the other |

Three rules follow from the writing discipline of that format and are
met by the delivery rules here. An item is durable before the tool that
reads it runs, because every event is a barrier. A `dispatch` is
durable before the tool runs, because `tool_dispatch` is raised at
hand-off and the executor waits on it; and it is written only for a
call handed to its tool, as agenttool RFC 0001 requires of a harness
that records one, so a call the cut reached first, or one the loop
refused itself, has none and reads as never started. And every entry a
cancellation leaves to write is written, because every event after a
cancellation is delivered with a usable context.

Two facts the record needs are supplied by the caller and carried by
the loop unread: the trigger of a run, and who decided an answer. The
loop learns nothing from either.

## Bindings

### Go

The root module of this repository is the reference loop. The contract
maps onto it as follows:

| Contract | Go |
| --- | --- |
| transcript | `Transcript = openresponses.Items`; `Items` for a fragment |
| hidden item | `Hidden(item)`, `Unhide(item)`; `Hidden` on the item events |
| filter | `Config.Filter`; `DefaultFilter`, `VisibleFilter(types…)` |
| transform | `Config.Transform`; `compact.New`, `compact.NewLocal` as the reference |
| configuration | `Config`; `Config.BaseRequest`, `Config.ResolveTools` |
| tools of the moment | `Config.Tools`, `Config.ToolProvider` |
| execution mode, bound | `Config.ToolExecution` (`ExecParallel`, `ExecSequential`), `Config.MaxParallelTools` |
| turn limit | `Config.MaxTurns` |
| retry policy | `Config.Retry{MaxAttempts, Backoff, Retryable, Revise}`; `DefaultBackoff`, `DefaultRetryable` |
| low-level loop | `Run(ctx, t, prompts, cfg)`, `Continue(ctx, t, cfg)` → `iter.Seq[Event]`; `EventBuffer`; `CanContinue` |
| agent | `Agent`; `New(cfg, opts…)`, `WithTranscript`; `Prompt`, `Continue`, `Resume`, `Steer`, `FollowUp`, `Subscribe`, `Abort`, `AbortCause`, `WaitForIdle`, `State`, `SetConfig`, `SetTranscript`, `Config` |
| refusals before a run | `ErrNoPrompt`, `ErrCannotContinue`, `ErrNoModel`, `ErrInputRequired`, `ErrNotPending`, `ErrRunning` |
| run ID, trigger, transcript on the context | `ContextWithRunID`/`RunIDFromContext`, `ContextWithTrigger`/`TriggerFromContext`, `ContextWithTranscript`/`TranscriptFromContext` |
| source | `Source`: `SourceInput`, `SourceResume` |
| events | `Event` with `EventType()`; `RunStart`, `TurnStart`, `ModelRetry`, `ModelBlocked`, `ItemStart`, `ItemUpdate`, `ItemEnd`, `ResponseEnd`, `ToolStart`, `ToolDispatch`, `ToolUpdate`, `ToolEnd`, `TurnEnd`, `RunEnd`, `Queued`; the `Event*` name constants |
| tool recorder | `Config.ToolRecorder`, the executor's recorder for every batch; `session.Recorder.RecordFunc` is the value a session recorder offers |
| reason, cause | `Reason` (`ReasonDone`, `ReasonStopped`, `ReasonInputRequired`, `ReasonAborted`, `ReasonError`); `StopCause` (`StopMaxTurns`, `StopHook`, `StopGuard`, `StopTerminate`, `StopPartialTerminate`, `StopRefused`) |
| pending call | `PendingCall{Call, Reason}`; `PendingReason` (`PendingDeferred`, `PendingAborted`, `PendingUnknown`); `PendingCalls` |
| decision | `ToolDecision{Action, Reason, Terminate, Args, By, Note}`; `ToolAction` (`Allow`, `Block`, `Defer`) |
| answer | `Answer{CallID, Output, Args, Note, Terminate, By}`; `Output`, `Approve`, `ApproveWith`, `Refuse`, `WithNote`, `WithBy`; `ContextWithDeciders`/`DeciderFromContext` for a host driving `Run` |
| hooks | `Config.BeforeTurn`, `BeforeModelCall`, `OutputGuard`, `BeforeToolCall`, `AfterToolCall`, `ShouldStopAfterTurn` |
| guard stop | an error wrapping `ErrGuard` from `ShouldStopAfterTurn` |
| chains | `ChainBeforeTurn`, `ChainBeforeModelCall`, `ChainOutputGuard`, `ChainBeforeToolCall`, `ChainShouldStopAfterTurn` |
| nested call | `Invoke(ctx, name, args)`; `ErrNoInvoker`; `Parent` on the tool events |
| queue mode | `QueueMode`: `QueueSteer`, `QueueFollowUp` |
| the loop as a model | `front/responses.New(cfg)` → `openresponses.Adapter` |
| the loop as a tool | `tools/agent.New(cfg, opts…)` → `agenttool.Tool`; `ChildInfo`; `InputRequiredError`; `WithArgs`, `WithStrictArgs`, `WithTranscript`, `WithObserver`, `WithSpawn`, `WithRunContext`, `WithNoAnswer`, `WithToolName`; `ContextWithRetry` |
| the loop as a peer | `front/a2a.New(cfg)`, `front/a2a.AgentCard`; `tools/a2a.New(client, card)` |
| the record | `session.Recorder`; `Start`, `Resume`, `Continue`, `Attach`, `Handle`, `Observe`, `ChildContext`, `Fold`, `Annotate`, `RecordFunc`; `session.RequestHash` |

Every error the package returns to its caller, sentinel or wrapped,
begins with `agentturn:`; the error texts a call's output carries,
`unknown tool "<name>"` and the others the batch section gives, are
the model's and are not prefixed. A panic in a tool is recovered by
agenttool's executor
into an error the model sees; a panic in a hook or a subscriber is not
recovered and unwinds without a `run_end`.


## Conformance

**A conforming loop** builds each request by the stated procedure with
the loop-owned members fixed and nothing that varies per run; appends
only, and never edits the transcript; decides every call of a batch in
the model's order before any executes; raises `tool_start` and exactly
one `tool_end` per call; appends outputs in the model's order and then
the notes; announces an item complete only when it is; delivers
`turn_start` and `response_end` once per turn; retries only an
uncommitted attempt and drops what it held; raises exactly one
`run_end`, last, with the reason the phase order gives, the cause, the
error and the pending calls with their reasons; refuses to run over a
transcript with an unanswered call unless the run's input answers it;
answers pending calls only through a resume that answers all of them;
settles a batch cut off by a cancellation or a failure with a
`tool_end` per call and the outputs of the calls that finished; and
delivers every event a cancellation leaves behind.

**A conforming agent** delivers events synchronously as barriers, one
at a time, in registration order; refuses a second run while one is
active; accepts a queued item without blocking and reports it at the
next event; delivers post-cancellation events with the cancellation
lifted; and ends a run with reason `error` when a subscriber fails.

**A conforming front** feeds prompts, resumes and queued items in and
consumes events out, and nothing else; correlates `tool_end` with
`tool_start` by call ID; renders withheld text on `item_end` and not
from deltas; and answers pending calls through the resume.

**A conforming recorder** writes the entries the record table gives
from the events it names, writes the `dispatch` from `tool_dispatch`
and from nothing earlier, and declines a request hash for an input the
path cannot rebuild.

### The scenario corpus

The reference's own tests hold the rules three defects found on the
way to this draft broke: a batch a hook or a consumer fails inside
pairs every call, a decision's terminate hint on an allowed call, and
a wire error event before the answer as a retried attempt (#101,
#102, #103).

Draft 0.2 will add `testdata/loop/`, a directory of scenarios driven by
a scripted model with no network, each holding the configuration in
the terms of this document, the model's scripted responses, the tools'
scripted results, the expected event sequence with each event's
members that matter, and the expected end: reason, cause, error, the
pending calls with their reasons, and the transcript. The set the
motivation's misreadings ask for: a clean `done`; tools then `done`; a
deferred call and its resume by output and by approval; a cancellation
mid-batch with one call finished and one cut; a turn limit; unanimous
and partial terminate; a retry that delivers nothing and then succeeds,
and one that held a reasoning item; a subscriber that fails during a
cancellation; a blocked call with a terminate hint; a nested call; a
steered item and a follow-up. A second implementation runs the corpus
and compares event by event.

## Versioning

This document is versioned with the Go module. A draft number changes
when a rule is added or changed; a rule's removal or a change that
makes a conforming loop non-conforming is a breaking change to the
module and is listed in the changelog as one.

## Prior art

- **pi-agent-core** is the loop this one was modelled on: a run and a
  turn as the vocabulary, subscribers awaited as barriers, steering and
  follow-up queues, and the rule that a steered item joins after the
  current batch. It has no decision seam that defers, no resume, and
  its transcript is its own message shape.
- **Claude Code** and the **Claude Agent SDK** have hooks before and
  after a tool call with allow, deny and ask, and a sub-agent as a tool
  that runs on a fresh transcript. Their transcript is the provider's
  content blocks and their session format changes between releases.
- **The OpenAI Agents SDK** names the handoff, an agent switch that
  keeps the conversation, and the input filter that decides what the
  receiver sees; this document's fourth composition is that pattern
  without a package.
- **LangGraph** makes durable execution the centre: every step is
  checkpointed and a crashed run resumes at its last checkpoint. This
  document takes the resume rule and leaves the checkpointing to the
  session format and the record.
- **OpenHands SDK** gives a tool an interrupt and a conversation a
  resource lock; agenttool RFC 0001 declines both and says why.
- **Codex** records its turn context and events as Responses items,
  which is where the choice of the wire item as the transcript comes
  from.
- **agenttool RFC 0001** and **agentsession RFC 0001** are the two
  documents this one sits between, and the source of its notation.

## Open questions

- **A `proceed` with no `dispatch` after it** (agentsession #78). With
  the `dispatch` written at hand-off, an approval or a rewrite that a
  cut or the loop's own refusal overtakes leaves a `proceed` decision
  that no `dispatch` follows, which agentsession RFC 0001's `decision`
  section does not admit and its verifier does not check. Whether the
  format admits the shape, with a `reject` or the run's end saying the
  call did not go, or a writer holds the `proceed` until the dispatch
  and loses what the overtaken approval said, is agentsession's to
  rule; the recorder writes the first shape until it does.
- **Who closes a provided tool** (#95). The host closes what it built,
  and under a tool provider the host never holds the value. Documenting
  that a provider caches per session, or a release hook called after
  each turn, are the options.
- **Durable queues** (#67). An accepted item is in no record until a
  run appends it. The format has a `queued` entry and an inbox rule; a
  recorder writing one from the `queued` event and a resume draining
  it would make the inbox durable from accept to append.
- **The stop decision as data** (#82). A guard stop is an error
  wrapping a sentinel, and a hook that returns a bare error records a
  failure and is retried by a supervisor that honours policy stops.
  Returning a decision with a cause instead, with the sentinel kept as
  sugar, is proposed.
- **The failure beside a cancellation** (#83). A subscriber's failure
  during a cancellation is joined into the cancellation on the end
  event and nothing reads it. A separate member for it is proposed.
- **Input required as an interface** (#84). The child tools' errors
  match one sentinel, and the pending calls are reached by a type
  switch per implementation. An interface both satisfy is proposed.
- **The handoff** (#96, #91). The fourth composition is the cheapest
  and the least documented, and a handoff that changes the provider
  sends the previous model's reasoning items to the new one, which
  rejects their signatures. The loop is where both configurations are
  visible at once; whether it drops them, or names the rule and offers
  the helper, or the item type carries the rule, is open. Every route
  that trims the history mid-path costs the responses after it their
  request hash, which is the format's question.
- **A second execution under one call** (#87). A child tool builds a
  fresh agent per call while the recorder continues the child session
  at its leaf, so the second run's path rebuilds a context the child
  did not read and its response cannot be hashed. Seeding the second
  execution from the recorded context, or opening a new root, are the
  two coherent answers.
- **A fold identifying itself** (#88). The local fold's summary request
  is a request with no tools and no instructions, and a replay tells a
  fold from a turn by that shape. A marker on the context the fold
  already passes is proposed.
- **Why a request was not hashed** (#92). A recorder declines a hash it
  cannot stand behind, and a response with no hash has three
  indistinguishable causes. A member on the response entry saying why,
  or a callback at the moment, is proposed.
- **A run the process died inside** (#94). The format has
  `interrupted` for it and the library can build the missing end; the
  resume writes nothing, so a crash and a branch taken mid-run are the
  same shape. Closing the open run at resume, or returning it for the
  caller to close, is open.
- **A front that serves a person** (#80). The Open Responses front is
  agent-as-a-model by design. Steering, cancellation, a deferred call's
  question and a tool's progress have no carrier over a socket. A
  second front with a written-down, versioned extension vocabulary and
  a way for the server to advertise it is proposed.
