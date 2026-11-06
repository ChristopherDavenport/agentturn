# RFC 0001: Agent Loop

Status: draft 0.8
Author: Christopher Davenport
Discussion: to be opened against this repository, then proposed to the
Open Responses community as a companion specification. The Go module at
its root is the reference loop and `testdata/loop/` the scenarios any
loop is checked against; agenttool RFC 0001 is the tool contract it
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

Each draft describes the Go module as released with it; [Changes in
draft 0.8](#changes-in-draft-08) lists what this draft added. Where the
module falls short of what is written here, or where a rule is
contested, the shortfall is under open questions with the issue that
tracks it.

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
- An item the caller supplies MAY carry its **provenance**: where text
  the person did not write came from and whether it is trusted, for an
  item the host or a hook composed, such as an instruction file reached
  mid-session, a prompt template or an MCP server's prompt. It names
  the instruction layers a message was composed from, what sent the
  item, in a trigger's terms, and the item's trust labels, each
  optional. The loop appends the item unwrapped, as a hidden one, and
  carries the provenance unread on its events; the item's own source
  stands over the trigger it was handed in under, for that item alone.
- The **filter** removes the items the model must not see before every
  call. The default drops every item whose type carries a slug prefix
  and every null; a configuration MAY keep listed extension types. An
  item the filter removes is out of the model's context and a recorder
  writes it as a record entry, never as context. Marking such an item
  hidden adds nothing.
- An item or content part under a provider's slug, such as Claude's
  `anthropic.redacted_thinking` block, is the model's output, not a
  harness's: the filter keeps it and the request carries it as the
  transcript holds it, for whichever model is configured. Leaving it
  out for another provider is the adapter's, as openresponses
  docs/extensions.md section 1.6 says, and not the loop's, so the
  request and its hash stay what the record rebuilds; a host switching
  models needs no attribution for it. An adapter that forwards to an
  upstream it cannot see behind, such as openresponses' `ClientAdapter`,
  applies that rule only once told which providers' slugs the upstream
  accepts, so a host configures it so (#285).

## The configuration

A configuration is what a run reads and nothing else is. It carries:

| member | what the loop does with it |
| --- | --- |
| name, description | how the agent presents itself when composed: the tool name and description as a tool, the card as a peer, the server info over a protocol. The loop itself reads neither |
| model | required; a run over a configuration with no model is refused |
| model name, instructions, reasoning, text | the corresponding request members, set on every request when set |
| request base | every other request member — tool choice, parallel tool calls, max output tokens, temperature, truncation, include, cache key, service tier, passthrough extras — copied onto every request. The loop owns input, tools, store, stream, stream options and previous response ID and overwrites them |
| tools, or a tool provider | the tools of the moment, resolved once per turn and wherever else the loop needs them |
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
0001 says. A provider is called whenever the tools are resolved: once
for each turn, once for the batch a resume approved, once for each
approval a resume holds to the replay rule, and whenever a host or a
recorder asks for the request base, so a provider called twice for one
turn's worth of work is no defect. A provider that builds tools owning
a resource is responsible for releasing them; it SHOULD build them
once per session and return the same values on every call (#95).

A configuration MAY be replaced between runs. The transcript is kept,
so the next run continues the same conversation under other
instructions, tools, hooks or model. This is the fourth way an agent
composes, [below](#composition): the loop needs nothing more for it,
and the switch around it is the host's.

## A run

### Starting a run

A run begins in one of two ways, and its **source** says which:

- **input**: new items are appended and the model is called. The
  low-level loop takes the items as its prompts, or continues from a
  transcript that already ends with something the model can answer: a
  message that is not the assistant's, or a function call output. An
  agent continues, too, from a transcript whose steered items queued
  ahead of the first model call end with such an item, so an input
  handed back as a delivery starts the run that takes it.
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
to the context, a kind, a reference and the layer that took the input,
in the caller's own terms, with any richer facts of the caller's about
the firing, when it was due or which attempt it is, is
carried on `run_start` and on the `item_end` of each item the run was
prompted with, and read by nothing in the loop; it is how a
recorder learns what caused the run without the loop learning what a
cron job is. The richer facts are written beside the kind, the
reference and the layer, so a loop MUST refuse, where the trigger
enters, a run or a queued item whose facts name one of those three or
cannot be encoded as JSON, rather than let a recorder fail a later run
over it.

### The turn

A turn proceeds in these phases, in this order. Each names the events
it raises and the hooks it calls; the events are defined
[below](#events).

1. **Cancellation.** If the run's context is cancelled, it ends with
   reason `aborted`.
2. **Before the turn.** The before-turn hook sees the working
   transcript and the turn's inputs so far, each item with its
   **input mode**, how it arrived, as described under phase 5, and MAY
   return items, which
   the loop appends to the transcript with their item events as it
   appends any input. They are then facts about the transcript: a
   recorded session rebuilds the request they were part of, which
   injection through the transform cannot give. A hook error ends the
   run with reason `error`, or, marked as a guard's, stops it with cause
   `guard` before the turn has a `turn_start`.
3. **Tools of the moment** are resolved and, for a provided list,
   validated. A resume's approved batch resolves them the same way and
   skips the validation.
4. **The request** is built as the [next section](#the-request) says.
   The before-model-call hook runs on it last. A hook that refuses
   raises `model_blocked` carrying the request as built, then stops the
   run with cause `guard` when its error is marked as a guard's and
   ends it with reason `error` otherwise; no call was made, and the
   event is what tells a recorder that from one that was made and
   failed.
5. **`turn_start`** carries the request exactly as it will be sent,
   the run's quarantine as the request was built under it
   (**quarantined**, the items it holds, a copy), and
   the **inputs** of the turn: the items appended since the previous
   turn's response, or since the run started for the first turn. They
   are what this turn's response answers. Beside them it carries the
   same items as **arrived**, each with its **input mode**, which says
   how the item came, and on a run's first turn also the items the
   transcript already ended with when the run started that the model
   did not produce, after its last output: a run that continues from
   a transcript answers them first, and the inputs, which name only
   what the run appended, never held them. The modes are:
   - `prompt`: an item the run was prompted with;
   - `resume`: an output or a note a resume appended, or one of the
     leading outputs a prompt opened with for the pending calls;
   - `steer`: an item drained from the steer queue;
   - `follow_up`: an item drained from the follow-up queue;
   - `deliver`: an item handed in by a [delivery](#queues), which
     queues it as a steer; the mode is what tells the two apart;
   - `hook`: an item the before-turn hook appended;
   - `tool`: a function call output the loop appended for a call it
     ran, the previous batch's or a resume's approved batch's, and the
     note a decision attached after the batch's outputs;
   - `continued`: an item already in the transcript, after its last
     model output, when the run started: a function call output, a
     user or developer message or a namespaced custom item, such as the
     outputs of the run before, which a continue answers.
   An item of mode `prompt` carries the run's trigger, and one of mode
   `steer`, `follow_up` or `deliver` the trigger its `queued` report
   carried; the others carry none. The before-turn hook of phase 2
   sees the same list without the `hook` items, since it has not run
   yet, so a layer that must act when a user's message arrives, or
   once on the turn that answers a handoff, reads the mode rather than
   inferring it from the transcript's tail, where a steer followed by
   a delivered output looks like a resume and an answer delivered as a
   message looks like the user's (#157).
6. **The model call**, as the [model call section](#the-model-call)
   says: item events as the stream delivers output items, an attempt
   retried under the retry policy when it failed before it began, and
   `response_end` with the folded response once the stream ends.
   Every completed output item is in the transcript when `response_end`
   is delivered. A model failure ends the run with reason `error`. An
   output guard error marked as a guard's stops the run with cause
   `guard` mid-stream: the turn's `response_end` is marked withheld,
   the withheld response's calls are closed, and nothing below runs.
7. **The batch**, as the [batch section](#the-batch) says: the
   function calls of the response are decided one at a time in the
   model's order, then executed, then their outputs appended in the
   model's order.
8. **`turn_end`** carries the folded response and the tool results in
   the model's order. It is raised whether or not the batch left calls
   pending; a deferred call's place holds an empty result, so the list
   is as long as the batch. A batch a cancellation or a failure cut off
   has no `turn_end`: it is settled as the
   [cancellation section](#cancellation) says, and the run ends.
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
12. **Limits.** If the run has taken its turn limit's worth of turns,
    it ends with reason `stopped` and cause `max_turns` when the model
    called tools or anything is queued, and with reason `done`
    otherwise. What is queued is not drained: it stays queued for the
    next run, which takes it after its prompt.
13. **Queues.** Items steered in while the turn ran are appended, with
    their item events. If the model called no tools, the follow-up
    queue is drained after them; if nothing was queued, the run ends
    with reason `done`. Otherwise the next turn begins at phase 1.

Phases 9 to 12 decide a stop before phase 13 drains anything, so an
item steered during a turn that ends the run stays queued for the next
run rather than being appended to a run that will not call the model
again; the run is past its last drain from that decision on.

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
   transcript is not replaced. A model call the transform makes for its
   own purposes, such as a fold's summary, MUST be distinguishable from
   a turn's call by the model it calls, without reading the request:
   the binding carries the mark beside the request, not on it, so
   neither the provider nor the request hash sees it (#88). A host MAY
   ask a transform that folds to fold on the next request whatever its
   budget, with instructions for that fold alone; the fold is reported
   and recorded as any other. The loop asks the same, with no
   instructions, when it rebuilds a request the server refused as too
   long, as [Retry](#retry) says, by a mark on the transform's
   context (#277). The transform MAY read the settings the request
   will carry, the instructions and the tool definitions of the
   moment, as step 4 builds them before the hook; the loop MUST make
   them available to it, read-only. A transform that folds against a
   budget meant as a share of the model's context window SHOULD count
   those settings in the budget, and SHOULD weigh an image by the
   provider's visual cost, a fixed cost or one derived from the
   image's dimensions, rather than by its encoded size (#279).
   The loop MUST make available to the transform which items of the
   transcript are the person's own messages: the user messages it
   appended as a prompt, a steer or a follow-up under a trigger of
   kind `human`, other than one composed from instruction layers,
   copied or hidden, which a recorder writes without that kind (see
   The record). A host that rebuilds a transcript gives them back, as
   it gives the producers of its reasoning items (step 3), and one
   that seeds an agent from a record reads them there by agentsession
   RFC 0001's rule for `human`, a message a fold pinned included. A
   transform that folds SHOULD keep each of them verbatim, up to a
   size bound the host sets, and MUST record those it kept as pinned.
   A message over the bound, or one the host composed, is folded like
   any other item. The kept messages MUST also stay within a total,
   less than the request's budget, so that they never grow from fold
   to fold until they alone pass it, and SHOULD leave room under it
   for the summary and the kept tail: a total that lets them fill the
   request alone makes every call fold again, and the request's
   settings share the budget with them (the reference holds a host's
   total to half of what the settings leave of the budget). The newest
   are kept, and once
   the next would pass the total, it and every older one are folded
   into the summary, which had summarised them already. Only the transform's context carries them: a run a
   hook or a tool starts is prompted with copies, which are not the
   person's (#298).
3. Apply the **filter** to the result, which removes what the model
   must not see. Then leave out every reasoning item the loop knows a
   model under another model name produced: a reasoning item carries a
   signature only its own provider accepts. The model's own reasoning
   items stay, since a provider may require them back on a turn that
   called a tool, and so does one whose producer the loop does not
   know. The model a request names is the model its input is built
   for: a before-model-call hook or a retry's revision that names
   another model and keeps the input the loop built has that input
   rebuilt for the model it names before it is sent. The loop MUST
   credit the items an attempt produced to the model that attempt's
   request named, not to the configuration's, and a host gives it the
   producers of a transcript it rebuilt; the working transcript keeps
   every item (#286). The context a hook or a tool runs under MUST
   carry the run's attribution, so a run a tool starts over the
   parent's items, a child the loop-as-a-tool opens with the
   conversation so far included, leaves out the reasoning another
   model produced as a configuration switch does; its recorder
   describes that omission with an `omit` naming the entries, since
   items a run is prompted with name no response for the format's
   reasoning rule to read (#242).

   Then apply the **quarantine**. A host MAY quarantine an item for
   the rest of the conversation, and a hook, such as a before-model-call
   guard that finds an instruction injected into a tool's output or a
   fetched page, MAY name items to quarantine through its context. A
   quarantine covers everything a request is built from: from then on,
   no request carries the item, or, for a function call output, whose
   call needs an answer, a request carries it with one fixed placeholder
   text in place of its output, and a quarantined function call is left
   out with its outputs, each the output that answers it, after it
   with its call ID and before another call's, so a provider that
   reuses call IDs loses no other call's answer. The loop applies it here, to what the transform
   and the filter returned, and keeps the item in the transcript, which
   stays append-only and what arrived. Because the transform runs
   first, the loop MUST give it the quarantine, read-only, with a
   version that changes whenever the quarantine grows. A transform that
   sends any part of the conversation to a model of its own, as a
   fold's summary request does, MUST leave out of it what a request
   leaves out, by the same rule. A transform that remembers what it
   built over a part of the conversation, as a fold remembers its
   summary, MUST build it again when the quarantine has grown to reach
   that part, since what it remembers may carry the item; it keeps its
   own split of the conversation, so the quarantine is a predicate it
   reads rather than a list it is given shortened. The loop applies
   the quarantine again to whatever input the before-model-call hook
   returns, so a hook that puts a quarantined item back, by replacing
   the input with items from elsewhere, sends it as the quarantine
   says. A quarantine that hook makes applies to the request it was
   given, as the hook returns; when the transform had folded an item the hook
   quarantined into that request, the loop builds the request once
   more, transform and hook included, so the fold is made without it.
   The transcript a tool reads from its context is the conversation as
   a request carries it, under the quarantine. A run a tool starts over
   its parent's items carries the parent's quarantine as it stood when
   that run started, as it carries the attribution: a child that runs
   on, detached or spawned, does not see what the parent quarantines
   later. Each run of one conversation builds under that
   conversation's quarantine, a handoff's legs included, so what one
   leg's guard quarantined the next leg's requests leave out too; a
   host that drives the low-level loop carries the quarantine from run
   to run, and so does a front that starts an agent per task or per
   request: it keeps the conversation's quarantine from one to the
   next and applies it to the copy of each item it loads, which a
   store or a caller hands back decoded afresh, so it names an item by
   its content rather than its identity: every copy equal to a
   quarantined item, but for the id and status a relay or a store sets,
   is quarantined, so a quarantined user message "yes" leaves out every
   "yes" the loaded conversation holds, and a recorder reseeding a
   quarantine on a reopen matches the same way. A front whose requests name
   no conversation, as an Open Responses one's do not, keeps a
   quarantine past a request only under a key the host derives from
   who is asking, MUST NOT share one between callers, since a shared
   set drops one caller's item for another's quarantine and tells each
   what the other quarantined, and bounds what it keeps per caller,
   so one caller's keys never evict another's; a recorder
   reopened on a record whose `omit` holds a quarantine has the agent it records
   quarantine what its transcript holds of it, so an agent seeded from
   a store that keeps the conversation whole leaves out what the record
   says was left out. A front that builds a turn's request itself
   builds it under a quarantine as the loop does, applied after its
   before-model-call hook, and builds it once more when that hook
   quarantined an item the transform had folded into it. A recorder
   writes a quarantine as the `omit` setting that describes it, `omit.outputs` for a placeholder and `omit.items` for
   an item left out, as the consequences below say, so the requests
   after it rebuild and hash, beside a host's trim that clears other
   outputs under another text too, each text a group of format 0.16's
   `omit.outputs`; a fold made again is recorded as any
   fold. The record keeps the item as it arrived, and so does whatever
   is exported from the record, a trajectory or a trace: they are the
   record of the conversation, not the model's input (#281).
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
they are decided here. A change to a setting by the hook is a config
delta on the path. A change to the input, by the transform, the hook
or a host that replaced the transcript, is described when the session
format can describe it, and costs the hash only when it cannot. When
the input sent differs from the input the path rebuilds only by edits
agentsession RFC 0001's `omit` describes — item entries left out with
their order kept, function call outputs sent as placeholder texts, one
text to a group of `omit.outputs` since format 0.16, so a host's trim
and a quarantine are described together, the image parts of all but the last few sent as one placeholder — the
recorder MUST write a `config` delta whose `omit` describes them before
the response, a replace carrying the setting whole when an item left
out is sent again, and hash the request against the path as edited; a
host MAY name the entries it trimmed instead of leaving the recorder to
find them. The recorder MUST decline the hash only for an input the
format cannot describe, an item added or rewritten, since the path
holds the transcript's items and the request carried others, rather
than write a hash the path cannot verify (#92, #233). A transform that
folds the transcript and reports the fold to the recorder, with what
it kept, is described as a compaction rather than as an `omit`.

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
  needs. The guard is given the items of the response that precede the
  message. Whether the message is the answer is not known when it
  runs: a model may speak before it calls a function in the same
  response, and the guard is not held until the response completes; a
  guard that needs finality reads it from the stop hook. A guard error
  fails the run, unless it is marked as a guard's, which is a guard
  stop: the message is withheld, neither appended nor given an
  `item_end`; the loop reads the rest of the response for its usage
  and nothing else, delivers a `response_end` marked withheld and no
  `turn_end`, and the run stops with cause `guard`, the error on the
  end event and the end marked withheld, which is no answer, whatever
  message the response spoke before the withheld one. The withheld
  `response_end` carries the loop's account of the response, not the
  server's: `incomplete` with `content_filter`, no error, the usage
  of the whole response when its terminal event arrived, and as its
  output the items the transcript took from it. A function call the
  same response added to the transcript before the message, which an
  item opening with `output_item.added` commits the attempt to, is
  never dispatched, and the loop answers it with an output of fixed
  text, never the guard's error, before `run_end`, so the transcript
  holds no call without an output and the next run may start. On a
  stream that sends `output_item.done` alone, nothing committed the
  attempt, so the calls before the message are still held, and are
  dropped with it. The replacement reaches the transcript and
  `item_end` only: the
  response carried by `response_end`, `turn_end` and the stop hook is
  the wire response as the model produced it, with the original
  message, so a front that must not show withheld text renders from
  the item events and never from the response. A front that relays the
  stream, whose deltas it cannot take back, carries the replacement on
  the item's done events and in the response it builds, and a front
  that calls the model itself runs the guard as the loop does. A
  message still open when another item starts or ends, or the run
  ends, is one the guard withheld, since the loop ends every message
  it keeps before anything else; a relaying front closes it emptied.
  A front that hands the caller the response's function calls to run
  holds them until the response completes, so a withheld response
  hands the caller none.
- A function call is known, while it streams, by its item ID, and by
  its output index when the event naming it carries none, so a stream
  that opens a second call at the output index of the first still
  gives each its own ID, and a call opened without an item ID and
  completed with one keeps the ID it opened with. The response the
  turn acts on, which `response_end` and `turn_end` carry and whose
  function calls form the batch, holds the calls the transcript holds:
  each function call of the completed response is paired with one the
  attempt completed, taken in the order they opened, which is the
  transcript's order, by item ID when both carry one, else by the call
  ID the model gave it among the completed calls not yet paired, else
  by order, and the completed call, with the ID the loop decided and
  the arguments it completed with, stands in the response's place.
  Every function call the batch runs MUST be one the transcript holds,
  under the ID the transcript holds it by. A function call the
  completed response lists that the stream opened and never completed
  is completed from the response: it takes the ID decided when it
  opened, paired by item ID or else by open order among the calls the
  attempt opened, never by its position in the response, which after a
  reused output index names another call, and it is appended with its
  `item_end` before `response_end`. The completed calls are paired
  first: a listed call whose item ID, or whose call ID and arguments,
  match a completed call is that call. A listed call left over pairs by
  its call ID with the calls of that ID in the order they opened,
  completed or not, and then by order, the completed calls first, so
  neither a completed call nor one never completed is taken for
  another that only shares its call ID. The response the turn acts on lists the calls in the
  transcript's order, the ones only the response completed after the
  ones the stream completed. A listed call the
  stream never opened is decided and appended the same way at that
  point, so every call the response lists is paired or decided and
  none runs that the transcript does not hold. A call the stream opened
  that the response does not list is not in the transcript and is not
  run; its `item_start` has no `item_end`, as for an item an attempt
  dropped. A response that arrived failed completes nothing, since the
  run ends with its error. An item that is not a function call is left
  as the response gave it (#236). A completed call no
  call of the response pairs with, which a terminal response built one
  item per output index leaves out when a later call took the index,
  makes the response's calls give way, as a group, to the completed
  calls in the order they opened, so every call the transcript holds is
  run and answered in the transcript's order. A
  stream that opens every call at one output index with no item IDs,
  as a text-call parser's does, therefore runs each call under its own
  ID and answers it with its own output, where pairing by index alone
  ran the last call opened there for every call listed at that index
  and left a call the response did not list dangling (#210).
- `response_end` carries the folded response, usage included, as soon
  as the stream ends and before any tool of the turn runs. A response
  that arrived with a failed status is delivered here too, before the
  run ends with the error, so a recorder can write it, and so is one
  the output guard withheld a message of, marked withheld, before the
  outputs that close its calls. A response the server itself ended
  `incomplete` with `content_filter` is not marked withheld.

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
recorder may already hold part of it. A model call that fails with an
error marked as a guard's, as a moderation layer around the model marks
its refusal, is a guard stop whether or not the attempt committed: it
MUST NOT be retried, and it is the output guard's stop in every other
respect. When the attempt had committed, its `response_end` is
delivered marked withheld, with what the transcript took from it,
before the calls the stream completed are answered with the fixed
text, so a recorder writes it incomplete with `content_filter` and the
outputs as the policy's rejects after it, and the end is marked
withheld. When it had not, nothing of the answer is in the transcript:
the loop MUST NOT deliver an empty withheld `response_end`, and
delivers `model_blocked` in its place, with the request sent and the
usage the provider reported, if any, so a recorder writes the refused
call as the `model_blocked` record a before-model-call guard's refusal
is, with the request's hash, and counts a call that may have been
billed. Either way the run stops with
cause `guard` and the error on the end event (#275). An item the model completes
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

An attempt the server refused as longer than the model's context
window, the `invalid_request` error with code `context_length_exceeded`
that openresponses' adapters report for every provider, MUST NOT be
sent again unchanged. When the configuration has a transform, the loop
SHOULD rebuild the request through it with a fold asked for, as the
request's step 2 says, and send it once, as a retry: `model_retry`
reports the refusal and carries the rebuilt request, so a record shows
the fold and then the attempt. It does so whatever the retry policy,
whose attempts it does not count: the attempt number the policy's
limit, backoff and revision are given counts the policy's own retries
alone, while `model_retry` numbers every call. A request a revision
made keeps the revision's model and settings when it is rebuilt, and
the transform weighs those, so an overflow on a smaller fallback is
folded for the fallback. It happens never twice in one turn. A
rebuilt request whose input is the one refused, because the transform
could not fold, is not sent, and the run ends with the refusal (#277).

Retry is off unless the policy names more than one attempt. The
default predicate retries the transient statuses (408, 409, 429 and
5xx), a stream that ended before its terminal event and transport
failures, and calls everything else, a 4xx in particular, final. A
loop MUST decide whether to retry by the error's code before its HTTP
status: an error whose code says the account cannot pay for the
request, Open Responses' registered `insufficient_quota`, MUST NOT be
retried, whatever its status, since OpenAI sends it as a 429 and no
retry can fix it (#308). The
default backoff honours a Retry-After header, uncapped, and otherwise
doubles from 500ms to a 30s cap. Cancellation cuts a delay short and
ends the run with reason `aborted`.

The revision MAY move the next attempt to another model or another
setting, which is how a fallback chain lives in the loop rather than
under it: the revised request is on the `model_retry` event, a recorder
takes its settings, and the path names the model that answered rather
than the one that did not, beside a record of each attempt that
failed. When the revision names another model and keeps the input the
loop built, the loop MUST rebuild that input for the revised model
before sending it, as [the request](#the-request) step 3 says, so the
revised model is sent no reasoning another model produced and the
record's rule for other models' reasoning rebuilds what was sent; what
the attempt produces is credited to the revised model. A revision that
replaces the input is sent as it is. A fallback written as a model under the loop still works and
still says nothing.

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
a barrier call running alone in its place, cancellation waited for. A
`tool_dispatch` is raised as each call is handed to its tool, after it
has taken a slot in the bound and its turn in a serial batch, a
resource chain or around a barrier, and before the tool runs; a call
the cut reaches first never reached a tool and raises none. Its turn
comes once every call it runs after has been settled, the after-call
hook included, not merely completed, so what that hook changes or
records lands before the next call is dispatched: in a chain the call
before it, and around a barrier every call of the segment before (a
barrier waits for every call since the previous barrier, and the calls
after a barrier for the barrier); a call of another chain in the same
segment is not held for it. It is
raised on the call's own goroutine, serialised with the run's events,
and the executor waits on it, so a recorder has the dispatch durable
before the side effect. A consumer that fails on it stops the call
before its tool runs and fails the run as any delivery failure does,
with the call pending as `undispatched` and no side effect behind it.
Each call is handed over with an **idempotency key**, agenttool RFC
0001's, which `tool_dispatch` carries: the loop mints one for every
call, unique beyond the session, and keeps it when the call runs again
on resume, taken from the answer that approved it or from the pending
call. A
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
ID, the turn's number, 0 for the batch a resume approved, which runs
before the first turn, a snapshot of the working transcript as it
stood when the batch started with the batch's calls included, and the
invoker for
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
| `max_turns` | the turn limit was reached with tools called or items queued; the queued items wait for the next run |
| `hook` | the stop hook ended the run |
| `guard` | the stop hook, the before-turn hook, the before-model-call hook or the output guard ended the run with an error marked as a guard's, or the model call failed with one; the error is on the end event. A model call's guard error after its response began is recorded as the output guard's stop is. Stopped before the model call, the turn has no `turn_start`; stopped by the output guard, its `response_end` is marked withheld, it has no `turn_end`, the calls the withheld response made are answered with an output of fixed text, and the end is marked withheld, which is no answer |
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
in transcript order, each with a reason and, for a call the run made,
the tool it resolved to, so a prompt can show what it asks about:

| pending reason | meaning |
| --- | --- |
| `deferred` | the decision hook handed the call to the caller in this run and nothing has answered it. The tool did not run. An agent seeded from a record that holds the call after its dispatch reports it `deferred` and **dispatched**: it waits on the caller, it may have run, and it is **ambiguous**, with the key and the arguments of that dispatch |
| `aborted` | the call was handed to its tool, by this run or an earlier one, and the run was cancelled or failed before its output was appended. The tool may have run to completion, so its side effect may have happened: the call is **ambiguous**, and carries the idempotency key and the arguments of its last hand-off, the dispatch a run of it again repeats |
| `undispatched` | the loop did not hand the call to its tool, so the tool did not run: a subscriber refused its `tool_dispatch`, or the cancellation or the failure reached it before its turn. A recorder that took the event before a subscriber that refused has already written the dispatch. Nothing has decided to run it, so an approval of it goes to the decision hook |
| `unknown` | the call was in the transcript the run was given, so the loop cannot say whether it ran, and it is **ambiguous** as well. A session recorded with dispatch entries can say, and an agent seeded with what it says reports the reasons above instead |
| `answered` | a record the agent was seeded with ends the call with an answer, the format's verdict for a call ended without running again, and stopped before the answer's output: the call is owed that output and nothing else. A live run never leaves one |
| `rejected` | a record the agent was seeded with ends the call with a reject before it reached its tool, and stopped before the refusal's output: the tool did not run, and the call is owed that refusal and nothing else. A live run never leaves one |
| `returned` | a record the agent was seeded with holds what the call's tool returned, a `result`, and stopped before the output joined the transcript, as when the process died while the rest of its batch ran: the tool ran once and its outcome is known, so the call is owed that output, or another the caller writes in its place, and nothing else. It is not ambiguous and is never run again. A live run never leaves one |

A call pending when the run started that the run did not hand to its
tool keeps the reason, the key, the arguments and, for one a reject
ended, the refusal's reason it had, except that a
call the resume approved and the cut reached first is `undispatched`,
or `aborted` when it was dispatched before it was held.

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
- an **approval**, optionally with replacement arguments, a reason and
  an idempotency key: the loop runs the call itself, with the key
  given, else the one the pending call carries, that of the dispatch
  it repeats, else a new one, and with the arguments given, else the
  ones that dispatch ran with, else its own. A call pending as
  `answered`, `rejected` or `returned` takes no approval.

An answer MAY carry a **note**, what the person said when answering,
**who decided** it in the session format's terms (`human`, `policy`,
`agent`), **why**, which for an output answering an ambiguous call
says why it was not run again, and, for an output taken from a record
rather than produced now, **where it was taken from**, in the terms of
the library that read it, so a recorder can tie the output it writes
to the one it repeats, and, for an output, whether it reports that the
call **failed**: an output for a call the caller gave up, such as one
saying a cut call's outcome is unknown, does, and a refusal does not.
A resume's answer has no default decider: a policy engine answers
pending calls as often as a person does, so an answer to a pending
call that names nobody is recorded as an anonymous decision rather
than guessed at. An answer to a question the loop puts to someone
during a run, a nested call's deferral (Nested calls) or a child's put
to the elicitor of the call that started it (Composition), is
different: it is `human` when it names none, since the question was
put to the user. A front that
takes answers from a remote party, to pending calls or to the questions
an elicitor asks, MUST record the decider that party names only when
the host has authorised that principal to name one; otherwise it MUST
record the front's stated default, `human` for a control front, where
a person at a client is who answers, and none, an anonymous decision,
for a peer front, which cannot tell a person on the caller's side from
a program. An unchecked claim is never recorded (#267, #291). A2A has
no field for who answered, since identity stays at the protocol layer,
so the A2A front carries the decider as a member of its own approval
data part. An answer MAY
ask the run to **terminate** once every answer is in, for a refusal
that should end the turn so the user can say what to do instead; the
outputs are still appended, so the transcript stays valid.

A resume MUST answer every pending call exactly once and MUST NOT name
a call that is not pending. An approval of an ambiguous call, pending
as `aborted` or `unknown` or held after its dispatch, is held to
agenttool RFC 0001's rule for running a call again, since the loop does
not have its result: the loop asks the tool's replay for the arguments
the call would run with, under the call it would run as, and approves
it when the answer is *safe*, or *keyed* and either run with the
arguments and under the key of the dispatch it repeats, which must be
known, or with new arguments under a key the answer chose, since a key
names one operation: another key with the same arguments would run the
operation that may have happened as a new one; otherwise it refuses the resume and
runs nothing, and the caller answers the call with an output saying
the outcome is unknown. An approval of a call pending as `answered`,
`rejected` or `returned` MUST be refused.
The one way past the rule is an approval that says the host accepts
running the call again, recorded as the proceed's reason. An approval
that passes the rule and gives no reason carries the rule's, `run
again: safe` or `run again: keyed`. The resume
then runs as follows, and its source is `resume`:

1. `run_start`.
2. The outputs of the answers that carry one are appended with their
   item events, then the notes of those answers, each in its
   answerer's voice: a user message for an answer by `human` or by
   nobody, a developer message for one by `policy` or `agent`.
3. The approved calls run as **one batch before the first turn**, with
   the decision hook skipped because the decision has been made, except
   for a call pending as `undispatched`: nothing decided it, so the hook
   rules on it with the approved batch as its batch, and a block or a
   deferral applies as it would in a run, a deferred call ending the run
   with `input_required` once the batch is in. Every
   `tool_start` is raised first, in the order the answers name the
   calls, each carrying a decision holding the caller's arguments, note,
   decider and reason and nothing else, or the hook's decision when it
   gave one; the tool events carry turn 0; the
   execution mode and bound apply; the after-call hook runs; the
   outputs are appended in the order the answers name the calls, not
   in transcript order, and their notes after them, in their
   answerers' voices as in step 2.
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
Such a prompt MUST be able to carry, per output, everything a resume's
answer can: who decided, why, where the output was taken from, and
whether it reports a failure; a recorder MUST record them as it
records a resume's, and an output that says none of them is recorded
as an answer that names nobody and does not report a failure, never
one inferred failed from its text (#305).

An agent given a transcript to start from — a stored session being
resumed — derives its pending calls from that transcript: every
function call without an output is pending with reason `unknown`, and
the agent refuses to run until they are answered. A host SHOULD seed it
as well with what a record says of them, the reason, the key and the
arguments, and the nested calls its tool made, so only a call that may have run is held to the replay rule
and one that never started is approved without it, and the output of
one that dispatched and completed off the path the agent continues, on
a branch a rebase left or in the session this one forks, or whose tool
returned before its output joined the path, a `result`, which is
pending as `returned`, with where it ran, so the host answers the call
with that output rather than running the call again or telling the
model its outcome is unknown. A held call is seeded with the decision
its hold records, its held mark and its subject among it, so a front
over a restarted session asks about the call that was asked about,
and about the part of it the deferral named, and not about the calls
held beside it (The record, #299). The loop
carries what it was seeded with onto the pending list of a run that
leaves the call pending, and sets none of it itself. Replacing the
transcript re-derives them the same way, except that a call the agent
already had pending keeps what it knew of it, so a held call stays
held; the host seeds the rest again from the record. Whatever the old
transcript was waiting on and the new one does not hold is
forgotten.

A host resuming a stored session gets the replay rule's answers from
the record through the recorder's library: a held call is the
caller's, one held after its dispatch included; one that never
started is approved, and the resume puts that approval to the
decision hook; one a reject refused before its output was written is
answered with the reject's reason as that output; one in flight, a `dispatch` and no output nor a
later hold or answer, or one the file cannot say about, is ambiguous,
and is approved when its tool says *safe*, or *keyed* and its last
dispatch carries a key, which the approval carries with the arguments
that dispatch ran with, and answered with the outcome unknown
otherwise, with a reason saying which; one answered before its output
was written gets the outcome unknown as that output. One whose tool
returned before its output joined the path, a `result` after its last
`dispatch` and no output, is not in flight but `returned`: it MUST be answered with
that result's output, failed when the result is, with where it was
taken from, and MUST NOT be run again or answered with the outcome
unknown.

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
  left pending, with reason `aborted`, or `undispatched` for a call
  the cancellation reached before its hand-off. In a sequential batch
  the cut calls end in the batch's order, so a call the cancellation
  reached before its hand-off ends after the call it waited behind. In
  a batch with a barrier, the calls of the segment that was running
  when the cancellation came end first, in no fixed order among
  themselves, then the calls of each segment not yet started, in the
  batch's order.
  The executor is waited for, so
  every result it holds is received before the run ends.
- The transcript holds only completed items. An output item the stream
  had opened and not completed is dropped, as under a retry.
- `run_end` follows every `tool_end` the cancellation produced and is
  the last event. A consumer that delivers `run_end` as soon as it sees
  the cancellation, before the cut-off calls have ended, is not
  conforming.
- The queues are untouched: anything steered or queued and not yet
  appended goes to the next run.

A tool call's own context ends with its batch. A tool that starts work
meant to outlive the call, a task that returns at once and reports
later, derives it from the **run context** the loop puts beside the
call's: the values of the context the run was started with and the run
ID, cancelled when the run is cancelled, with the cause, and not when
the batch or the run ends by itself. Once the run has ended, cancelling
the agent reaches its next run and not that work, which keeps a cancel
of its own if it must be stopped later. The tool recorder is on the run
context too, so a record such work writes of what it started reaches
the host. The work's own call is not: the run context of a run
started from a tool call, a child's, carries the call that started it
and a root run's carries none, so the work carries its own call for
the record to name it. What belongs to the batch, the transcript, the invoker,
the tool elicitor and the steer signal, is not on it.

A steer does not cancel a running tool. The loop puts beside each call
a **steer signal** that is raised when an item is steered into the run,
during the batch or before it and not yet drained; a tool that only
waits listens for it and returns early with what it has, and the
steered item joins the run after the batch as ever. The signal is fresh
for each batch that follows a drain.

Every event a cancellation leaves behind — the `tool_end` of each
cut-off call, the appended outputs, the `run_end` — MUST be delivered
to the consumer, and an agent MUST deliver it with a context whose
cancellation is lifted, so a subscriber that writes durable state has a
context it can use. A subscriber that fails for a reason of its own
while the run is being cancelled ends it with reason `aborted` and its
error joined to the cancellation on the end event, so the failure rides
on the abort rather than vanishing, and `run_end` MUST carry that
failure as a member of its own, `failure`, as well; `failure` is empty
otherwise. A consumer that matches the reason stops reading there, and
the failure that matters, a recorder that could not write, would go
unseen: a front renders the reason, and a host that keeps a record
checks `failure`, since a non-empty one means the record of the run may
be short (#83).

## Events

Every event carries the run ID, and every event of a turn carries the
turn number. The catalogue, with the members beyond those two:

| event | members | when |
| --- | --- | --- |
| `run_start` | `source`, `trigger` | first event of a run |
| `turn_start` | `request`, `inputs`, `quarantined` | after the request is built and before it is sent |
| `model_retry` | `attempt`, `error`, `delay`, `request`, `quarantined` | after an uncommitted attempt failed and before the next is sent |
| `model_blocked` | `request`, `error`, `usage`, `quarantined` | in place of `turn_start` when the before-model-call hook refused; in place of `response_end` when the model refused with a guard's error before the attempt committed, with the usage the provider reported, if any; the last event before `run_end` |
| `item_start` | `item`, `response_id`, `hidden`, `copied`, `provenance` | an item entering the transcript: an input as it is appended, an output item as the stream opens it |
| `item_update` | `item`, `stream`, `response_id` | one wire event of an output item, with the item as accumulated |
| `item_end` | `item`, `response_id`, `hidden`, `copied`, `trigger`, `model_call_id`, `provenance` | the item is complete and in the transcript; `copied` when the caller marked the item as taken from another conversation; `provenance` when the caller gave the item one; `model_call_id` is the ID the model gave a call the loop renamed; `trigger` is the run's for an item the run was prompted with, other than an output a resume appends for a pending call, whose decision names who gave it |
| `response_end` | `response`, `withheld` | the stream ended; before any tool of the turn runs; `withheld` when the output guard withheld a message of it |
| `tool_start` | `call_id`, `name`, `args`, `decision`, `parent`, `invoked`, `replayed` | after preflight, in the model's order; `invoked` is the arguments a nested call was invoked with, before a decision rewrote them; `replayed` when a nested call is given its recorded result |
| `tool_dispatch` | `call_id`, `name`, `parent`, `idempotency_key` | the call has been handed to its tool, before the tool runs; after its `tool_start` and before its `tool_end` |
| `tool_update` | `call_id`, `name`, `partial` | a progress update from a running tool |
| `tool_end` | `call_id`, `name`, `result`, `error`, `blocked`, `deferred`, `reason`, `parent`, `withdrawn`, `cut`, `replayed` | the call settled, in completion order; `reason` is the decision's for a blocked or deferred call; `withdrawn` when a consumer refused its `tool_dispatch`, so its tool never ran; `cut` when a cancellation or a failure cut it off before it had a result of its own, so its output is not appended and it stays pending; `replayed` when the result is the one a record held for a nested call that ended |
| `turn_end` | `response`, `tool_results` | after the batch's outputs are appended |
| `quarantine_added` | `items`, `quarantined` | the host quarantined while no run of the agent was active, or after its run's end was delivered; belongs to no run, and is delivered at once, or after the event being delivered |
| `run_end` | `items`, `reason`, `cause`, `error`, `failure`, `pending`, `withheld`, `quarantined` | last event of a run; `withheld` when the output guard's stop ended it; `failure` when a consumer or a hook failed for a reason of its own while the run was being cancelled |
| `queued` | `item`, `mode`, `input`, `hidden`, `copied`, `trigger`, `provenance` | an item accepted into a queue; belongs to no run; while a run is in flight, without waiting for its next phase (#302); `input` says how it arrived, `steer` or `follow_up` for an item queued and `deliver` for one delivered; `copied` when the caller marked the item as taken from another conversation; `trigger` is the item's provenance's source when it names one |

An event's members are the facts as of the event. A loop MUST deliver
each event so that a consumer that keeps it, as a view's render queue
or a recorder that writes later does, finds the same values in it
afterwards, whatever the loop does next: the item an `item_start` or
`item_update` carries is the item as accumulated at that event, and
later wire events do not change it (#218).

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
  response_end | model_blocked          withheld: then the outputs closing its
                                        calls, (item_start item_end)*, and run_end;
                                        model_blocked, a model guard's refusal
                                        before the attempt committed, ends the run
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
  and the rest are pending as `aborted`, or as `undispatched` when
  they were not yet handed to their tools. A `tool_end` counts as raised
  when the loop delivers it, whether or not every consumer took it, so
  a consumer that fails on one never earns the call a second.
- `item_end` for an output item follows the wire event that completed
  it; a partial item is never announced as complete. The item is in the
  transcript when `item_end` is delivered.
- Outputs are appended in the model's order; `tool_end` arrives in
  completion order. A consumer correlates the two by call ID, never by
  position.
- A call ID names one function call, and is never empty. The loop
  decides a call's ID when the call opens, at `output_item.added`, or
  at its `output_item.done` on a stream that sends no
  `output_item.added` for it. A call the model gives no ID, or one a
  call in the transcript already has, or one another call of the
  attempt took, held or not, or one the host reserved on the agent or
  on the run's context, takes an ID of the loop's own, the model's
  with every character outside letters, digits, `_` and `-` replaced
  by `_`, cut so the whole is at most 64 characters, and a random
  suffix: its `item_start`, every `item_update`, its `item_end`, the
  transcript, its output, the `response_end` the turn acts on and
  every later request carry the new one, so a consumer sees one ID for
  the call from the moment it opens and the provider is sent the ID
  the record holds. The `item_end` keeps the model's ID beside the
  item. A call is known by its item ID, and by its output index when
  the event naming it carries none, so a stream that opens a second
  call at the output index of the first still gives each its own ID,
  and a call opened without an item ID and completed with one keeps
  the ID it opened with; the response the turn acts on holds the
  calls the transcript holds, paired with the response's as the
  [model call section](#the-model-call) says, one only the response
  completed included, so the batch never runs a call the transcript
  does not hold or a call under another's ID. The
  IDs a transcript held stay
  reserved when `SetTranscript` replaces it. A host that seeds the
  loop with less than a whole session, a context after a fold or a
  trim, a branch after a rewind or a fork, or a child session reopened
  under its call, reserves the IDs of every call in the session, on
  every branch, since the record names a call by its ID alone and
  derives a subsession's ID from it.
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
refused. An agent MAY be **held idle** for a change of several steps
that no run may interleave with, a rewind among them: the hold is
refused while a run is active, every run start is refused until it
ends, and its check is one step with a run's start, so no run begins
between the check and the change (#220). The agent's **state** is a snapshot: the transcript, whether a
run is active and which turn it is on, the queues' contents, and the
pending calls with their reasons.

## Queues

An agent accepts items outside a run's own input and appends them at a
stated point:

- **steer**: the item is appended after the current batch, before the
  next model call. When the agent is idle it is consumed by the next
  run before that run's first model call: after its prompt, or on a
  resume after the approved batch, so a steered item never precedes
  the prompt that starts a run and never waits a model call for it. A
  resume that refuses without calling the model leaves it queued.
- **follow-up**: the item is appended when the run would otherwise
  end, so the agent keeps going instead of going idle. It is drained
  only after a turn in which the model called no tools, after the
  steered items.

An item MAY be queued with a trigger of its own, what brought it in,
which rides on its `queued` event: the run's trigger names what started
the run, and an input that joins it has a provenance of its own.

Accepting an item MUST NOT block on the delivery barrier, so steering
from inside a subscriber is safe. Each accepted item is reported as a
`queued` event, with which queue it went into and the run that was in
flight, empty when the agent was idle. The report is not the accept:
the item is queued when the call returns, and the report is delivered
under the barrier, before anything that item produces. A run in
flight SHOULD report an accepted item without waiting for its next
phase (#302): a quiet tool raises no event, and a host showing what it
accepted would otherwise show a steer typed during the tool only once
the tool ends. The reference reports it at once when no event is being
delivered, on its own thread of control so the accept still returns
without running a subscriber, and otherwise right after the event that
is. An item a subscriber accepts while such a report is being
delivered waits for the run's next event, so a subscriber that queues
on every report does not spin; an item accepted from elsewhere while
it is delivered, a second person's steer, is reported right after it,
since only the subscriber's own accepts can chase the reports. The
reference tells a subscriber's accept by the context it hands the
subscriber, and counts a steer or follow-up queued with no context as
the subscriber's; a subscriber that queues later with a context it
kept is still counted as one, and its item waits for the run's next
event. Reports made right after an event MUST stop after a bounded
number of passes, whoever's accepts are owed, so that a subscriber
whose accepts read as from elsewhere, queued on every report under a
context of its own, cannot stall the run; what is left is reported at
the run's next event. An idle agent reports it at the start
of the next run. A host that must not lose an input therefore writes
it before queueing it rather than from the event, and a subscriber's
error cannot refuse the item; an error on a report delivered between
the run's events ends the run at its next event, as one on a report
delivered ahead of that event does, unless that event is the run's
end, when nothing is left to end and the error is dropped.

A run that stops after a turn, on its turn limit, a stop hook, a
terminating result or a call that needs input, decides so before it
drains the queues, so an item steered during that turn stays queued
for the next run rather than being appended to a run that will not
call the model again. A run drains the queues for the last time when
it would otherwise end, after its final `turn_end`, or not at all once
it has decided to stop, and is then past its last drain: its `run_end`
is still being delivered and the agent still reads as running, but an
item steered then waits for the next run, and its `queued` event names
no run. The mark is set in the same step as the drain that finds both
queues empty, or as the decision to stop is made; a run that ends
another way, a guard before or during the model call, a cancellation
or a failure, is marked as it leaves the loop, and drains nothing between.

An agent offers a **delivery** for an input that arrives on its own
time, a background task's result: the item is queued as a steer, and
the delivery waits until a model call has seen it. A delivery MUST
accept its items, in the order given, before it waits, and a caller
MUST be able to take the acceptance without waiting for the outcome,
which the agent still settles: it joins the run in flight or starts
one once that run ends, as below (#302). The waiting is the point, since the
outcome is what closes the race below (#288); the split lets a view
that forwards a person's steer answer once the agent has it, so a
second steer typed while a quiet tool runs is not held behind the
first's outcome. A delivery so accepted is settled by the agent,
whoever waits: a run it starts is the agent's, and runs when the
caller has gone away, until an abort. A delivery MUST NOT start a run
once the run in flight when it was accepted was aborted before taking
it: the abort stopped the agent, a person's interrupt among them, and
a run the delivery started would undo it. The item stays queued for
the host's next run, and the delivery reports that it was aborted,
with the aborted run's end.
An abort the agent receives after a delivery is accepted and before
its follow-on run starts MUST settle that delivery as aborted and
start no run; the accepted items stay queued (#312). This holds when
the abort finds no run to stop, the run in flight being past its last
drain or having ended, and the delivery about to start the run that
takes the item: a person's interrupt pressed as a run ends, or a
host's abort as it shuts down, would otherwise be lost to a run it
never saw. The agent records the abort's arrival against the
deliveries it has accepted and not settled, and a delivery checks it
in the same step as it starts a run, so an abort either stops the
delivery or reaches the run it started. A delivery so settled reports
that it was aborted, with the abort's cause and no run's end, since
no run ended on it. Either way the report tells the abort from the
end of the caller's own wait, across a wire too. A run that took the
item and was then aborted did not refuse it: the delivery reports
that run's end and no refusal.
A delivery taken in one step, whose caller waits for the outcome, is
settled by that wait instead: a caller that stops waiting starts
nothing and leaves the item queued, for the run in flight to take or
the host's next run. A delivery refused before its
items are queued, while the agent is held idle, accepts nothing. A run in flight
that drains it and then sends a request has taken it. A run that stops
after its turn or ends without draining it leaves it queued, and,
unless it was aborted or an abort has arrived since the accept,
the delivery starts a run that takes it once
that run has ended, as a
continue after a steer would, unless calls are pending, when it is
left for the resume and the delivery says input is required. A
delivery MUST NOT start a run once the last run ended on a terminating
result and no run has started since: what the result says is the
host's to read, a handoff's transfer above all, and a run the delivery
started would answer under the sender's configuration and race the
host's switch to the receiver. It leaves the item queued for the
host's next run, the receiver's, which takes it before its first model
call, and says so (#274, #290). A host that reads the result and
follows no handoff, or stops at its bound, MUST then start a run that
takes a delivery so left, under the configuration the agent holds, a
transfer that run ends on past the bound not followed, so the delivery
is not left for an input that may never come. A run
that drained it and ended before the next request, on a guard, a
cancellation or a failure, leaves it in the transcript unanswered,
and the delivery reports that run's end rather than start another. The
asymmetry is deliberate: a stop after the turn was decided without the
item, and one before the call with it. A delivery made from inside
the run, by its tool, hook or subscriber or by a child run a tool
made, does not wait, since the run waits on it: the item joins the run
as any steer does, or waits for the next run when the run is past its
last drain.

A front that lets a remote party queue input into an agent SHOULD also
offer the delivery, with these semantics: the item joins the run in
flight when that run will still take it, and otherwise starts a run
once the run in flight has ended, unless calls are pending or that run
ended on a terminating result. The front
reports whether the item joined and the end of any run it started. It
SHOULD carry both steps of a delivery, the accept as soon as the
agent has the items and the outcome once the agent has settled it, so
a remote view takes the accept as a local one does (#302). A
remote party cannot observe atomically whether a run will take a
steer, since a run past its last drain still reads as running, so a
queue and a read of the state are a race that only the delivery closes
(#288). A front serving a recorded agent SHOULD take queued items and
deliveries through the recorder's own, which writes them first, so a
remote party's input survives a quit as a local one does (#244); that
includes a steer a peer front delivers into a working task and one it
keeps for the message answering the task's calls, which the record
then holds once, under the entry it was first written with.

The queues live in memory. An item accepted is in no record until a
run appends it or a subscriber writes it, and it survives a
cancellation, a configuration change and a transcript change but not
the process. A session recorder writes each `queued` event as a
`queued` entry, which makes the inbox durable from the event to the
append; a host that must not lose an input it accepted while the agent
was idle, whose event waits for the next run, writes it itself. A
`queued` event says how the input arrived, delivered or queued, beside
the queue it joined, and the recorder writes that on the `queued`
entry. A recorder hands back the inputs it owes after a restart or a
rewind the way they arrived, so the turn that takes a delivery reads
it as one, and says whether a delivery is among them: a delivery owed
with nothing else to start a run is the host's to start one for, as
the delivery would have, with a continue. A recorder
offers a delivery that writes the `queued` entry before the agent
accepts the input, as it does for a queue, so a delivery that arrives
while the agent waits on an approval survives a quit (#244); its
delivery in two steps writes the entry before the accept too, so the
accept a caller takes at once is on the record (#302). An input
the agent refuses, held idle, MUST leave nothing owed: the recorder
writes no entry for it when it can tell the agent will refuse, and
otherwise neither hands it back nor writes it again after the next
run end.

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
| before turn | turn phase 2 | the working transcript, and the turn's inputs so far with how each arrived, as phase 5 defines them, the hook's own items excepted | items to append as facts | fails the run unless marked as a guard's, which stops it with cause `guard` | items appended in order; the first error drops them all |
| before model call | turn phase 4, last | the finished request | edits it in place | `model_blocked`, then the run fails; marked as a guard's, the run stops with cause `guard` | in order, each seeing what the last left; the first error stops |
| output guard | as the stream completes an assistant message | the message | a replacement or none | fails the run unless marked as a guard's, which withholds the message and stops it with cause `guard` | in order, each seeing the last's replacement; the last stands; the first error stops |
| decision (before tool call) | preflight, per call, model order; a nested call as a batch of one | the call, the tool, the arguments, the batch and index, and for a nested call its parent | a decision or none | fails the run; returned to the tool for a nested call | the strictest action wins, block over defer over allow; a block ends the chain, a defer does not; rewritten arguments pass to the hooks after; the first reason and decider of the standing action, the first note; the first decline text among the deferrals of the kind that stands, asking or held, so an asked call never takes a hold's (#304); terminate if any set it |
| after tool call | as each call settles, blocked calls excepted | the call, the result, the error | an override or none | fails the run; returned to the tool for a nested call | — |
| should stop after turn | turn phase 10 | the response, the results, whether the turn was final, the transcript | stop or not; a guard error stops with cause `guard` | fails the run unless marked as a guard's | in order until one stops; the first error stops |
| transform | request step 2 | a copy of the transcript | the input for this call | fails the run | — |
| filter | request step 3 | the transformed list | what the model sees | — | — |

The before-turn hook is where a layer acts on what arrived for the
turn: a scope that ends when the user speaks again revokes on an input
of mode `prompt`, `steer` or `continued` whose item is a user message,
and not on one of mode `deliver`, which a background task's answer
arrives by; a handoff receiver started on the sender's transcript acts
once on the turn whose inputs hold the transfer's output as
`continued`. The list is not the transcript's tail: a delivered output
after a steered message reads as what it is, and so does an answer
delivered as a message.

The stop hook, the before-turn hook, the before-model-call hook and
the output guard tell a policy stop from a failure by marking the error
as a guard's, which the reference binds as a sentinel the error wraps.
The two that run before the model call are where a cost limit or a
deadline stops a run before it spends another call; the output guard
is where a content policy stops a run whose answer it will not let
through.
Whether the decision should be returned as data instead is open (#82).
A layer that must see every turn, a meter for one, belongs in a
subscriber rather than in the stop hook, which stops at the first hook
that stops.

The hooks run with the run's context, which carries the run ID, the
number of the turn in progress for a hook a turn calls, and, for the
decision and after-call hooks, the batch's transcript snapshot and the
invoker. The context of everything a run calls, the transform, the
hooks, the model and the tools, MUST carry the run ID, and within a
turn the turn's number, the one its events carry (#275). A hook MUST NOT make a nested call: the decision hook
takes one call at a time and would wait for itself.

## Nested calls

A tool whose own work is to call other tools — a code-execution kernel
with a loopback bridge — MAY **invoke** one of the turn's tools through
the loop rather than holding a tool set of its own, where the policy,
the events and the record would all be absent. The loop runs the call
as if the model had asked for it under the call in flight:

- the decision hook decides it, seeing a batch of one at index 0 and
  the **parent**, the call that made it, which a call the model made
  does not carry, so the hook can tell a nested call from a turn of one
  call and knows the deferral it returns is settled below rather than
  left for a resume. A nested call cannot be handed to the caller, since it belongs to a
  tool that is running, so a deferred one is put to the user through
  the run's elicitor when it has one, never one the invoking tool put
  in place of it, since a tool must not answer for the policy that
  holds its calls, and the decision hook sees the run's elicitor too;
  the question names
  the call, its arguments, the first 500 bytes of them, and the
  decision's reason, asked on the
  invoking tool's context so it is filed under that call, with the
  call it asks about on that context as data, its parent, ID, name,
  whole arguments and the deferral, so a front that answers for
  longer than the one call can make a rule of it without parsing the
  question. An accept
  allows the call and a decline blocks it, with `declined when asked`
  and the reason as the refusal's; either is the decision `tool_start`
  carries, an accept with the reason, or `allowed when
  asked` when there was none. The decision an answer makes is by the
  decider the answer names, under agenttool RFC 0001's Questions
  (`human`, `policy`, `agent`), or `human` when it names none, since
  an answer that names nobody is the user's; an answer naming a
  decider the session format does not know is refused, as a failure
  to ask is (#267). The answer's note reaches the model as a user
  message only for `human`; a rule's or another model's is a
  developer message, since its words are not the user's, and on a
  decline the note ends the refusal's reason, named for whoever gave
  it. A host that raises a question about a call MUST be able to
  supply the text the asking model reads when the answer declines: a
  deferral MAY carry a **decline text**, which the loop MUST use in
  place of its own `declined when asked` and the reason, and which
  the answerer's note still ends, named for whoever gave it, so the
  host's words are never attributed to the person (#304). The same
  holds wherever a call is asked about through the loop's question: a
  child run's call, a front's own held call. A front that escalates a
  call to its caller instead MUST use the decline text when the caller
  refuses it, the caller's note after it named for the caller, and a
  call held beside it reads its own hold's decline text. Without an elicitor, or on a cancel or a
  failure to ask, the hook's deferral refuses the call, and the error
  reads `a nested call cannot be deferred to the caller: ` and the
  reason. The decision's note and terminate hint are ignored;
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

A nested call already on the record keeps its identity when its
parent runs again, after a restart or a cancellation, so a keyed tool
reached through a nested call deduplicates across the parent's
attempts (#261). Under one parent, the k-th invocation of a tool with
JSON-equal arguments, the arguments the invoking tool gave before a
decision rewrote them, matches the k-th recorded nested call of that
tool with those arguments, whatever order they arrive in, and takes
its ID. A matched call that ended, with a result of its own or a
refusal rather than cut off, returns its recorded result without
running, whole: the output, the failure, a refusal with its reason, the
terminate hint and the details a recorder keeps: `tool_start` and `tool_end` are raised, both marked
`replayed`, with no decision and no `tool_dispatch`. A matched call
that was handed to its tool and never ended may have run, so it is
decided as any nested call and then held to agenttool RFC 0001's rule
for running a call again under its recorded key, the arguments it
was handed over with being those the rule compares: it runs under
that key when the rule allows, and is otherwise given the outcome
unknown as its error without running. A matched call never handed
over runs as a new one would, under a new key. An invocation with no
unconsumed counterpart is a new call, with a new ID and key, and
matching continues past it. The cost of the rule: an invoking tool
that takes another path but repeats an identical call under the same
parent gets that call's recorded result. The record a call is matched
against is the pending call's: a host seeds it from what a recorder
wrote, each nested call's invocation, its hand-off with its key, and
its end with its result whole, and a run that leaves a call pending
carries on it what it was seeded with and the nested calls the run
made under it.

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
  parent's do not run inside it. A child run's deferral SHOULD be put
  to the elicitor of the call that started the child, as a nested
  call's deferral is: a question naming the call, filed under the
  parent's call, whose answer is the decision on the child's call, by
  the decider the answer names, so the child goes on and its model
  reads what the answerer said; a call held only beside an asked one
  is not asked about, and its fate is its batch's answers: it is
  released, as the policy decided it, once every call asked about in
  its batch was accepted, and refused unrun, by the policy, once one
  was declined, with its own hold's decline text when it has one; a
  front that escalates held calls to its caller
  settles them the same way. The call
  fails with input-required only when there is nobody to ask, no
  elicitor, a failure to ask or a cancel: an error that says so and
  names the calls, so the parent's model sees it as an error output.
  The child's call is not made a pending call of the parent's run,
  which would give the loop a sub-agent concept. Every composition
  tool that fails with input-required names its waiting items in one
  shape: each item's ID, its name and arguments when it is a call, and
  the callee, a child run ID or a remote task ID, so a host asks once
  whether any callee needs input and finds what it waits on whichever
  tool failed (#84). A cancellation on the
  parent reaches the child through the context. An **observer** the
  host installs receives every event of the child run, in order, with
  the parent's call context, which is how one recorder serves every
  level of nesting; a **spawn** callback hands the host the child's
  agent, keyed by the call, so a hub can steer it, cancel it without
  cutting its siblings, or prompt it again once the tool has returned.
  A composition tool that can continue its callee from the record
  SHOULD report itself keyed (agenttool RFC 0001's replay). A call run
  again under its key SHOULD continue that callee rather than start it
  afresh, so a deferral that was open when the process stopped is
  asked again under the same call, and its answer decides the
  callee's call: the child continues its session from the record,
  the inputs it had accepted and not taken handed back to it first, as
  Queues has a recorder hand back what it owes, every call it left
  pending answered by the replay rule, a call its
  policy was deciding, which never started, put to the policy again,
  and one it held with nobody answering put to the policy again when
  the tool puts the child's deferrals to the caller's elicitor; a
  child left held, its call failing input-required as the child left
  it, keeps the inputs it owes queued until a run of it takes them; a
  child whose run had ended with nothing pending returns that end and
  runs nothing. A call run again under another key is another
  operation, and starts its callee afresh (#297). A call that repeats
  a hand-off under its key whose callee the record does not hold, the
  host keeping no record of callees, or the process having stopped
  after the callee started and before the record named it, MUST NOT
  start the callee afresh, which could repeat its work: it fails with
  the outcome unknown, as a call that may have run and must not run
  again does. A hand-off its call's output or result followed ended
  that operation, and a link written before it names no callee of a
  later one. A nested call to a composition tool, one a tool made
  through Invoke, follows the same rule, its hand-offs and its end
  read from its nested call records (Nested calls).
  A **detached** child is the background task: the call returns once
  the child's run has started, the child runs on the run context, so
  the parent's cancellation still cuts it and the parent's run ending
  does not, and its end goes to a callback from which the host delivers
  the answer with the agent's delivery, which joins the parent's run
  when it will still take the answer and starts one otherwise; a steer
  tested against whether the parent is running waits for the next
  prompt when the parent's run is past its last drain. A later run of a child the host marks as a retry
  reaches the observer marked.
- **As a peer.** The loop exposed to A2A callers, with its card derived
  from its name, description and tools; a remote A2A agent wrapped as a
  tool, a task mapped to one call and input-required to a returned
  error the model can answer, naming what the task waits on in the
  shape a child's input-required does: each call its status message
  lists, else the question, with the task as the callee. A front
  serving the loop as a peer MUST NOT leave a pending call answerable
  only through a task it no longer knows: either it keeps its tasks as
  long as the conversations that refer to them, or the next message on
  the conversation lists again the calls a lost task left pending, so
  the caller answers them on a new task. A2A asks for no durability,
  since a task may be purged (§3.3.2), so the reference front does the
  second whatever task store its host gives it (#257). A tool that
  consumes a remote agent MAY **detach** a call: the call returns once
  the remote task exists and has not finished, with an output saying
  it is running, and its callee is recorded as a peer, whose work
  outlives the call. The task is followed on the run context, by the
  stream the message went on or by a send that returns at once
  (A2A §3.2.2) followed by Get Task (§3.1.7); a question the task asks
  is answered while it is followed as it is inside a call; an abort of
  the parent's run cuts it, the follow ending and the task cancelled
  with a cancel notice recorded, and the parent's batch or run ending
  by itself does not. A call whose answer is final before the task
  runs, or that fails before it returns, returns as a call that is not
  detached does: its callee is the call's own, and a task it leaves is
  stopped. The final result
  reaches the parent through the agent's delivery, as a detached
  child's does, under a trigger of kind `callee` whose ref is the
  call's ID and whose `task` member names the callee's task, and a
  recorder writes the delivered item with agentsession RFC 0001's
  `from_link` naming the latest `peer` link of that call, to that task
  when the trigger names one, and none when the path holds no such
  link (#290). A front serving the loop as a peer MAY accept a message
  for a task that is still working, the steer A2A allows (§3.3.3); it
  is opt-in, and refusing a message on a conversation with a task in
  flight stays the default. A front that accepts it takes a message
  that names the working task, hands its text and files to the running
  agent as a delivery under the trigger of the caller that sent it,
  after the owner check any message gets, and answers with that task.
  The conversation still runs one task at a time: a message naming
  only the context starts a new task in A2A, and joining it to the
  running one is a further choice a front states; the reference front
  refuses it as busy, since its caller would hold a task that never
  runs. A run the delivery starts belongs to the same task, and the
  task does not end while the delivery is outstanding: the front
  continues the agent with it after a run that ended without taking
  it, as a delivery's own continue would; a run that stops for input
  leaves it for the message that answers the calls, whose run takes it
  after the answered batch, as the Queues section has it, and a cancel
  of that task drops it; one that fails or is cancelled drops it with
  the task. A front bounds the deliveries a task holds untaken and
  refuses one past the bound. A message carrying
  answers to pending calls, or any data part, is refused while the
  task runs, since nothing it answers has been asked. The consuming
  tool offers a way to send to a working task, the task a detached
  call follows among them (#287). A tool consuming a remote agent
  whose task waits only on calls the remote escalated SHOULD put each
  asked call to the elicitor of the call that started the task, naming
  the call, the remote's reason and the remote agent, filed under that
  call; the remote's name and reason are its own, untrusted words, and
  the question MUST show them as the remote's, not as the caller's own
  text. It SHOULD answer on the same task with one approval per asked
  call, the round whole or not at all, since the remote refuses a
  message that leaves one unanswered, carrying the answerer's note, and who decided where the remote
  accepts it, since a front that does not let its caller name a
  decider refuses the message; the caller's own record keeps the
  decider either way. A call held beside an asked one is not asked
  about. With nobody to ask, or with the task also waiting on other
  input, the call fails input-required and names those calls, as it
  does without the rule. A detached call's task, followed on the run
  context, MUST NOT be answered by asking anyone, even where the run
  context carries an elicitor the host prompted with, since the
  question would reach a person after the run moved on; it keeps the
  host's own answer (#296).
- **As itself, under new settings.** The configuration replaced and the
  transcript kept, so the next run continues the same conversation
  under another agent's instructions, tools and model. This is what an
  agent framework calls a handoff. The loop learns no handoff concept:
  the switch around it, the transfer tools, reading the transfer after
  a terminating stop, the trim, replacing the configuration and
  continuing, is the host's, and a host that takes a handoff continues
  the receiver under a trigger of kind `handoff` whose ref names the
  sender; it MUST bound the handoffs one input may cause, so two agents
  that hand a conversation back and forth stop, and at the bound it
  tells its caller so rather than answering with a transfer's text,
  the last transfer taken but its receiver not run, so the next input
  goes to the agent the conversation names, in process and served
  alike; it applies a trim
  once, between runs, leaving a transcript the loop can continue; and
  a receiver started without its own question and record channels
  inherits the sender's (#274). A host that offers transfer tools
  SHOULD offer them as it offers the agent's other tools: a filter
  that removes a tool from the offer, and a wrapper around the agent's
  tools, apply to the transfers too, on every request, and the
  check that a set of tools has distinct names (The tools of the
  moment) covers them, a tool of the moment that takes a transfer's
  name failing the turn. A wrapper keeps a transfer's name and its
  result as they are, since the host reads a handoff from the call's
  name and the transfer's own output; a wrapper that changes either
  makes the call no handoff, as a blocked call is none (#307). A
  trim between runs that hands a
  conversation to a receiver MUST leave a transcript the loop can
  continue: when the trim would leave a model output last, a transfer's
  preamble once its call is trimmed out, the host delivers a developer
  message that names the transfer, the sender and the receiver as the
  input of the receiver's run, so the receiver's first request ends in
  input the model can answer. It MUST be the run's input rather than
  part of the trimmed transcript, so the loop appends it and a recorder
  writes it, once, and every request after it keeps its hash; a front
  that rebuilds the trimmed conversation for a later request or task
  finds it in the history and MUST NOT add another (#276).
  A trim holds for the rest of the conversation: a front that keeps
  the history whole, or is sent it whole, so it can find the handoffs
  in it, MUST give the receiver's later requests or tasks the
  conversation trimmed at each handoff as it was when the handoff was
  taken, so what an agent sees does not depend on whether it took the
  handoff in process or over a wire, and the transcript it continues
  stays continuable.
  The loop's refusal to continue from a model output stands. A
  recorder writes the switch as a config delta, so the settings any
  response was produced under are on its path. A terminating tool
  result is the usual trigger, with the destination in the result's
  details where the model cannot see it. A front that owns the loop
  takes the handoff within the one request: both fronts ask a host
  function for the receiver's configuration when a run stops on a
  terminating result, and relay the receiver's run in the same
  response or task, up to a bound on the handoffs one task or request
  follows. A front that starts a run under a configuration
  other than its own, a handoff's receiver within a task or the
  configuration a later task or request starts under, gives that
  configuration the serving side's question channel (its elicitor) and
  tool record channel when it has none of its own, so a held call is
  put to the same people however the conversation reached its current
  agent (#234). A transcript holds whatever
  the previous model produced, reasoning items included, and a
  reasoning item carries a signature its own provider issued; a
  handoff that changes the model MUST leave them out of the receiver's
  requests. The loop owns that rule (#91): it leaves out of each
  turn's request the reasoning items another model name produced, as
  the request procedure says, so a configuration replaced between runs
  needs nothing more, and the transcript and the record keep the
  items. The rule covers the loop's own requests; a transform that
  calls a model of its own, as a local fold calls one for a summary,
  owns what it sends, and the reference fold sends no reasoning item
  at all, since a summariser cannot read one. A front that rebuilds a conversation the loop has not seen,
  from a store or from a caller's input, attributes its reasoning items
  by the handoffs the conversation took, those before the first to the
  configuration it started under and those after each to the
  receiver, and gives the loop that attribution; the loop never learns
  what a handoff is. The record can describe a request that leaves
  items out of the middle of its history: agentsession RFC 0001's
  `omit` names the item entries left out, their order kept, since
  format 0.11, and tool outputs and image parts sent as placeholders
  (`omit.outputs`, `omit.images`) since 0.14, under several texts
  since 0.16. A recorder that writes
  that `omit` before the response hashes the request against the path
  as edited, and only an input the format cannot describe leaves the
  responses after it unhashed, as the request procedure has it (#233).
  A handoff that trims what the receiver sees does it once, between
  runs, by replacing the transcript, and the receiver's first request
  carries the `omit` that describes the trim; a recorder moved to the
  last entry kept keeps every later response verifiable as well when
  the trim takes a prefix. Where each agent keeps a session of its own,
  the receiver's is a fork of the sender's at the handoff's last entry,
  a session whose header names that entry as its base, and every hash
  on the fork's path verifies when the receiver starts from the fork's
  context, the sender's transcript up to that entry.

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
| `run_start` | `run` start, with the loop's source as the format's, which agrees with the format's reading of the segment: a resume's first output, decision or dispatch takes up a call pending when it began, a note typed before it aside, and an input's does not, the trigger joined as `kind:ref`, or whichever is set, as `ref`, and the trigger's kind, ref and source apart as `trigger`, with the trigger's richer facts as members of that object beside them, refused when one names kind, ref or source; an `env` entry, compared with the last one written members it does not define included, when the host supplies one and it changed; the full initial `config` from the configuration's base request when nothing has been written yet, and a `config` delta from it when the configuration changed since the last run, so the items the new configuration's before-turn hook appends are filed under it. The run entry carries the hash of the configuration's base request in a member of the recorder's own, so a recorder seeded from a stored path, by a resume, a fork, a rebase, a continuation or a child session reopened under its call, compares its first run with the base that path's last run started under; a path that recorded none is compared by its settings at the leaf, leaving out instructions the host composes as parts when the base does not join them |
| `turn_start` | a `config` delta when the request's settings differ from the path's; the request hash is computed here and written on the response. When the host names the parts the request's instructions are composed of, for any session the recorder writes, a child run's included, and they join to the instructions sent, the entries carry `instructions_parts`, a delta naming the parts that moved and each run of unchanged parts as a `keep`, and `instructions_omitted` for what the host left out when it differs from the list in force, which stays in force until a `config` changes it: on a delta each run of parts unchanged in the list in force as a `keep`, `[]` when nothing is left out any more, and on a replace whole whenever it is non-empty. With the instructions recorded as parts, a recorder SHOULD write a configuration change as a delta whenever a delta can describe it, a switch to a configuration whose tools are disjoint included: a replace starts the path's parts afresh, after which a later hand-back can no longer name them by hash, so length alone is not a reason to replace; parts that do not join, or that the format refuses, a part with no ID, two sharing one, or one whose hash is not that of its text, are dropped and the string is written, since the record describes what was sent; a part carrying its text and also a keep or the hash of that text is written with both cleared, its source and trust kept, since the text says all they would. The quarantine in force, the event's `quarantined`, MUST be recorded as the format's `omit`, its outputs sent as the placeholder and every other item, a call with its outputs, left out, even when the recorder cannot describe how the request's input differs from the path and so writes no request hash, as for a run on a root of its own that sends the items of the run before it or an input a hook added to: otherwise a resume, an agent attached after one, or a child started from the record sends what the run withheld. The MUST covers a quarantine made after the run's last request, by a tool of its last batch, a hook after that request or the host during the run, which `run_end` carries, one a refused request's hook made, which `model_blocked` carries, and one the host made while no run was active, which `quarantine_added` carries, so the record holds every quarantine before the agent runs again: the recorder writes it after that event's other entries, as a `config` carrying only the `omit` it adds, so no response is hashed under it and the next request is (#294) |
| `model_retry` | a record entry with the attempt, the error, the delay, the failed attempt's model and whether the retry policy revised the request, before the response of the attempt that answers; the revised request's settings are settled with that attempt, so only the attempt that answered is configured on the path. The record entry stays beside the count below, since it says what the count cannot |
| `model_blocked` | for an error marked as a guard's, a custom entry in `agentturn:model_blocked` carrying the guard's error, the request hash, the model and the usage the event carries, when it carries any, since the run stopped rather than failed and a failed `response` would make its end read as a failure; for any other error, a failed `response` carrying the hook's error and the request hash. Either way the call that was refused is told from one that was made and failed |
| `item_end` | an `item`, with the display flag off for a hidden item, and the run's trigger as `source` for an item the run was prompted with, an answer's output on a resume excepted, whose kind `human` is not written on an item that names instruction layers, is copied or is hidden, which keeps the trigger's other members (#303). An item carrying a provenance is recorded with it in agentsession RFC 0001's members: its layers as `instructions_part`, one object for one layer and a list for several, its trust as `trust`, on a copied item merged into the trust it was copied with, and its source as `source`, over the run's trigger. Items a run is prompted with that were taken from another conversation, marked `copied`, as a child prompted with its parent's conversation is, are recorded marked as copied, with agentsession RFC 0001's `copied_from` naming the session they came from, the session whose call spawned the run's, and the entry of it each repeats unchanged, none for an item made for the copy such as a placeholder output for a call in flight, and carrying that entry's `trust`, or, for an item that names no entry, the trust of the path it was copied from, as agentsession RFC 0001 has it; a recorder writes no decision, dispatch or result for a call so marked or its output, since the call is not this session's, and a reader treats such items as context, not as this session's model output or tool calls (#243). An input that arrived under a trigger of kind `callee`, a detached call's late result whether delivered, queued or handed back owed after a restart, carries agentsession RFC 0001's `from_link` naming the latest `peer` link on the path of the call the trigger's ref names, of the task its `task` member names when it names one, and none when the path holds no such link, a cancel notice never (#290). Before a caller-supplied output for a call that was neither dispatched nor rejected, a `reject` decision with the output's text as its reason and the decider the caller named, and `policy` as the decider when the loop refused the call itself in this run, for a name no tool has or arguments that are not an object, or closed it because the output guard withheld the response that made it; before one for a call that may have run, dispatched in an earlier run or on a path that does not promise dispatch records, an `answer` decision with the answer's decider and reason, since a `proceed` would say the call went on toward its tool and a `reject` that it never reached it, and no second `dispatch` says the tool did not run again; before one for a call the path already shows answered, nothing; one for a call that has its output is refused with agentsession's `ErrCallCompleted`, since the context would hold two. The answer's output for a call that completed off the path the run continues, which the caller marked with where it was taken from, is written with a `link` to the child session that produced the output it repeats, when the record holds one: the `subsession` link a branch of this session holding the original output has for the call, written again on this branch before the `answer`, since a link on a branch left says nothing of this one; a fork writes none, since a `subsession` link names a child of the session that writes it and the child is the origin's: an `answer` whose output repeats one that ran in the session a fork was made from MUST name that output's entry and session in the output's `copied_from`, carrying its `trust`, so a reader reaches the child session that produced it through the origin's link for the call, which the fork's prefix lacks when the fork was made at the call (#237). A call the loop renamed keeps the model's ID in an `agentturn:model_call_id` member beside the `item`. An output's `item` MUST be marked `failed` when the call's `tool_end` carried the tool's error for a hand-off of this run, or when the answer it is reports that the call failed, as an output saying a cut call's outcome is unknown does; a call refused, by a block, by the loop for a name no tool has or arguments that are not an object, or by an answer's output for a call that never started, is recorded by its `reject` and MUST NOT be marked |
| `response_end` | the `response`, with `request_hash` when the input the loop sent is the input the recorded path rebuilds, under the `omit` written before it when the input differs by edits the format describes, as the request procedure has it, and none otherwise, in which case a custom entry in `agentturn:unhashed` is written before the first response left without a hash for a cause, and again when the cause changes, saying why: the reason and the position where the input sent and the input the path rebuilds part, naming the item each holds there, so a reader tells a request a hook or a transform changed from one carrying items the recorder never wrote (#92); and `attempts`, the calls it took, when the retry policy tried it again; a `response_end` marked withheld is written `incomplete` with `content_filter` and no error, as the loop gives it, so the guard's text is not on it, before the outputs that close its calls; so is the failed `response` `run_end` writes for a call left in flight, where an abort during the retry's backoff counts the attempt that was due, since nothing tells it from an abort before that attempt streamed |
| `tool_start` | a `decision`: `reject` with the reason for a block, `call blocked` when it gave none, and `answer` in its place for a call that may have run, since the format keeps `reject` for a call no `dispatch` reached; nothing for a call a `reject` or an `answer` already ended, which takes only its output, and a refusal, with `ErrCallCompleted`, for one that has its output, which the format reads as ended, so a loop that lost track of a call fails its run rather than the record hiding it; `hold` for a defer, with the reason when given, marked held, with its subject, for a call deferred only beside an asked call of its batch, as the prose after this table has it; `proceed` for a call that was held, whose arguments were rewritten, with the arguments, or whose decision gave a reason, with the reason, and for a call an earlier run dispatched that goes to its tool again, since running it again is a decision: when its decision gives no reason, the proceed is written before its second `dispatch` with `run again` as the reason, and not at all when the loop refuses the call before it. The decider is the decision's, and `policy` for a call nothing was holding and no earlier run dispatched whose decision names nobody. A nested call is a record entry instead, carrying the arguments it was invoked with when a decision rewrote them, and none for a nested call `replayed`, whose record is already on the path |
| `tool_dispatch` | a `dispatch`, durable before the event returns, so the tool runs after it or not at all, carrying the call's idempotency key as `idempotency_key`; a second one for a call an earlier run dispatched and a resume runs again, carrying the key it runs under, that of the dispatch it repeats unless a decision made it a new operation, so the path holds one per hand-off; for a nested call, a record entry carrying its key, durable before the event returns, the same for one run again; and a refusal, with agentsession's `ErrCallCompleted`, for a call whose output is on the path, so its tool does not run again as the same call; a refusal, with agentsession's `ErrCallReturned`, for a call whose `result` is on the path, and so for a decision on one at its `tool_start`, since the format takes neither after a result and the call is owed that output; and a refusal for a call no `function_call` entry on the path names, other than one the filter keeps from the model, so no tool runs unrecorded. Every `decision` and `dispatch` names its call by `target`, the entry of its `function_call`, whichever writer wrote it: a live run's, one seeded by a resume or a rebase, a reopened child's. A function call the filter keeps from the model is a custom entry, which no `target` can name, and takes none. Subscribers are called in order, so one that vetoes a dispatch is registered before the recorder; one registered after it refuses a call whose dispatch is already durable, and the call's `tool_end` says the hand-off was withdrawn, for which the recorder MUST write agentsession RFC 0001's withdrawal, a `reject` carrying `withdrawn: true` with the refusal as its reason, directly after that `dispatch`, so the call reads as it was before it, never started when it has no other, and is not held to the replay rule (#119) |
| `tool_end` | for a call the model made whose tool returned a result of its own, neither `cut` nor `withdrawn`, while another call of its batch has not settled, a `result` entry naming the call, carrying the output as the batch will append it and `failed` as its `item` will, durable before the event returns, since the batch appends the outputs only once every call has settled and a process that dies in between would otherwise leave a call that ran with nothing to show for it: a recorder MUST write it then, and MUST NOT write one for a call whose output joins as soon as it settles, the last of its batch; the output that later joins the path is that result's item, with no decision before it. Then a recordable details value as a record entry in its namespace, its `call_id` naming the call, or for a nested call the call whose tool made it; a nested call's record, carrying the result whole, its parts included, and whether the call was cut or its hand-off withdrawn, and none for a nested call `replayed`; for a child run that was not observed, its session written from the items it added and its `link`; an observed child's `link` and session are written by the observer from the child's own events, starting at its `run_start`, and the child's `queued` reports are among them: one that names a run MUST go to that run's session, and one that names no run, an input the child accepted while idle and reports before the start of the run that takes it, or accepted while its run's end was delivered and reported after it, MUST go to the session of that run, written before anything appends the item, so the item that drains it names it in `queued_from` with its trigger as `source`: when the call's child already has a session that its next run continues, the report is written there as it arrives, so it is on the record whether or not the child runs again |
| `turn_end` | nothing of its own |
| `run_end` | first, when a `turn_start` had neither a `response_end` nor a `model_blocked`, a failed `response` carrying the run's error; then `run` end, its `pending` list naming the run's calls left without an output, an earlier run's call the run wrote a decision or a `dispatch` for among them, with the reason mapped onto the format's cascade: `done`, `input_required` and `error` as themselves, the error's text as `ref`; `aborted` as `interrupted` with the error's text, since the host asked; `stopped` as `aborted` when the last `response_end` was marked withheld, since the format reads a run whose last response is incomplete so, as `stopped` when the last response made calls, as `done` when it made none, and for a run with no response of its own as `stopped` when it answered a pending call and left none, `aborted` otherwise, with the cause as `ref`, and for a guard's stop the cause followed by the guard's error |
| a record a tool writes while it runs, a question it asks the user | a record entry at the leaf, its `call_id` naming the call on the tool's context when the session holds it, or the nearest call up a nested call's chain that it holds. A question is two record entries: the question, written before it is put to the user, and the answer, written after it is given and naming the question's entry and carrying the decider the answer names, `human` when it names none, as Nested calls has it, so a run cut while the user was deciding shows the open question under the call that was waiting on it; both name the call the question is about, its tool and its parent, when the asker put that call on the context as the Nested calls section has it, and the question's entry takes that call's `call_id` when the context carries none the session holds |
| a fold the transform reports | a `compaction` naming what was kept and what was pinned, with agentsession RFC 0001 0.21's `requested` for a fold a host or a person asked for, and its `instructions` when they gave the summariser any; a recorder MUST NOT write `requested` for a fold the budget or the loop's retry after a context-length refusal forced, nor `instructions` without `requested` (#306); or a record entry for a fold that failed, carrying `requested` and `instructions` by the same rule, so a fold asked for that failed still says it was asked for, and, for one the transform backs off from, the split and the hash of the prefix it would have folded, so a transform seeded from the record in another process backs off from the same prefix |
| `queued` | a `queued` entry with the item, the mode and the trigger, its richer facts included, without the kind `human` for an input that names instruction layers, is copied or is hidden, as the item that drains it has its `source` (#303), and agentsession RFC 0001's `deliver: true` for an input that arrived as a delivery, before anything appends the item; the item entry that drains it names it in `queued_from` with the trigger as `source`. A run end closes the entries of the inputs it did not append, and the recorder writes them again after it, since the agent still holds them, a delivery again as one; so does a rewind. The loop's follow-up mode is spelled `follow_up` and the format's `followup`; a writer maps the one onto the other |

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

The sessions a recorder creates, the root and each child session it
opens for a tool's run, are described alike. A recorder that names its
harness MUST write that harness in the header of every session it
creates, unless the host names a different one for that session. A
child session MUST take the media form, `inline` or `sidecar`, of the
session whose call spawned it, unless the host names another, so the
blobs of a tree of sessions are kept one way. In a sidecar session the
recorder converts media as it writes, since agentsession RFC 0001 makes
converting an item's media the writer's: it MUST store each inline
medium it records, an image, a video or a file carried as a data URL,
base64 or percent-encoded, in any item type that carries one, and in
item, queued, result and compaction entries alike, as a blob before
the entry, write the entry with the
blob's `sidecar:` reference, and compute the request hash over the
request with the same substitution, the form a reader rebuilds. It
MUST store each blob with the media type its data URL carried, as
agentsession RFC 0002's Media section has a writer give it, the
session being given that type for the blob; a data URL whose type the
store cannot keep stays inline, as an item that needs a type of its
own does there. The loop and the model keep the inline form, and what
the record hands back to an agent, the transcript, the pending calls'
outputs, the notes and the queued inputs a resume, a reopen or a
rewind seeds it with, MUST carry each `sidecar:` reference resolved to
a data URL of the type the session holds for the blob, read for that
session, and of `application/octet-stream` for a blob held with none,
as agentsession RFC 0001's Request hash has a writer resolve one; a
reference with no reader to resolve it is an error, never handed on.
So no `sidecar:` URL reaches the agent or a provider (#284). A session created for a
run under a named configuration, such as a child the loop-as-a-tool
runs, MUST carry that name as its display name, an `info` entry before
the run's first entry, unless the host names the session otherwise; a
session reopened for a later run keeps the name it has, so a name a
person set since stands. A recorder that writes `result` as the
`tool_end` row has it SHOULD name `result`, and `queued` as it writes
the `queued` row, in the `records` of every
session it creates and writes from events, so a reader takes a call
with a `dispatch`, no `result` and no output as one whose tool had not
returned when the record stopped.

A host that starts a child agent itself, not through a model's
function call, such as a pass that consolidates memory after a turn,
MUST record it as a subsession of the session it serves. It writes the
parent's `subsession` link with a `spawn_id` before the child's header,
and the child's header names the parent in `parent_session` and the
`spawn_id` in `spawned_by` (agentsession RFC 0001, `link`). A
`spawn_id` MUST NOT repeat within the parent session. Such a child is
not the loop-as-a-tool's spawned child, which a call started and the
spawn callback of Composition hands the host: that one is linked by
its call. A recorder that offers the host a way to start one SHOULD
derive the child's ID over `["spawn","<parent>","<spawn_id>"]`, as
agentsession RFC 0001's header table recommends, so a reader computes
it from the link alone, and MUST refuse a `spawn_id` the session
already carries, on any branch, writing nothing, as that format's
`link` rule has a writer do (agentsession RFC 0003 carries the refusal
as `spawn_id_repeated`). Its session is otherwise one the recorder
creates for a child: it names the recorder's harness, takes the
parent's media form, is named after the configuration it runs under,
and is written from its run's events; a later run of the same child
continues it at its leaf, and a child it starts through a tool is its
own subsession. A child started outside the parent's run, which so
holds none of the run's quarantine, leaves out what the parent's
record quarantined. Verification, export and delegated-work accounting
then reach its work as they reach a tool's child's (#294).

A recorder that continues a path on which a run is open that it is
not running owns that run and closes it before it writes anything
else, as agentsession RFC 0001 has it: on resuming a session a crash
cut off, with `error` and a `ref` naming the cut; on a rewind into the
run, and on a fork whose base is inside it, with `interrupted` and a
`ref` naming the rewind or the fork. The inputs the closed run owed are
queued again after its end. The loop is not involved: a run it is
running is never open to anyone else.

A **rewind** of a recorded agent moves the record's leaf to an entry
and replaces the agent's transcript, pending calls, owed notes and
reasoning attribution with what the record holds there. The
quarantine goes with them: the transcript is the context there, shaped
by the `omit` in force at the entry, so what was quarantined before
the entry stays left out or a placeholder, and what the branch left
quarantined after it is not, since the record at the entry never
quarantined it; a host that wants an item kept out of the new branch
quarantines it again, as a guard that finds it again does. It is one
operation: it MUST be refused, with nothing changed, while a run is
active, and that check MUST be atomic with a run's start, which holding
the agent idle gives; a failure at any step MUST leave the agent and
the record as they were, and an error MUST NOT report an undo of a
step that changed nothing (#220).

Six facts the record needs are supplied by the caller and carried by
the loop unread: the trigger of a run, who decided an answer and why,
where an answer's output was taken from when a record held it,
whether an answer's output reports that the call failed, whether
an item a run is prompted with was taken from another conversation,
and an item's provenance. The loop acts on none of them, but takes an
item's provenance source as the trigger the item arrived under.

A loop MUST let a host that adds to the conversation text the person
did not write say where it came from and whether it is trusted, the
provenance an item carries (The transcript), at every point it takes
an item from the host: a prompt, a queue, a delivery and what the
before-turn hook returns. It MUST carry the provenance unread on the
item's events, and MUST refuse instruction layers on an item that is
not a message, and a source of kind `human` beside instruction layers.
A recorder MUST record both on the item, so a reader
never takes it for the person's words. It writes them on the item's
entry in the members agentsession RFC 0001 defines and invents none:
`instructions_part` for a message composed from instruction layers,
which a replay then does not send again as an input of its run, and
never on an item that is not a message; `trust`; and `source` for what
sent it, which on a queued input is also the `queued` entry's
`trigger`, since the item that drains an entry carries the trigger the
entry held. agentsession RFC 0001 0.21 defines one trigger kind,
`human`: a person sent the input, and a user message whose trigger has
it is the person's words. A host MUST start a run, or queue an input,
under a trigger of kind `human` only for what the person typed, and
MUST name its own kind, as the provenance's source, for a message it
composed. A front that takes input from a remote party, to prompt,
queue, deliver or resume, SHOULD carry the trigger that party claims
for it, so a person at a client claims `human` for what they typed,
and likewise the answerer it claims for a reply to a question. It MUST
run the input, or record the answer, under the claim only when the
host has authorised that principal to claim one, as for a decider
(Pending calls); otherwise it uses the front's own kind naming the
principal, `control` for a control front, and an unchecked claim is
never recorded. The front authenticates nobody and trusts no claim of
its own accord: whom to trust is the host's to decide. A recorder MUST NOT write the kind `human` on the `source`
of an item, or the `trigger` of its `queued` entry, when the item names
instruction layers, is copied or is hidden. The last is agentturn's
own rule, not the format's: agentsession RFC 0001 0.21's `visible:
false` only tells a renderer to hide an item, and its composed markers
are `instructions_part` and `from_link` alone, but agentturn treats a
hidden message as one the host prompts with, since a message kept out
of the person's view is not what they typed. For such an item the
run's or the context's trigger is written without its `kind`, and keeps its `ref`, its `source` and its other members, since
a steer's `queued` entry is the only place its arrival is written; a
trigger left with no member is not written (#303). agentsession RFC
0001 reads a user message's words from its own trigger and two
members alone: one naming `instructions_part` or `from_link` is
composed whatever its trigger; otherwise a trigger of kind `human` is
the person's words, one of another kind, an empty kind included, is
not, and no trigger makes no claim, which a reader never presents as
the person's either. `copied_from` and `visible` are not among those
members: a copy or a hidden message is told from the person's words
by its trigger alone, which is why a recorder clears the kind on them.
An entry recorded before 0.21 that carries `human` beside
`instructions_part` is read as composed, as agentsession RFC 0001
reads it, and a recorder queuing such an owed input again keeps its
trigger as recorded. A layer named here carries no trust of its own, so the
item's trust MUST hold every label the host knows of each layer it
names, beside any the content has for another reason, as agentsession
RFC 0001's `instructions_part` has a writer carry them: the host, which
alone knows the layers' labels, supplies that union. A copied item
carries the trust of what it copies, and agentsession RFC 0001 0.21
lets a copy's `trust` hold more labels than its source's, never fewer
(agentsession#279), so a recorder MUST NOT drop a label its provenance
names: it writes the copy's trust with every label of the source's and
every label of the provenance's (#300). An input queued with its marks, hidden or with a
provenance, and still owed when the process stops is handed back after
a restart marked as it was queued, so the item that drains it is
recorded as the item it was: a recorder writes them on every `queued`
entry it writes for the input, a run end's again included, as
agentsession RFC 0001's `visible` (`false`), `trust` and
`instructions_part` on the `queued` entry (format 0.21,
agentsession#278), and reads them back where it reads the inputs the
path owes. A recorder MUST also read them, for each the entry does not
carry in the format's member, from the interim `agentturn:visible`,
`agentturn:trust` and `agentturn:instructions_part` members a draft 0.6
recorder wrote, each in the form the format's member of that name has
on an `item`, and MUST NOT write those members: an entry that queues
such an input again carries the marks in the format's members alone.
An input queued marked copied is recorded as the copy it is on its
`queued` entry too: the entry's `trigger` without the kind `human`, as
the item's `source` has it, and its `trust` the trust the item that
drains it carries, the copy's merged with the provenance's, since the
drained item carries its entry's trust as written. The format's
`queued` entry has no member for the copy mark itself, so an input
owed across a restart is handed back with that trigger and trust,
unmarked.

A decision that defers a call MAY mark it **held**, deferred only
because another call of its batch was asked about, as a policy that
holds the calls it would allow beside one it asks about does, and MAY
name the part of the call it is about, one command of a compound shell
line. A recorder that writes `hold` for a call deferred only because another
call of its batch was asked about MUST mark that hold as held, and
SHOULD record the part of the call the deferral is about. A recorder
writes the mark as the hold `decision`'s `agentturn:held` member,
`true`, and the part as its `agentturn:subject` member, a string;
agentsession RFC 0001 defines neither, and a hold without the mark is
a call asked about. A reader that seeds pending calls from the record
MUST read both back from those members onto the pending call's
decision, however it reads the session, a sidecar one's blobs through
its read options included, so a front over a restarted session asks
about the asked call alone and says which part it is about (#299). A
recorder SHOULD write a deferral's decline text as the hold
`decision`'s `agentturn:decline_text` member, a string, and a reader
that seeds pending calls SHOULD read it back onto the decision, so a
call reopened from the record is refused in the host's words, a
child's held call beside a declined sibling among them (#304).

## Bindings

### Go

The root module of this repository is the reference loop. The contract
maps onto it as follows:

| Contract | Go |
| --- | --- |
| transcript | `Transcript = openresponses.Items`; `Items` for a fragment |
| hidden item | `Hidden(item)`, `Unhide(item)`; `Hidden` on the item events |
| provenance | `WithProvenance(item, Provenance{Layers, Source, Trust})` (`Layer{ID, Source}`, `Trust{Reads}` in agenttool's labels), `ProvenanceOf`, `ErrProvenance`; `Provenance` on the item events and `Queued` |
| copied item | `Copied(item)`, which `tools/agent`'s `WithCallConfig` sets on the items it takes from the parent; `Copied` on the item events |
| filter | `Config.Filter`; `DefaultFilter`, `VisibleFilter(types…)` |
| reasoning another model produced | `ReasoningModels` (`Attribute`, `For`); `WithReasoningModels` for an agent, `ContextWithReasoningModels` for `Run` and `Continue`, and on every hook's and tool's context |
| transform | `Config.Transform`; `compact.New`, `compact.NewLocal` as the reference; `compact.MarkFold`/`compact.IsFold` for a fold's own model call; the person's own messages, `PersonMessages` (`Has`), `PersonMessagesFromContext` for a transform, `WithPersonMessages`/`Agent.SetPersonMessages` for an agent and `ContextWithPersonMessages` for `Run` and `Continue`, kept by `compact` under `compact.WithPersonBound` (`compact.DefaultPersonBound`) and the newest within `compact.WithPersonTotal` (a quarter of the budget by default, at most half of what the settings leave) beside `compact.WithPin`, and read from a record by `session.TranscriptMarks`, which `session.AgentOptions` and `MoveTo` use; `(*compact.Transform).FoldNext(instructions)` for a fold on request, `ContextWithFoldRequest`/`FoldRequested` for the loop's after an overflow, `compact.Fold.Forced`, and `Requested`/`Instructions` for a fold asked for through `FoldNext` alone; the settings a transform reads, `ContextWithRequest`/`RequestFromContext`, weighed by `compact.EstimateSettings`, and images by `compact.ImageTokens` in `compact.Estimate` |
| configuration | `Config`; `Config.BaseRequest`, `Config.ResolveTools` |
| tools of the moment | `Config.Tools`, `Config.ToolProvider` |
| execution mode, bound | `Config.ToolExecution` (`ExecParallel`, `ExecSequential`), `Config.MaxParallelTools` |
| turn limit | `Config.MaxTurns` |
| retry policy | `Config.Retry{MaxAttempts, Backoff, Retryable, Revise}`; `DefaultBackoff`, `DefaultRetryable` |
| low-level loop | `Run(ctx, t, prompts, cfg)`, `Continue(ctx, t, cfg)` → `iter.Seq[Event]`; `EventBuffer`; `CanContinue` |
| delivery a control offers | `Deliverer`, which `*Agent` and `front/control`'s `Client` implement; `front/control` serves it as `POST /deliver`, `ErrNoDelivery` when the control offers none; in two steps, `Accepter` (`Accept` → `*Delivery`, whose `Wait` and `Done` give the outcome; `NewDelivery` for another implementation), which `*Agent`, `front/control`'s `Client` and `session`'s `RecordedControl` implement, served as `POST /accept`: the accept, then the outcome, on one response; an outcome's `ErrAborted` crosses as the `aborted` code beside the cause's, so a remote caller's `errors.Is` tells it from the end of its own wait |
| a remote caller's trigger | `front/control`'s `Client` sends the trigger on its caller's context (`ContextWithTrigger`) as a `trigger` member of `POST /prompt`, `/queue`, `/deliver`, `/accept` and `/resume`, and the answerer on `Reply`'s context (`ContextWithAnswerer`) as an `answerer` member of `POST /reply`; the `Handler` runs the input, or names the answerer, under it for a principal holding the scope `WithTriggerClaims` names, its kind and ref defaulting to `control` and the principal's name, and otherwise under `control` naming the principal |
| agent | `Agent`; `New(cfg, opts…)`, `WithTranscript`, `WithPending`, `SetPending`, `WithReservedCallIDs`, `ReserveCallIDs`; `Prompt`, `Continue`, `Resume`, `Steer`, `FollowUp`, `Queue`, `Deliver`, `Accept`, `Subscribe`, `Abort`, `AbortCause`, `WaitForIdle`, `WhileIdle`, `State`, `SetConfig`, `SetTranscript`, `SetReasoningModels`, `Config` |
| refusals before a run | `ErrNoPrompt`, `ErrCannotContinue`, `ErrNoModel`, `ErrInputRequired`, `ErrNotPending`, `ErrRunning` (`ErrHeld` for a delivery during a hold, `State.Held`), `ErrTerminated` (a delivery after a terminating stop), `ErrAborted` (a delivery an abort stopped before a run took it, wrapping the cause), `ErrAmbiguousCall`, `ErrCallAnswered`, `ErrTriggerExtra` (`Trigger.Validate`) |
| run ID, turn, trigger, transcript on the context | `ContextWithRunID`/`RunIDFromContext`, `ContextWithTurn`/`TurnFromContext`, `ContextWithTrigger`/`TriggerFromContext`, `ContextWithTranscript`/`TranscriptFromContext`, `ContextWithReservedCallIDs` |
| run context, steer signal | `RunContext(ctx)`, `Steered(ctx)` |
| source | `Source`: `SourceInput`, `SourceResume` |
| events | `Event` with `EventType()`; `RunStart`, `TurnStart`, `ModelRetry`, `ModelBlocked`, `ItemStart`, `ItemUpdate`, `ItemEnd`, `ResponseEnd`, `ToolStart`, `ToolDispatch`, `ToolUpdate`, `ToolEnd`, `TurnEnd`, `RunEnd`, `Queued`; the `Event*` name constants |
| tool recorder | `Config.ToolRecorder`, the executor's recorder for every batch; `session.Recorder.RecordFunc` is the value a session recorder offers |
| tool elicitor | `Config.ToolElicitor`, installed on every tool call's context; `session.Recorder.Elicitor(by, fn)` wraps one so the question and the answer are recorded under the call |
| reason, cause | `Reason` (`ReasonDone`, `ReasonStopped`, `ReasonInputRequired`, `ReasonAborted`, `ReasonError`); `StopCause` (`StopMaxTurns`, `StopHook`, `StopGuard`, `StopTerminate`, `StopPartialTerminate`, `StopRefused`) |
| idempotency key | `ToolDispatch.IdempotencyKey`, on `agenttool.Call` for the tool |
| pending call | `PendingCall{Call, Reason, Tool, Dispatched, IdempotencyKey, Args, Nested}`, `PendingCall.MayHaveRun`; `PendingReason` (`PendingDeferred`, `PendingAborted`, `PendingUndispatched`, `PendingUnknown`, `PendingAnswered`, `PendingRejected`, `PendingReturned`); `PendingCalls` |
| decision | `ToolDecision{Action, Reason, Terminate, Args, By, Note, Held, Subject, DeclineText}`; `ToolAction` (`Allow`, `Block`, `Defer`) |
| answer | `Answer{CallID, Output, Args, Note, Terminate, By, Reason, Origin, Failed, IdempotencyKey, RunAgain}`; `Output`, `Approve`, `ApproveWith`, `Refuse`, `OutcomeUnknown`, `WithNote`, `WithBy`, `WithReason`, `WithOrigin`, `WithFailed`, `WithIdempotencyKey`, `WithRunAgain`, `RunAgainReason`, `OutcomeUnknownReason`; `Answer.Item` for an output opening a `Prompt`, carrying the answer's decider, reason, origin and failure; `ContextWithDeciders`/`DeciderFromContext`, `ContextWithReasons`/`ReasonFromContext`, `ContextWithOrigins`/`OriginFromContext` and `ContextWithFailures`/`FailedFromContext` for a host driving `Run` |
| hooks | `Config.BeforeTurn`, `BeforeModelCall`, `OutputGuard`, `BeforeToolCall`, `AfterToolCall`, `ShouldStopAfterTurn` |
| guard stop | an error wrapping `ErrGuard` from `ShouldStopAfterTurn`, `BeforeTurn`, `BeforeModelCall`, `OutputGuard` or the model; the output of a call a withheld response made is `WithheldCallOutput` |
| chains | `ChainBeforeTurn`, `ChainBeforeModelCall`, `ChainOutputGuard`, `ChainBeforeToolCall`, `ChainShouldStopAfterTurn`, `ChainTransform` |
| nested call | `Invoke(ctx, name, args)`; `ErrNoInvoker`; `Parent` on the tool events, `ToolStart.Invoked`, `Replayed` on `ToolStart` and `ToolEnd`; `PendingCall.Nested` holding `NestedRecord{CallID, Parent, Name, Args, Dispatched, IdempotencyKey, DispatchArgs, Ended, Result, Err, Blocked, Reason}`, `RecordedDetails` for the details a replayed result gives back |
| queue mode | `QueueMode`: `QueueSteer`, `QueueFollowUp`; `Queued.Input` (`InputSteer`, `InputFollowUp`, `InputDeliver`); `Agent.Redeliver` for a delivery a record owes |
| the loop as a model | `front/responses.New(cfg)` → `openresponses.Adapter` |
| handoffs a front takes | `WithHandoff`, `WithStart`, `WithTransfers(route)` in both fronts; `front/responses.Route`, `Handoffs`, `HandedTo`, `Attribute`, re-exported by `front/a2a` but for `Attribute`; `front/responses.TrimHandedOff`, `HandoffNote`, `TransferName` |
| the handoff | `handoff.New(agents…)` → `*Team` (`Agent{Config, Handoffs []To}`, `To{Agent, Filter, Enabled}`); `Team.Config`, `Team.Offer` for the host's offer over each agent's whole list, transfers included, `Team.Route` (a `front/responses.Route`, which both fronts' `WithTransfers` take), `Team.Run` (→ end, path), `Team.Resume`, `Team.Handoff` and `Team.Trim` (for the fronts' `WithHandoff` and `WithHandoffTrim`), `Team.MaxHandoffs`, `ErrTooManyHandoffs`, `ToolName`, `RemoveAllTools`, `Dest`; the fronts' `WithMaxHandoffs` and `WithHandoffTrim` |
| the loop as a tool | `tools/agent.New(cfg, opts…)` → `agenttool.Tool`; `ChildInfo`; `InputRequiredError`, which implements `agentturn.InputRequired` (`PendingCalls() []WaitingCall`), as `tools/a2a`'s does; `WithAskParent`; `WithArgs`, `WithStrictArgs`, `WithTranscript`, `WithObserver`, `WithSpawn`, `WithRunContext`, `WithDetach`, `WithNoAnswer`, `WithToolName`; `ContextWithRetry`; `WithRecord` (`Record`, its owed inputs as `QueuedInput`), with `session.Recorder.ChildRecord` as its loader, for a call run again under its key, and `ErrNoRecord` when the record holds no child of a repeated hand-off |
| the loop as a peer | `front/a2a.New(cfg)`, `front/a2a.AgentCard`, `front/a2a.WithRecorderFor`; `tools/a2a.New(client, card)`; an escalated call put to the caller's elicitor: `tools/a2a.WithAskCaller()`, `WithSendBy()`, `AskedCall.Callee`; a detached call: `tools/a2a.WithDetach(fn)`, `WithPollInterval`, `Answer(final)`, and `CalleeTrigger(callID, task)` (`TriggerCallee`) to deliver its result under; a steer of a working task: `front/a2a.WithDeliverWhileBusy()`, served by `Executor.RequestHandler`, `MaxSteers`, `Redeliverer` and `ContextWithRedeliverer` for a recorded one (`session.Recorder.Redeliver`), and `tools/a2a.Steer(ctx, client, info, parts…)` |
| quarantine | `Agent.Quarantine`, `Agent.Quarantined`; `Quarantine(ctx, items…)` from a hook or a tool, `ErrNoQuarantine`, `ErrNotQuarantinable`; `QuarantinedOutput`, the placeholder; `QuarantineFromContext` → `QuarantineView` (`Has`, `Generation`, `Apply`) for a transform, which `compact` reads; `NewQuarantineSet` → `QuarantineSet` (`Add`, `Items`, `Has`, `Apply`), `ContextWithQuarantineSet` for a host carrying one through `Run` and `Continue`, and `WithQuarantineSet` for an `Agent`; `QuarantineSet.Bind` and `Merge` for a front keeping a conversation's set across tasks or requests, as `front/a2a` does per context and `front/responses` under `WithQuarantineScope` (`QuarantineScope`, `MaxQuarantineScopes` per principal); `session` `Recorder.Attach` seeds it from the record's `omit` |
| the record | `session.Recorder`; `Start`, `Resume`, `Continue`, `Attach`, `Handle`, `Observe`, `ChildContext`, `StartChild` and `ContinueChild` for a child the host starts, `Fold`, `Annotate`, `EntryOf`, `Omit`, `RecordFunc`, `Elicitor`, `Queue`, `Deliver`, `Accept`, `Redeliver`, `Control` (`RecordedControl`, for a front), `Requeue`, `Rebase`; `session.MoveTo` for a rewind; `WithInstructionsParts`, `WithMedia`; `session.RequestHash`; the replay rule from the record: `session.AgentOptions`, `session.Pending`, `session.ReplayAnswers`, `session.CallIDs`, read with `Recorder.ReadOptions`, `session.WithBlobs` resolving a sidecar session's media (`session.ErrNoBlobReader` without one); the compaction back-off from the record: `session.CompactOptions`, `session.LastFailedFold`, `compact.WithFailedFold`, `compact.PrefixHash` |

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

**A conforming front** feeds prompts, resumes, queued items and deliveries in and
consumes events out, and nothing else; correlates `tool_end` with
`tool_start` by call ID; renders withheld text on `item_end` and not
from deltas; and answers pending calls through the resume.

**A conforming recorder** writes the entries the record table gives
from the events it names, writes the `dispatch` from `tool_dispatch`
and from nothing earlier; describes an input that differs from the
path only by edits the format's `omit` can say, a trim or a
quarantine among them, with a `config` delta written before the
response and hashes it, declining a request hash only for an input
the path cannot rebuild, and records the quarantine `turn_start`,
`model_blocked`, `run_end` and `quarantine_added` carry as `omit`
whether or not it can describe the input; writes a
`result` entry, durable before
`tool_end` returns, for a call whose tool returned while another call
of its batch has not settled, and none for the last of its batch; and,
when it offers the host a way to start a child of its own, writes the
`spawn_id` link before the child's header and refuses a `spawn_id` the
session carries on any branch, writing nothing; records an item's
provenance on its entry as `instructions_part`, `trust` and `source`;
and marks held a hold for a call held only beside an asked one. The
rule on a host that starts such a child is not checked here: this
section has no class for a host, and a host meets it by starting the
child through a conforming recorder.

### The scenario corpus

The reference's own tests hold the rules three defects found on the
way to this draft broke: a batch a hook or a consumer fails inside
pairs every call, a decision's terminate hint on an allowed call, and
a wire error event before the answer as a retried attempt (#101,
#102, #103).

`testdata/loop/` holds the scenarios a loop is checked against, one
JSON file each. A scenario drives an agent through one or more runs
against a scripted model and scripted tools, with no network, and says
what the agent must deliver: the events of each run, its end and the
transcript after it. A conforming agent passes every scenario. A second
implementation writes a runner in its own language that reads the
files as they are and drives its own agent through them; the
reference's runner is `conformance_test.go` at the root of this
repository, and it uses the module's exported API alone.

#### A scenario

| member | holds |
| --- | --- |
| `name` | the file's name without `.json` |
| `description` | the rule the scenario pins, in a sentence |
| `rules` | the anchors of the sections of this document it exercises, each of which MUST name a heading here |
| `config` | the configuration: `model`, the model name; `instructions`; `max_turns`; `tool_execution`, `parallel` or `sequential`; `max_parallel_tools`; `retry.max_attempts`; `fold`, the items a transform folds the transcript to when the loop asks it for a fold, leaving it as it is otherwise, and no transform when absent. Zero or absent is the member's default |
| `tools` | the tools of the moment, in order: each a `name`, its `sequential` and `barrier` properties, its `replay`, `safe`, `keyed` or `unknown`, and the `result` it answers a call with when `results` names none for the call |
| `results` | a scripted result per call ID |
| `decisions` | the decision hook's decision per call ID: `action`, one of `allow`, `block` and `defer`, with a `reason` and a `terminate` hint. A call the map does not name, and every nested call, is decided none. A scenario without the member has no decision hook |
| `transcript` | the items the agent starts from |
| `pending` | what a record says of calls of `transcript`, which the agent is seeded with: each a `call_id`, its `reason`, the `idempotency_key` and `args` of the hand-off it would repeat, and `nested`, the nested calls the record holds under it, each a `call_id`, `parent`, `name`, the `args` it was invoked with, whether it was `dispatched`, with its `idempotency_key` and `dispatch_args`, and whether it `ended`, with the `output` or the `error` it ended with |
| `model` | the model's attempts, consumed in order across every run of the scenario |
| `runs` | the runs, in order, on one agent |
| `known_divergence` | the issue that tracks where the reference loop breaks the scenario's rule; a runner reports the scenario skipped and names it |

A **scripted result** is one of: `output`, the text the tool returns,
with `terminate` setting the result's terminate hint; `error`, a
message the tool fails with; `wait` with the value `cancel`, a tool
that returns only when its call's context is cancelled, and then
returns the cancellation; and `invoke`, a nested call the tool makes
through the invoker, a `name` and `arguments` object, before it returns
its `output` or fails with the nested call's failure, or `invokes`,
several made in order, the first failure failing the tool. A result
that invokes and gives no `output` returns the outputs of its nested
calls, one a line. A tool given no
result for a call fails the scenario.

A **scripted attempt** is either an answer, `id`, the response ID, and
`output`, the items the response holds, streamed as a well-formed Open
Responses stream and completed; or a failure, `partial`, the items
streamed before the failure, and `error`, a `message`, whether it is
`retryable`, whether it is marked as a `guard`'s refusal, and the Open
Responses error `code` it carries, `context_length_exceeded` say, with
which the attempt fails. An answer's `no_done` lists the output indexes
whose `output_item.done` the stream leaves out, so only
`response.completed` completes those items, and `at_index_zero` streams
every item at output index 0, one after the other, as some servers
do, while `response.completed` lists each at its own position. A stream opens each item
with `output_item.added`, sends its text, arguments or summary as one
delta and closes it with `output_item.done`, between
`response.created` and `response.completed`. The runner's retry
policy waits no time between attempts and retries the errors the
script marks retryable and nothing else. A model called more or fewer
times than the scenario scripts fails it.

A **run** is a `prompt`, its items; a `continue`, `true`; or a
`resume`, its answers, each a `call_id` and either an `output`, the
text of the call's output, or `approve`, `true`, and optionally
`terminate`. A run MAY name events at which the runner acts, each an
**event pattern** matched against the event as delivered the way
expected values are matched below:

- `cancel_at`: the runner cancels the run's context;
- `fail_at`: the runner's subscriber fails;
- `steer_at` and `follow_up_at`: an `event` pattern and `items`, which
  the runner queues as a steer or a follow-up;
- `deliver_at`: an `event` pattern and `items`, which the runner
  delivers from its subscriber with the context the event was handed,
  a delivery from inside the run, which joins it and does not wait.

The runner subscribes one subscriber before the run, which records each
event and then acts on it, so the event it fails on is recorded too.

A run's `expect` holds `refused`, true for a run the agent must refuse
before it starts, delivering no event; `events`; `end`, the run's end
event; and `transcript`, the agent's transcript after the run. Any of
them MAY be left out, and what is left out is not compared.

#### Matching

Each event is **projected** to a JSON object whose `type` is the
event's name and whose members are the event table's, under those
names, with `turn` on every event of a turn. `error` is a boolean that
says whether the event carries one, since the text of an error is the
binding's. A decision is `action`, `reason` and `terminate`, and absent
when there is none; a result is `output`, as the Open Responses output
value, and `terminate`; an item, a request and a response are their
Open Responses JSON; a pending call is `call_id`, `name` and `reason`.
`turn_start` carries `inputs` as items and `arrived` as a list of
`item` and `mode`. The end is the `run_end` projection's `reason`,
`cause`, `error`, `pending` and `withheld`.

The run's events other than `item_update` and `queued` are compared one
to one, in order, with `expect.events`, and the two lists MUST be the
same length. The `item_update` events count how a stream was cut, which
is the model's, and a `queued` report MAY fall between any two events.

An expected value matches when it is a **subset** of the projected one:
an object matches an object that holds each of its members with a
matching value, an array matches an array of the same length element by
element, `null` matches a value that is null or absent, and any other
value matches an equal one. A scenario therefore pins only what its
rule says, and never a run ID, a delay, an ID the loop or the stream
mints, a timestamp or a binding's error text.

A scenario MUST be deterministic under any conforming loop: where the
completion order of a batch would vary, it runs the batch
sequentially, gives it one call, orders it with a barrier, or holds a call with `wait`.

#### The scenarios

| scenario | what it pins |
| --- | --- |
| `clean-done` | one turn answered by a message ends `done`; the request carries the settings and the loop's transport members |
| `tools-then-done` | a call runs after `response_end`, its output is appended, and the next request carries it |
| `preflight-settles` | a call naming no tool and one whose arguments are not an object are settled in preflight with the batch section's error outputs |
| `deferred-resumed-by-output` | a deferred call ends the run `input_required` with the call pending as `deferred` and an empty result in its place on `turn_end`; a resume's output answers it |
| `deferred-resumed-by-approval` | an approval runs the call as a batch at turn 0 without the decision hook |
| `resume-refusal-terminates` | an answer that asks to terminate ends the run `stopped` with cause `refused` without a model call |
| `pending-refuses-prompt` | a prompt over an unanswered call is refused with no event; one that opens with the call's output is a resume |
| `blocked-with-terminate` | a blocked call is never dispatched, and a decision's terminate hint ends the run |
| `terminate-unanimous` | every result terminates: cause `terminate` |
| `terminate-partial` | some results terminate: every call is answered and the cause is `partial_terminate` |
| `max-turns` | the turn limit after a turn with calls: cause `max_turns`, the outputs appended |
| `retry-then-answer` | an attempt that delivered nothing is retried, and the turn has one `turn_start` and one `response_end` |
| `retry-drops-held-reasoning` | a reasoning item held by a failed attempt is dropped without an `item_end` |
| `cancel-mid-batch` | a cancellation ends each started call, appends the finished call's output, leaves the cut call `aborted` and raises no `turn_end` |
| `subscriber-fails-during-cancellation` | a subscriber failing on one cut call's `tool_end` does not cost the others theirs; the run ends `aborted`; a call the cut reached first is `undispatched` |
| `nested-call` | a nested call's events carry its parent, fall between the parent's dispatch and end, and append nothing |
| `nested-replayed` | a parent run again whose invocations match ended nested calls on the record, in another order, gets each its recorded result under its ID, `replayed`, with no dispatch |
| `nested-dispatched-runs-again` | a nested call handed over and never ended runs again under its recorded ID and key when its tool is keyed, and is given the outcome unknown without running when its tool says nothing |
| `nested-diverges` | an invocation the record does not hold is a new call, matching continues past it, and a second identical invocation is new |
| `steer` | a steered item is appended after the batch and `turn_end`, with mode `steer` |
| `follow-up` | a follow-up is appended when the run would end, and the run takes another turn with mode `follow_up` |
| `deliver` | a delivery from inside the run joins it as a steer would and the turn that takes it reports mode `deliver` |
| `pending-returned` | a call pending as `returned` takes no approval, the resume refused before it starts, and is answered by an output |
| `model-blocked` | a model's guard refusal before the answer opened raises `model_blocked` in place of `response_end`, is not retried, and ends the run `stopped` with cause `guard` |
| `call-completed-by-response` | a function call only `response.completed` completes joins the transcript under the ID it opened with before its batch runs, and its output is paired with it (#236) |
| `fold-retry` | a request refused as longer than the window is folded through the transform and sent again as a retry, never unchanged |
| `calls-pair-by-item-id` | calls a stream opens at one output index pair with the response's by item ID, never by output index, one only the response completes among them (#236) |

Four rules are not in the corpus, because a scenario cannot express
them: a delivery from outside the run, which waits, or starts a run
once the run in flight has ended, or leaves its items queued with the
error after a terminating stop, needs a caller concurrent with the run
and a host that switches the configuration between runs, which a run
of the corpus does not have; the quarantine needs a before-model-call
hook that names items by identity, which the runner does not script;
the record's rules need a recorder, which the corpus does not run; and
a front's rules need a front. The reference's own tests hold them.

## Versioning

This document is versioned with the Go module. A draft number changes
when a rule is added or changed; a rule's removal or a change that
makes a conforming loop non-conforming is a breaking change to the
module and is listed in the changelog as one. Each draft lists what it
changed, in a section of its own.

The scenario corpus is versioned with this document. A scenario is
added when a rule is, and changes only when the rule it pins does, in
the same draft; a scenario that a conforming loop fails is a defect in
the scenario, fixed without a change of rule.

## Changes in draft 0.8

Draft 0.8 adds the rules below to draft 0.7, marked as Versioning has
it.

- **Queues.** An abort the agent receives after a delivery is accepted
  and before its follow-on run starts settles that delivery as aborted
  and starts no run, the items left queued, when the abort finds no
  run to stop as much as when it reaches the run in flight. Both
  report that the delivery was aborted, with the aborted run's end
  when one ended on the abort, told from the end of the caller's own
  wait; breaking for a caller that read draft 0.7's aborted end with
  no refusal (#312).

## Changes in draft 0.7

Draft 0.7 adds the rules below to draft 0.6, marked as Versioning has
it.

- **The record.** An owed queued input's marks are written in
  agentsession RFC 0001 0.21's `visible`, `trust` and
  `instructions_part` on the `queued` entry, and the interim
  `agentturn:` members draft 0.6 wrote are read back, never written
  (agentsession#278); breaking for a recorder that wrote them, and for
  a reader of a session that does not read the format's members.
- **The record.** A recorder merges a provenance's trust labels into
  a copied item's trust, as agentsession RFC 0001 0.21 lets a copy
  hold more labels than its source, where draft 0.6 had it refuse the
  item (agentsession#279).
- **The record and the transcript.** A trigger of kind `human`, which
  agentsession RFC 0001 0.21 defines, is the host's for what the
  person typed alone; a provenance with instruction layers MUST NOT
  name a source of that kind (a loop refuses it), and a recorder MUST
  NOT write that kind on the source or queued trigger of an item that
  names instruction layers, is copied or is hidden, which keeps the trigger's
  other members (#303); breaking for a host
  that names such a source, and for a recorder that wrote the run's
  trigger on every prompt item.
- **Events and the record.** The `queued` event carries `copied`, and
  a recorder writes a copied input's `queued` entry with the copy's
  merged trust and without the kind `human`, so the item that drains
  it matches it (#303).
- **The record.** A fold a host or a person asked for is recorded with
  agentsession RFC 0001 0.21's `requested` and its `instructions`, and
  one the budget or an overflow's retry forced with neither; the record
  entry of a failed fold carries them by the same rule (#306).
- **Retry.** A loop decides whether to retry by the error's code
  before its status, and never retries `insufficient_quota`, whatever
  its status (#308); breaking for a predicate that retried a 429 by its
  status alone.
- **Pending calls.** A prompt that opens with outputs for pending
  calls carries, per output, who decided, why, where it was taken from
  and whether it reports a failure, as a resume's answer does, and a
  recorder records them as a resume's (#305).
- **Nested calls.** A deferral may carry a decline text, which the
  asking model reads in place of the loop's `declined when asked` when
  the answer declines, followed by the answerer's note; a front that
  escalates a call to its caller uses it for the caller's refusal, and
  the record keeps it as the hold's `agentturn:decline_text` (#304).
- **Composition, as a peer.** A tool consuming a remote agent puts the
  calls the remote escalated to its caller to the elicitor of the call
  that started the task, and answers them on the same task with an
  approval each (#296).
- **Composition, as itself.** A host offers the transfer tools as it
  offers the agent's other tools, its filter and its wrappers applied
  to them on every request (#307).
- **The request, step 2.** The loop makes available to the transform
  which items are the person's own messages, and a host that rebuilds
  a transcript gives them back; a transform that folds keeps them
  verbatim up to a bound the host sets, the newest within a total
  less than the request's budget, and records them as pinned (#298).
- **Queues.** A delivery accepts its items before it waits, and a
  caller can take the acceptance without waiting for the outcome,
  which the agent settles, and starts no run once the run in flight
  was aborted before taking it; a front SHOULD carry both steps, and a
  recorder writes the entry before the accept. A run in flight SHOULD
  report an accepted item without waiting for its next phase, in a
  bounded number of passes after an event, and a subscriber's error on
  such a report ends the run at its next event (#302).
- **The record.** A front that takes remote input SHOULD carry the
  trigger the party claims, `human` for what a person at a client
  typed, and the answerer it claims for a reply, and runs the input or
  records the answer under it only for a principal the host has authorised
  to claim one, otherwise under its own kind; the control front's
  `control` stays the default (#303).
- **Composition, as a tool.** A composition tool that can continue its
  callee from the record SHOULD report itself keyed, and a call run
  again under its key SHOULD continue that callee, so a deferral open
  when the process stopped is asked again under the same call, and the
  inputs the callee owed are handed back to it, and kept by a callee
  still held until a run of it takes them; one
  whose callee the record does not hold fails with the outcome
  unknown rather than start it afresh, a nested call as much as one
  the model made (#297).

## Changes in draft 0.6

Draft 0.6 adds the rules below to draft 0.5, marked as Versioning has
it.

- **The transcript and the record.** An item the host or a hook
  composed from text the person did not write may carry its
  provenance: the instruction layers it was composed from, what sent
  it and its trust. A host that adds such text MUST be able to say so,
  and a recorder MUST record it on the item as agentsession RFC 0001's
  `instructions_part`, `trust` and `source`, the source also as the
  `queued` entry's `trigger` (#300); breaking for a recorder. The
  item's trust holds the labels the host knows of its layers; a copy's
  trust is never narrowed; an owed queued input keeps its marks across
  a restart in `agentturn:visible`, `agentturn:trust` and
  `agentturn:instructions_part` members until agentsession's `queued`
  entry defines them (agentsession#278).
- **Events.** `item_start`, `item_end` and `queued` carry the
  provenance, and `queued` the item's source as its trigger (#300).
- **The record.** A hold written for a call deferred only beside an
  asked call of its batch MUST be marked held, as the decision's
  `agentturn:held` member, and SHOULD carry the part of the call the
  deferral is about, as its `agentturn:subject` member; a reader that
  seeds pending calls MUST read both back from them, and a seeded
  pending call keeps them (Pending calls) (#299); breaking for a
  recorder or a reader that does not. The scenario corpus holds no
  record rules, so `session/held_test.go` pins it.

## Changes in draft 0.5

Draft 0.5 adds the rule below to draft 0.4, marked as Versioning has
it.

- **The record.** A host that starts a child agent itself, not through
  a model's function call, records it as a subsession linked by a
  `spawn_id`: the link before the child's header, the header's
  `parent_session` and `spawned_by`, the child's ID derived over
  `["spawn",…]` (SHOULD), a repeated `spawn_id` refused (MUST, on a
  recorder), and the parent's recorded quarantine carried to a child
  started outside its run (agentsession RFC 0001 0.19, `link`; #294);
  breaking for a host that starts such a child and does not record it.
- **Events and the record.** `turn_start`, `model_retry`,
  `model_blocked` and `run_end` carry the run's quarantine as
  `quarantined`, a new `quarantine_added` reports one the host made
  while no run was active, and a recorder MUST record it as `omit` even
  for a request whose difference from the path it cannot describe, for
  a quarantine made after the run's last request and for one made
  between runs (#294); breaking for a recorder.
- **Conformance.** A conforming recorder that starts a host's child
  writes its link first and refuses a repeated `spawn_id` (#294).

## Changes in draft 0.4

Draft 0.4 adds the rules below to draft 0.3. A new MUST that a loop, a
front or a recorder conforming to draft 0.3 can fail makes it
non-conforming, so under Versioning each such change is breaking, and
is marked so here; the module's changelog lists the change that meets
it.

- **The transcript.** An item under a provider's slug is kept by the
  filter, and leaving it out for another provider is the adapter's
  (#285).
- **The request.** The transform's context carries the settings the
  request will carry (#279). A request refused as longer than the
  window is never sent again unchanged: the loop asks the transform for
  a fold and sends the folded request as a retry outside the retry
  policy's count (#277); breaking. A request's input is built for the
  model it names, and that model is credited with the reasoning its
  answer produced (#286); breaking. The quarantine: a host or a hook
  MAY quarantine an item for the rest of the conversation, the loop
  applies it after the transform and the filter, and a transform, a
  front and a recorder MUST honour it, across a handoff's legs, a
  front's tasks or requests and a reopened record (#281); breaking. A
  recorder MUST describe an input that differs from the path only by
  edits the format's `omit` can say, and decline the hash only
  otherwise (#233, #92); breaking.
- **The model call.** A model's refusal marked as a guard's stops the
  run with cause `guard`, not `error`, and one made before the answer
  opened raises `model_blocked` in place of `response_end`, with the
  usage the provider reported (#275); breaking. A function call only
  the terminal response completes joins the transcript, under the ID
  it opened with, before its batch runs, and a response's calls pair
  with the stream's by item ID, call ID or open order, never by output
  index (#236); breaking. `item_start` and `item_update` carry a
  snapshot of the item (#218). The context of everything a turn calls
  carries the turn's number, 0 in a resume's approved batch (#275).
- **Ending a run.** A failure during an abort is reported on its own
  beside the cause (#83).
- **Pending calls and resume.** A call a record holds a result for and
  no output is pending as `returned`, takes no approval and is never
  run again (#273); breaking. A resume's answer has no default
  decider; a front records the decider a remote party names only when
  the host authorised it, and its stated default otherwise (#267,
  #291); breaking. An output answer MAY say the call failed (#278).
- **Delivery and the queues.** An agent offers a delivery, and a front
  that lets a remote party queue input SHOULD offer it too (#288); it
  is recorded as one (#244). A delivery MUST NOT start a run once the
  last run ended on a terminating result (#274, #290); breaking. A
  rewind is one operation no run interleaves with (#220); breaking.
- **Nested calls.** A nested call's deferral goes to the run's
  elicitor, never to one the invoking tool installed, and its answer
  is by the decider the answer names, `human` when it names none
  (#267); breaking. A nested call made again when its parent runs again
  keeps its ID and key (#261); breaking.
- **Composition.** A host that takes a handoff bounds the handoffs one
  input causes, leaves a transcript the loop can continue, delivers
  the developer note a trim needs as the receiver's input, keeps a
  trim for the rest of the conversation, and gives a receiver started
  without its own channels the sender's (#274, #276, #234); breaking.
  A child's deferral SHOULD be put to the elicitor of the call that
  started it, and what a composition tool waits on has one shape
  (#84). A run a tool starts carries the parent's reasoning
  attribution and quarantine (#242, #281), and records the
  conversation it opens with as copied (#243, #237). As a peer: a front
  MUST NOT leave a pending call answerable only through a task it no
  longer knows (#257); a call MAY detach (#290); a front MAY accept a
  steer of a working task (#287).
- **The record.** The sessions a recorder creates name its harness,
  take their parent's media form and are named after their
  configuration (#219, #283, #292); breaking. In a sidecar session the
  recorder stores media as typed blobs as it writes, and resolves every
  reference it hands back to an agent to a data URL of its type (#284);
  breaking. A recorder writes a
  call's `result` when it settles before its batch (#273), a delivery's
  `deliver` (#244), a copy's `copied_from` (#243, #237), a late
  result's `from_link` (#290), a withdrawn dispatch as the format's
  reject (#119, #278), a failed output as failed (#278), an observed
  child's queued reports (#289), why a request was not hashed in
  `agentturn:unhashed` (#92), and the decider on a question's answer
  (#267); the MUSTs among them are breaking for a recorder.
- **Conformance.** The recorder rule names the `omit` and `result`
  MUSTs. The corpus adds `deliver`, `pending-returned`,
  `model-blocked`, `call-completed-by-response`,
  `calls-pair-by-item-id` and `fold-retry`, and its runner the `fold`
  transform, `no_done`, `at_index_zero`, a `guard` or `code` error and
  `deliver_at`.
- **Open questions.** Closed: a dispatch a later subscriber refuses
  (#119), who closes a provided tool (#95), the failure beside a
  cancellation (#83), a fold identifying itself (#88), a key for a
  nested call (#145, #261), input required as an interface (#84), the
  handoff (#96, #274) and why a request was not hashed (#92, answered
  by `agentturn:unhashed`). A second execution under one call (#87),
  resolved in draft 0.3, is no longer listed.

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

- **The stop decision as data** (#82). A guard stop is an error
  wrapping a sentinel, and a hook that returns a bare error records a
  failure and is retried by a supervisor that honours policy stops.
  Returning a decision with a cause instead, with the sentinel kept as
  sugar, is proposed.
- **A front that serves a person** (#80). The Open Responses front is
  agent-as-a-model by design. Steering, cancellation, a deferred call's
  question and a tool's progress have no carrier over a socket. A
  second front with a written-down, versioned extension vocabulary and
  a way for the server to advertise it is proposed.
