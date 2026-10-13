// Package a2a is the serve side of A2A for agentturn: it exposes an
// agentturn.Config as an A2A agent through the a2a-go SDK, so a loop can
// be a peer of any A2A caller. The consume side, a remote A2A agent as a
// agenttool.Tool, is the tools/a2a module; together they let an agent behind
// A2A be a component of another agent and the reverse.
//
// [Executor] implements a2asrv.AgentExecutor. Each message/send runs the
// loop once, on an agentturn.Agent seeded with the transcript stored
// for the message's context ID, streams assistant text as artifact
// chunks, and ends the task from the run's reason: completed, canceled,
// failed or, when the model called a tool the caller owns,
// input-required with the pending function_call items on the status
// message. A run a guard stopped
// (agentturn.StopGuard) with no answer (agentturn.RunEnd.Answer) is a
// refusal: the task ends rejected, with [RefusedText] as its status
// message, rather than completed or failed, whichever hook the guard
// is on, ShouldStopAfterTurn after a turn that only called tools
// included, and OutputGuard withholding the message it was given,
// after a message of the same response it let through or not, which
// is no answer (agentturn.RunEnd.Withheld). The
// guard's error is not sent, since its text may carry the rule a
// caller could phrase around; the host has it on RunEnd.Err and in the
// record.
//
// A task keeps its artifacts, so under a configuration with an
// OutputGuard the executor does not stream a message's text: it writes
// the message's artifact whole when the message completes, from what
// the guard left. The original text of a message the guard replaced,
// or stopped the run on, never enters the task.
//
// A run that a terminating tool result stopped
// (agentturn.StopTerminate or agentturn.StopPartialTerminate) is how a
// handoff ends the sender's part. [WithHandoff] is asked for the
// receiver's configuration, which the executor sets on the task's
// agent before continuing it, so the receiver answers within the same
// task, its text streamed as the sender's was, and the record holds
// both runs, the receiver's under a trigger of kind "handoff" naming
// the sender. The agent attributes each reasoning item to the model
// that produced it, so the receiver's requests leave out the sender's
// when the two run different models (agentturn.ReasoningModels).
// Without the option, or when it declines, a terminating stop with no
// answer completes the task with the text of the last output of a call
// whose result set Terminate, the answer the tools gave on the model's
// behalf; a sibling's output, or a blocked or failed call's, is not
// taken for it. The handoff lasts for the task: [WithStart] or
// [WithTransfers] picks the configuration the conversation's next task
// starts under, and without either the task starts under the
// executor's own.
//
//	exec := a2a.New(cfg)
//	handler := a2asrv.NewHandler(exec)
//	mux.Handle("/invoke", a2asrv.NewJSONRPCHandler(handler))
//	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(a2a.AgentCard(ctx, cfg, url, version)))
//
// # Caller-owned tools
//
// A caller that executes some tools itself declares them on its message
// under [MetaCallerTools] as Open Responses function tools. They are
// offered to the model alongside the agent's own tools, and a
// BeforeToolCall defers every call to one of them, so the loop ends the
// run with ReasonInputRequired and the pending calls listed. The task
// enters input-required and the status message carries those
// function_call items as data parts. The caller answers by sending a
// message for the same task whose data parts are the function_call_output
// items, one per pending call and nothing else, and the run continues
// from there. A message never carries a function_call: a call is the
// agent's to make, and one is refused as invalid params. Nor may it
// declare a tool named as one the agent offers, or one the route
// [WithTransfers] gives takes for a handoff, whose calls the caller
// would answer in the agent's place. A nested call a tool makes to a
// caller-owned tool is blocked: the caller answers only the calls the
// model made. The transcript in the [ConversationStore] holds the
// unanswered calls in the meantime, which is the invariant the root
// package documents for an input-required boundary. A2A's
// input-required state and agentturn's ReasonInputRequired are the
// same thing seen from the two sides.
//
// The caller answers exactly the calls to the tools it owns: those its
// message declared and those the executor's [WithCallerTools] gave
// every caller. A call to one of the agent's own tools is never handed
// to it, its answer taken as the tool's output, whichever hook holds
// the call. When the agent's BeforeToolCall defers such a call, as a
// policy that asks a person does, the question goes to the serving
// side: the call is put to Config.ToolElicitor with agentturn.Ask,
// the call on the elicitor's context for agentturn.AskedCallFrom, and
// the person's accept runs the tool while their decline refuses the
// call, both recorded as decided by "human". Without an elicitor, or
// with no answer, the call is refused with a reason saying it cannot
// be handed to the caller, and the model sees that reason as the
// call's output. The task therefore never goes input-required on a
// call of the agent's own, and a transfer the hook held becomes a
// handoff only when the person lets it run. A stored conversation that
// nonetheless holds a pending call to a tool the agent offered when it
// made the call and the caller does not own, left by an older release
// or seeded, is not the caller's to answer: the call is dropped when
// the conversation is loaded, as an aborted run's unanswered calls are,
// so the next message goes on without it, and a message that answers
// it is refused as invalid params. Which configuration made a call is
// what the conversation up to it starts under, as [WithStart] or the
// route of [WithTransfers] picks it, the executor's own without either;
// a call that configuration did not offer, or whose name the caller
// owns under [WithCallerTools] or its declaration, stays the caller's,
// since a receiver of a handoff without the agent's tool of that name
// offered the caller's and the sender's deferred call to it is the
// caller's whichever agent answers next. The question is put from the
// hook, before any call of the batch runs, so the batch waits on the
// person as it would on any hook; before this, a deferral ended the
// run instead.
//
// # Recording
//
// [WithRecorderFor] attaches a record to each conversation: it is
// handed every task's agent, with the context ID, before the run
// starts, and subscribes what writes the conversation's session. The
// subscriber sees every event in step with the run, before the
// executor relays it, and its failure fails the task; a record that
// cannot be opened fails the send before any task exists. The
// transcript still comes from the [ConversationStore], which drops
// the calls an aborted or failed run left unanswered while the session
// keeps them as cut off, so the store is the conversation's source and
// the session its record.
//
// # Authentication
//
// [WithAuthenticator] names a principal on every call and makes each
// task and conversation its creator's. [Executor.Interceptor], which
// [Executor.RequestHandler] installs, authenticates every call of the
// A2A handler from its metadata, on every method and transport, and
// refuses one it does not accept before it reaches the executor or the
// task store, so an unauthenticated message naming a task leaves that
// task as it was. It refuses, as task not found, a call about a task or
// conversation another principal owns, or a stored conversation whose
// owner was lost, and it does not serve tasks/list. The executor runs no
// task for a call that did not pass the interceptor. Each task's run is prompted under the trigger
// {Kind: "a2a", Ref: principal}, so the record says who asked.
// [Executor.Handler] answers an HTTP request early with 401;
// [Executor.Secure] declares the scheme on the card, which is served
// apart, unauthenticated, so a caller can learn it. TLS is the
// deployment's.
//
// # Escalation
//
// [WithEscalation], which requires [WithAuthenticator], changes who
// answers the agent's own policy: a call of the agent's own tool its
// BeforeToolCall defers is put to the caller, not to
// Config.ToolElicitor. The task enters input-required with the call
// among the status message's data parts and the policy's reason as text
// and under [MetaEscalatedCalls], and the caller answers with an
// [Approval] data part on a message for the same task, beside the
// outputs of any calls to its own tools. Only the calls the policy
// deferred in that task may be approved, and an approval runs the call
// as the policy decided it, its rewrite of the arguments, which the
// question shows, and its note included. A call the policy held only
// beside an asked one is not put to the caller; the policy releases it
// with the caller's answers. The run the answer resumes is under the caller's principal,
// and the caller's note reaches the model as the caller's words, in the
// user's voice, never the developer's. It is for a headless agent nobody
// watches, whose caller's human plane should decide; without it each
// system's policy is its own.
//
// # Cancel
//
// tasks/cancel on a task with a run in flight aborts the run, and the
// task ends canceled. A task its last run left input-required has no
// run: its calls are pending in the conversation, the caller's own and
// those put to it under [WithEscalation], and a caller that gives the
// task up no longer asks for them. The cancel settles them before the
// task ends canceled, while the conversation waits on the task
// ([WaitingStore]): each call the task's status message listed is
// refused with agentturn.Refuse, its output saying "the caller
// <principal> canceled the task, so this call was not run", and the
// answer's reason the same. The refusal is a run of the conversation's
// agent, which what [WithRecorderFor] attached records, under the
// caller's trigger {Kind: "a2a", Ref: principal} and naming nobody as
// the decider, as an [Approval]'s refusal does, so a session recorder
// writes each as the caller's refusal of the held call, with that
// reason. The run ends without calling the model, since nobody asks for
// the work; a call the policy held beside an asked one does not run.
// The [ConversationStore] then holds no unanswered call, the calls put
// to the caller are forgotten, and the conversation's next message
// starts clean, the model reading the refusals with it. A late answer
// to a settled call is refused as invalid params. A failure to record
// fails the cancel, as it fails a task. The settlement does not depend
// on the cancel's request, so a caller that sends the cancel and hangs
// up still has the calls settled.
//
// A run lets go of its record and of the conversation before it writes
// the task's final status, so a cancel sent as soon as the caller sees
// input-required finds no run and settles; a cancel that comes before
// the status reaches the SDK settles too, since the conversation waits
// on the task by then. The SDK can still answer such a cancel with
// a2a.ErrTaskNotCancelable: a2a-go v0.3.15 hands a cancel that arrives
// while it holds the finished execution to that execution, whose result
// is already input-required (the TODO in its local manager's
// handleCancelWithConcurrentRun). The conversation is settled then, but
// the SDK's task store keeps the task input-required: a message on the
// task within a minute ends canceled, a later one that answers the calls
// is refused as answering no pending call and one that does not runs as
// a new turn, and a retried cancel ends the task canceled. The
// settlement needs nothing the process held, so a cancel after a restart
// settles the task the old process left, from the task the
// [ConversationStore] names as waited on ([WaitingStore]) or, under a
// store that keeps none, from the calls the task's status listed. A
// cancel from a principal that does not own the task, or may not write its
// conversation, is refused as not found and writes nothing. A second
// cancel finds the task canceled and settles nothing. The cancel holds
// the conversation as a task does, so it never settles while a task
// runs: when another task is in flight on the conversation, that task
// answers the calls or lists them as its own, and the cancel settles
// nothing. Nor does it when a later task has moved the conversation on:
// a message that answered the calls, or one that answered none of them
// and so listed them again under its own task, leaves them that task's
// to answer or cancel. A message on the task that was on its way when
// the cancel came, let through before the task was canceled, does not
// run: the task stays canceled.
//
// # One task per conversation
//
// A context ID runs one task at a time, so a conversation's store and
// its record never see two runs at once. A message on a context ID
// with a task in flight is refused at once with [ErrConversationBusy],
// wrapped with a2a.ErrInvalidRequest, rather than queued: the caller
// sends again once the task has ended, the answers to an
// input-required task among them, and a served agent whose tool sends
// to its own conversation gets the error rather than waiting on
// itself.
package a2a
