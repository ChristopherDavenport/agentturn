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
// nonetheless holds a pending call to a tool the agent offers and the
// caller does not, left by an older release or seeded, is not the
// caller's to answer: the call is dropped when the conversation is
// loaded, as an aborted run's unanswered calls are, so the next message
// goes on without it, and a message that answers it is refused as
// invalid params. A pending call whose name the caller owns too, under
// [WithCallerTools] or its declaration, stays the caller's, since a
// receiver of a handoff without the agent's tool of that name offered
// the caller's. The question is put from the hook, before any call of
// the batch runs, so the batch waits on the person as it would on any
// hook; before this, a deferral ended the run instead.
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
