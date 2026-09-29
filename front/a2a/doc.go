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
// (agentturn.StopGuard) with no answer (agentturn.RunEnd.Answer) fails
// the task with the guard's error rather than completing it, whichever
// hook the guard is on, ShouldStopAfterTurn after a turn that only
// called tools included.
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
// from there. The transcript in the [ConversationStore] holds the
// unanswered calls in the meantime, which is the invariant the root
// package documents for an input-required boundary. A2A's
// input-required state and agentturn's ReasonInputRequired are the
// same thing seen from the two sides.
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
