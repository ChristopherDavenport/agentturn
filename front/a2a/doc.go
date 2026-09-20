// Package a2a is the serve side of A2A for agentturn: it exposes an
// agentturn.Config as an A2A agent through the a2a-go SDK, so a loop can
// be a peer of any A2A caller. The consume side, a remote A2A agent as a
// agenttool.Tool, is the tools/a2a module; together they let an agent behind
// A2A be a component of another agent and the reverse.
//
// [Executor] implements a2asrv.AgentExecutor. Each message/send runs the
// loop once with agentturn.Run on the transcript stored for the message's
// context ID, streams assistant text as artifact chunks, and ends the
// task from the run's reason: completed, canceled, failed or, when the
// model called a tool the caller owns, input-required with the pending
// function_call items on the status message.
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
package a2a
