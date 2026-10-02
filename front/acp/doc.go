// Package acp serves agentturn agents over the Agent Client Protocol
// (https://agentclientprotocol.com), so an editor or a terminal client
// that speaks ACP can drive a loop: Zed, an ACP TUI, or a test client
// over an in-memory pipe. It is a front: it feeds prompts in and turns
// the loop's events into ACP session updates. It speaks v1, the stable
// protocol, and the v2 draft, through github.com/ironpark/acp-go.
//
// [New] takes a function that builds the [agentturn.Agent] of one ACP
// session from the session's working directory, so a host decides per
// session which configuration, tools, recorder and transcript it gets.
// [Server.Serve] runs one connection over a transport and speaks
// whichever version the client's initialize asks for; [Server.AgentV1]
// and [Server.AgentV2] are the constructors acp1.NewAgentSideConnection
// and acp2.NewAgentSideConnection take, for a host that serves one:
//
//	srv := acp.New(func(ctx context.Context, s acp.Session) (*agentturn.Agent, error) {
//		return agentturn.New(cfgFor(s.Cwd)), nil
//	}, acp.WithInfo("dex", version))
//	err := srv.Serve(ctx, acpgo.NewStdioTransport(os.Stdin, os.Stdout))
//
// # Updates
//
// Both versions stream a run's events as session/update notifications
// as they happen:
//
//   - output_text deltas as agent_message_chunk, reasoning and
//     reasoning summary deltas as agent_thought_chunk, in v2 under the
//     ID of the item they belong to;
//   - tool_start as a tool call with status pending, its raw input the
//     arguments the tool receives; tool_dispatch moves it to
//     in_progress, tool_update replaces its content with the partial
//     output, and tool_end completes or fails it with the output the
//     model sees. A nested call, one a tool made with agentturn.Invoke,
//     is reported the same way with the invoking call's ID under
//     [MetaParent] in the update's _meta.
//
// The prompt's text, embedded text resources and resource links become
// one user message; images become input_image parts.
//
// # Turns in v1
//
// A session/prompt is one [agentturn.Agent.Prompt], and the
// [agentturn.Agent.Resume] calls its permission requests lead to, until
// the run ends without input required. The response carries the usage
// summed over every response of the turn and a stop reason from the
// run's end: end_turn for done and most stops, refusal for a guard's
// stop, max_turn_requests for MaxTurns, cancelled for an abort. A run
// that ends with an error is answered with the error. A session runs
// one prompt at a time.
//
// # Turns in v2
//
// A session/prompt is answered once its message is in the transcript,
// with the message's ID, and the message is echoed as a user_message
// under that ID. A prompt to an idle session starts a turn, reported
// running, that runs as a v1 turn does and ends with an idle update
// carrying the stop reason and the turn's usage. A prompt to a session
// whose turn is running joins it: the message is steered into the run
// (agentturn.Agent.Steer), which takes it after the current tool batch,
// and the prompt is answered then. A message steered after the run took
// its last steer is taken by another run of the same turn, so the turn
// goes idle only once nothing is queued. The turn reports
// requires_action while a permission request is out and running again
// once it is answered. A run that ends with an error ends the turn
// idle with acp2.StopReasonInternalError and the error under "error" in
// the update's _meta.
//
// session/cancel aborts the run and the turn ends idle, cancelled. A
// message steered into it and not yet taken stays queued, as the agent
// keeps it for its next run, and starts the next turn at once.
//
// # Permission
//
// A call the configuration's BeforeToolCall defers ends the run with
// input required. Each pending call is put to the client as a
// session/request_permission, allow once or reject once, with the
// deferral's reason as the tool call's content. An allowed call is
// approved and runs inside the loop; a rejected one is refused with
// [RefusedOutput] and ends the run without calling the model again, so
// the user says what to do instead, and is marked failed. Both are
// recorded as decided by "human". A turn cancelled while the client is
// choosing leaves the calls pending, marked cancelled (failed in v1);
// the session's next prompt answers them first, a call that may have
// run with agentturn.OutcomeUnknown and any other with [NotRunOutput].
//
// A configuration whose ToolElicitor is [Elicitor] asks a nested
// call's question the same way, under the nested call's ID.
//
// # Not yet
//
// An item marked agentturn.Hidden is never streamed, so it needs no
// counterpart, but the deltas of a message an OutputGuard withheld have
// already gone out and neither version can take them back. There is no
// follow-up queue, no way to withdraw a queued message, and no
// session/load, session replay on resume, modes or slash commands.
// Sessions live in memory for the life of the [Server], per protocol
// version, and session/resume finds them there.
package acp
