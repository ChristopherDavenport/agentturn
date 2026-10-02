// Package acp serves agentturn agents over the Agent Client Protocol
// (https://agentclientprotocol.com), so an editor or a terminal client
// that speaks ACP can drive a loop: Zed, an ACP TUI, or a test client
// over an in-memory pipe. It is a front: it feeds prompts in and turns
// the loop's events into ACP session updates. It speaks ACP v1, the
// stable protocol, through github.com/ironpark/acp-go.
//
// [New] takes a function that builds the [agentturn.Agent] of one ACP
// session from the session's working directory, so a host decides per
// session which configuration, tools, recorder and transcript it gets.
// [Server.Agent] is the constructor acp1.NewAgentSideConnection takes,
// and [Server.Serve] runs one connection over a transport:
//
//	srv := acp.New(func(ctx context.Context, s acp.Session) (*agentturn.Agent, error) {
//		return agentturn.New(cfgFor(s.Cwd)), nil
//	}, acp.WithInfo("dex", version))
//	err := srv.Serve(ctx, acpgo.NewStdioTransport(os.Stdin, os.Stdout))
//
// # Turns
//
// A session/prompt is one [agentturn.Agent.Prompt], and the
// [agentturn.Agent.Resume] calls its permission requests lead to, until
// the run ends without input required. The prompt's text, embedded
// text resources and resource links become one user message; images
// become input_image parts. session/cancel aborts the run and the turn
// ends cancelled.
//
// The run's events go out as session/update notifications as they
// happen:
//
//   - output_text deltas as agent_message_chunk, reasoning and
//     reasoning summary deltas as agent_thought_chunk;
//   - tool_start as a tool_call with status pending, its raw input the
//     arguments the tool receives; tool_dispatch moves it to
//     in_progress, tool_update replaces its content with the partial
//     output, and tool_end completes or fails it with the output the
//     model sees. A nested call, one a tool made with agentturn.Invoke,
//     is reported the same way with the invoking call's ID under
//     [MetaParent] in the update's _meta.
//
// The prompt response carries the usage summed over every response of
// the turn and a stop reason from the run's end: end_turn for done and
// most stops, refusal for a guard's stop, max_turn_requests for
// MaxTurns, cancelled for an abort. A run that ends with an error is
// answered with the error.
//
// # Permission
//
// A call the configuration's BeforeToolCall defers ends the run with
// input required. Each pending call is put to the client as a
// session/request_permission, allow once or reject once, with the
// deferral's reason as the tool call's content. An allowed call is
// approved and runs inside the loop; a rejected one is refused with
// [RefusedOutput] and ends the run without calling the model again, so
// the user says what to do instead. Both are recorded as decided by
// "human". A turn cancelled while the client is choosing leaves the
// calls pending; the session's next prompt answers them first, a call
// that may have run with agentturn.OutcomeUnknown and any other with
// [NotRunOutput].
//
// A configuration whose ToolElicitor is [Elicitor] asks a nested
// call's question the same way, under the nested call's ID.
//
// # Not yet
//
// ACP v1 has no counterpart for several things the loop reports: an
// item marked agentturn.Hidden is never streamed, so it needs none,
// but the deltas of a message an OutputGuard withheld have already
// gone out and cannot be taken back; steer and follow-up queues have
// no v1 method, since a v1 session runs one prompt at a time; and
// session/load, session/list, modes and slash commands are not served.
// Sessions live in memory for the life of the [Server], and
// session/resume finds them there.
package acp
