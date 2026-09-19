// Package agentturn is a composable agent loop over Open Responses: one
// loop, one transcript type, one tool contract, hooks and queues.
// Everything else is a front that feeds prompts in and consumes events
// out, or a subscriber.
//
// The transcript is openresponses.Items. What a session stores, what the
// model receives and what a front renders are the same bytes. The model
// is any openresponses.Streamer: a remote server through
// openresponses.Client.AsAdapter, a local adapter, or another agent
// served by front/responses.
//
// # Vocabulary
//
// A run is one [Run], [Continue], [Agent.Prompt] or [Agent.Continue]
// until the agent goes idle. A turn is one model call plus the tool
// executions it requested.
//
// # Low-level loop
//
// [Run] and [Continue] are observational: they yield [Event] values in
// order and the loop does not wait for the consumer between phases.
//
//	for ev, err := range agentturn.Run(ctx, transcript, prompts, cfg) {
//		if err != nil { ... }
//		switch e := ev.(type) {
//		case *agentturn.ItemUpdate:
//			if d, ok := e.Stream.(*openresponses.OutputTextDeltaEvent); ok {
//				fmt.Print(d.Delta)
//			}
//		}
//	}
//
// # Agent
//
// [Agent] adds queues, subscribers and run control over the same loop.
// Subscribers are awaited in registration order and every event is a
// barrier: tool preflight for a turn does not start until every
// subscriber has returned for the assistant item, and [Agent.Prompt]
// settles only after the run_end subscribers finish.
//
// # Composition
//
// The loop never learns a sub-agent concept. It knows a [Model] and a
// list of agenttool.Tool, and every composition is one of those two things:
// front/responses serves a loop as a model, tools/agent wraps a config as
// a tool, and the A2A and MCP adapters do the same across a protocol.
package agentturn
