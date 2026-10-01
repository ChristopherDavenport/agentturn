package responses

import (
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// Route says where a transfer call hands the conversation: the
// configuration that receives it, and the text the transfer tool
// answers the call with when it runs and hands off. false means the
// call is no transfer, or none the conversation may take.
type Route func(call *openresponses.FunctionCall) (to agentturn.Config, output string, ok bool)

// Handoff is a transfer the conversation took.
type Handoff struct {
	// Call is the transfer call.
	Call *openresponses.FunctionCall
	// Output is the index in the transcript of the call's output, after
	// which the conversation is the receiver's.
	Output int
	// To is the configuration route named for the call.
	To agentturn.Config
}

// Handoffs returns the transfers in t that handed the conversation on,
// in transcript order: the calls route names whose output, after the
// call, is the text route gives, the transfer tool's own. A call
// answered otherwise did not hand off: one BeforeToolCall blocked or
// rejected, one whose tool failed, and one an OutputGuard withheld,
// which the loop closes with [agentturn.WithheldCallOutput] unrun. So
// is a call with no output, and one whose output has parts in place of
// text.
//
// The transcript records no decline: a transfer [WithHandoff] declined
// ran and keeps the tool's text. A host whose WithHandoff can decline
// declines the same calls in route, or blocks them before they run.
func Handoffs(t agentturn.Transcript, route Route) []Handoff {
	type transfer struct {
		call   *openresponses.FunctionCall
		to     agentturn.Config
		output string
	}
	open := map[string]transfer{}
	var out []Handoff
	for i, item := range t {
		switch it := item.(type) {
		case *openresponses.FunctionCall:
			if to, output, ok := route(it); ok {
				open[it.CallID] = transfer{it, to, output}
			}
		case *openresponses.FunctionCallOutput:
			tr, ok := open[it.CallID]
			if !ok {
				continue
			}
			delete(open, it.CallID)
			if it.Output.Parts == nil && it.Output.Text == tr.output && it.Output.Text != agentturn.WithheldCallOutput {
				out = append(out, Handoff{Call: tr.call, Output: i, To: tr.to})
			}
		}
	}
	return out
}

// HandedTo returns the configuration the last of the [Handoffs] in t
// handed the conversation to, and false when t holds none. It is the
// function [WithStart] wants for a host whose handoffs are transfer
// tools.
func HandedTo(t agentturn.Transcript, route Route) (agentturn.Config, bool) {
	hs := Handoffs(t, route)
	if len(hs) == 0 {
		return agentturn.Config{}, false
	}
	return hs[len(hs)-1].To, true
}
