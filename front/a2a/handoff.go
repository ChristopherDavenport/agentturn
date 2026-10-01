package a2a

import (
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/front/responses"
)

// Route says where a transfer call hands the conversation; it is
// front/responses' [responses.Route], which both fronts share.
type Route = responses.Route

// Handoff is a transfer the conversation took, as [responses.Handoff].
type Handoff = responses.Handoff

// Handoffs returns the transfers in t that handed the conversation on,
// as [responses.Handoffs] does.
func Handoffs(t agentturn.Transcript, route Route) []Handoff {
	return responses.Handoffs(t, route)
}

// HandedTo returns the configuration the last of the [Handoffs] in t
// handed the conversation to, as [responses.HandedTo] does: the
// function [WithStart] wants for a host whose handoffs are transfer
// tools.
func HandedTo(t agentturn.Transcript, route Route) (agentturn.Config, bool) {
	return responses.HandedTo(t, route)
}
