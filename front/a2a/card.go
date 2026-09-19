package a2a

import (
	"strings"

	"github.com/ChristopherDavenport/agentturn"
	"github.com/a2aproject/a2a-go/a2a"
)

// AgentCard derives the A2A agent card from the config: Name and
// Description describe the agent, and the skills are one default skill
// from the description plus one skill per tool, so a caller sees what
// the agent can do without a second description. url is the JSON-RPC
// endpoint. Callers set Version, Provider and security fields as they
// see fit.
func AgentCard(cfg agentturn.Config, url string) *a2a.AgentCard {
	name := cfg.Name
	if name == "" {
		name = "agent"
	}
	skills := []a2a.AgentSkill{{
		ID:          skillID(name),
		Name:        name,
		Description: cfg.Description,
		Tags:        []string{"agent"},
	}}
	for _, t := range cfg.Tools {
		skills = append(skills, a2a.AgentSkill{
			ID:          skillID(t.Name()),
			Name:        t.Name(),
			Description: t.Description(),
			Tags:        []string{"tool"},
		})
	}
	return &a2a.AgentCard{
		Name:               name,
		Description:        cfg.Description,
		URL:                url,
		Version:            "0.1.0",
		ProtocolVersion:    string(a2a.Version),
		PreferredTransport: a2a.TransportProtocolJSONRPC,
		Capabilities:       a2a.AgentCapabilities{Streaming: true},
		DefaultInputModes:  []string{"text/plain", "application/json"},
		DefaultOutputModes: []string{"text/plain", "application/json"},
		Skills:             skills,
	}
}

func skillID(name string) string {
	id := strings.ToLower(strings.TrimSpace(name))
	id = strings.NewReplacer(" ", "_", "/", "_", ":", "_").Replace(id)
	if id == "" {
		return "default"
	}
	return id
}
