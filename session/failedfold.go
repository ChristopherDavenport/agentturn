package session

import (
	"encoding/json"
	"fmt"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentturn/compact"
)

// LastFailedFold returns the last failed fold on the path to the
// session's leaf that the transform backed off from, the one carrying a
// prefix hash, or nil when there is none. It is the fold a transform
// in this process would still remember.
func LastFailedFold(s *agentsession.Session) (*FailedFold, error) {
	if s.Leaf() == "" {
		return nil, nil
	}
	path := s.Path(s.Leaf())
	for i := len(path) - 1; i >= 0; i-- {
		c, ok := path[i].(*agentsession.CustomEntry)
		if !ok || c.NS != FailedFoldNS {
			continue
		}
		var f FailedFold
		if err := json.Unmarshal(c.Data, &f); err != nil {
			return nil, fmt.Errorf("session: failed fold %s: %w", c.ID, err)
		}
		if f.PrefixHash != "" {
			return &f, nil
		}
	}
	return nil, nil
}

// CompactOptions returns the options that seed a compact transform with
// the session at its leaf: compact.WithFailedFold with the
// [LastFailedFold], so a host that restarts, or resumes the session in
// another process, does not ask again for a summary that failed. It is
// what a host resuming a session passes to compact.New or
// compact.NewLocal beside its own options, as [AgentOptions] is for
// agentturn.New; a host handed the recorder rather than the session
// asks [Recorder.CompactOptions]. The fold's prefix is the transcript the transform was
// given when it failed; one a compaction before it shortened is not
// the context a resume rebuilds, so it is not matched, and the
// transform asks once more and remembers that.
func CompactOptions(s *agentsession.Session) ([]compact.Option, error) {
	f, err := LastFailedFold(s)
	if err != nil || f == nil {
		return nil, err
	}
	return []compact.Option{compact.WithFailedFold(f.Split, f.PrefixHash, f.TokensBefore)}, nil
}
