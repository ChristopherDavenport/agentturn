package session

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/ChristopherDavenport/agentsession"
)

// The namespaces of the records tools/a2a writes through
// agenttool.WriteRecord while a call runs: the callee the call reached,
// and the cancel the call sent it. The recorder writes each as a link
// entry of the call rather than as a custom entry, reading the JSON
// those records are documented to carry, so this module imports
// neither tools/a2a nor a2a-go.
const (
	a2aCalleeNS = "agentturn:a2a_callee"
	a2aCancelNS = "agentturn:a2a_cancel"
)

// a2aCallee is the JSON of an [a2aCalleeNS] record.
type a2aCallee struct {
	URL      string `json:"url"`
	Task     string `json:"task"`
	Context  string `json:"context"`
	Relation string `json:"relation"`
}

// a2aCancel is the JSON of an [a2aCancelNS] record.
type a2aCancel struct {
	URL  string `json:"url"`
	Task string `json:"task"`
}

// record writes a record a tool wrote for the call callID, "" for none
// the session holds: a callee record as the call's subsession or peer
// link, a cancel record as the cancel notice naming the link of the
// callee it was sent to, and any other record, or one of those the
// recorder cannot write as a link, as a custom entry in its namespace.
// A record is never dropped: one that does not decode, names a relation
// the format has no link for, names no URL or no call, or a cancel
// whose callee's link is not on the path, is the custom entry it was
// before the recorder knew these namespaces.
func (w *writer) record(ctx context.Context, ns string, raw json.RawMessage, callID string) (string, error) {
	if l := w.calleeLink(ns, raw, callID); l != nil {
		id, err := w.append(ctx, l)
		if err == nil {
			if l.Cancel == "" {
				w.callees = append(w.callees, calleeCopy(l, id))
			}
			return id, nil
		}
		// The store holds a link to the rules a writer is held to,
		// which calleeLink checks first; one it refuses all the same
		// is written as the record it came from.
		if !errors.Is(err, agentsession.ErrCalleeLink) {
			return "", err
		}
	}
	return w.append(ctx, &agentsession.CustomEntry{NS: ns, Data: raw, CallID: callID})
}

// calleeLink is the link entry a callee or cancel record of the call
// callID is written as, or nil for a record that stays a custom entry.
func (w *writer) calleeLink(ns string, raw json.RawMessage, callID string) *agentsession.LinkEntry {
	if callID == "" {
		return nil
	}
	switch ns {
	case a2aCalleeNS:
		var c a2aCallee
		// A callee with no URL is one the tool could not say where it
		// sent to; the format names a callee in another system by it.
		if json.Unmarshal(raw, &c) != nil || c.URL == "" {
			return nil
		}
		callee := agentsession.Callee{URL: c.URL, Task: c.Task, Context: c.Context}
		switch c.Relation {
		case "subsession":
			return agentsession.NewCalleeSubsessionLink(callID, callee)
		case "peer":
			return agentsession.NewPeerLink(callID, callee)
		}
	case a2aCancelNS:
		var c a2aCancel
		if json.Unmarshal(raw, &c) != nil || c.Task == "" {
			return nil
		}
		// The latest link of the call to the task, a call dispatched
		// again after a restart reaching the same task included.
		for i := len(w.callees) - 1; i >= 0; i-- {
			l := w.callees[i]
			if l.CallID == callID && l.Task == c.Task && l.URL == c.URL {
				return agentsession.NewCancelSentLink(l)
			}
		}
	}
	return nil
}

// calleeElsewhere reports whether l names a callee in another system,
// reached over a protocol such as A2A: a subsession or peer link
// carrying a URL, and not a cancel notice. Such a link names no child
// session of this store, even when its Session is set.
func calleeElsewhere(l *agentsession.LinkEntry) bool {
	return (l.Rel == agentsession.RelSubsession || l.Rel == agentsession.RelPeer) && l.URL != "" && l.Cancel == ""
}

// calleeCopy is the callee link l names, with id as its entry ID: what
// the writer keeps of a link it appended, the entry itself being the
// store's once appended, and what a link copied onto another branch is
// built from.
func calleeCopy(l *agentsession.LinkEntry, id string) *agentsession.LinkEntry {
	c := &agentsession.LinkEntry{Rel: l.Rel, Session: l.Session, CallID: l.CallID, URL: l.URL, Task: l.Task, Context: l.Context}
	c.ID = id
	return c
}

// calleesOf returns the callee links in another system on path, in
// order, for a cancel notice written later on it to name.
func calleesOf(path []agentsession.Entry) []*agentsession.LinkEntry {
	var out []*agentsession.LinkEntry
	for _, e := range path {
		if l, ok := e.(*agentsession.LinkEntry); ok && calleeElsewhere(l) {
			out = append(out, l)
		}
	}
	return out
}

// calleesAt returns the callee links in another system of the call
// callID that the path to entry, the call's output on a branch, holds
// for the dispatch the output follows, with whether a cancel notice on
// that path names each: the callees whose work that output is.
func calleesAt(s *agentsession.Session, entry, dispatch, callID string) []agentsession.CalleeLink {
	var out []agentsession.CalleeLink
	for _, c := range agentsession.Callees(s.Path(entry)) {
		if c.CallID == callID && c.URL != "" && c.Dispatch == dispatch {
			out = append(out, c)
		}
	}
	return out
}

// copyCallees writes the callee links of another branch onto this one,
// each whole, and after each one a cancel notice named, a notice of its
// own naming the copy: a notice's target is an entry ID, which the copy
// does not share with the link it copies.
func (w *writer) copyCallees(ctx context.Context, links []agentsession.CalleeLink) error {
	for _, c := range links {
		id, err := w.append(ctx, calleeCopy(c.LinkEntry, ""))
		if err != nil {
			return err
		}
		l := calleeCopy(c.LinkEntry, id)
		w.callees = append(w.callees, l)
		if !c.Cancelled {
			continue
		}
		if _, err := w.append(ctx, agentsession.NewCancelSentLink(l)); err != nil {
			return err
		}
	}
	return nil
}
