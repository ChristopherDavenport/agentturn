package session

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"

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
// own says the record was written by the call callID itself, not by a
// call its tool made through agentturn.Invoke, whose callee is that
// nested call's: a link would read as the callee having answered the
// call the session holds, which the record does not say. A record is
// never dropped: one that does not decode, carries a member the link
// has no place for, names a relation the format has no link for, names
// no URL or no call, or was a nested call's, and a cancel whose
// callee's link is not on the path, is the custom entry it was before
// the recorder knew these namespaces.
func (w *writer) record(ctx context.Context, ns string, raw json.RawMessage, callID string, own bool) (string, error) {
	var l *agentsession.LinkEntry
	if own {
		l = w.calleeLink(ns, raw, callID)
	}
	if l != nil {
		id, err := w.append(ctx, l)
		if err == nil {
			w.noteCallee(l, id)
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
		if decodeStrict(raw, &c) != nil || c.URL == "" {
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
		if decodeStrict(raw, &c) != nil || c.Task == "" {
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

// decodeStrict decodes raw, a single JSON value, into v, refusing a
// member v has no field for: a record whose JSON says more than the
// link can carry, from a tools/a2a newer than this recorder, stays the
// custom entry that keeps all of it.
func decodeStrict(raw json.RawMessage, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("session: trailing data after the record")
	}
	return nil
}

// noteCallee keeps what the writer must know of a callee link or a
// cancel notice it appended as id: the link, for a later notice to
// name, or that the link the notice names was asked to stop.
func (w *writer) noteCallee(l *agentsession.LinkEntry, id string) {
	if l.Cancel != "" {
		w.cancelled[l.Target] = true
		return
	}
	w.callees = append(w.callees, calleeCopy(l, id))
}

// calleeElsewhere reports whether l names a callee in another system,
// reached over a protocol such as A2A: a subsession or peer link
// carrying a URL, and not a cancel notice. Such a link names no child
// session of this store, even when its Session is set.
func calleeElsewhere(l *agentsession.LinkEntry) bool {
	return (l.Rel == agentsession.RelSubsession || l.Rel == agentsession.RelPeer) && l.URL != "" && l.Cancel == ""
}

// calleeCopy is the callee link l names, with id as its entry ID, the
// members the library does not define included: what the writer keeps
// of a link it appended, the entry itself being the store's once
// appended, and what a link copied onto another branch is built from.
// A target or cancel member, which a callee link does not carry, is
// left behind, and so is a redacted member: the copy is appended
// natively, and l may be a redacted session's entry, whose member
// agentsession keeps in Unknown when it was read under a minor before
// 0.13 or is in a form the format does not define.
func calleeCopy(l *agentsession.LinkEntry, id string) *agentsession.LinkEntry {
	c := &agentsession.LinkEntry{Rel: l.Rel, Session: l.Session, CallID: l.CallID, URL: l.URL, Task: l.Task, Context: l.Context}
	c.ID = id
	if len(l.Unknown) > 0 {
		c.Unknown = maps.Clone(l.Unknown)
		delete(c.Unknown, "target")
		delete(c.Unknown, "cancel")
		_ = agentsession.ClearRedacted(c) // fails only on a nil entry
	}
	return c
}

// sameCallee reports whether a and b name the same callee of the same
// call: a copy of the other.
func sameCallee(a, b *agentsession.LinkEntry) bool {
	return a.Rel == b.Rel && a.Session == b.Session && a.CallID == b.CallID && a.URL == b.URL && a.Task == b.Task && a.Context == b.Context
}

// calleesOf returns the callee links in another system on path, in
// order, for a cancel notice written later on it to name, and the IDs
// of those a cancel notice on path names.
func calleesOf(path []agentsession.Entry) ([]*agentsession.LinkEntry, map[string]bool) {
	var out []*agentsession.LinkEntry
	cancelled := map[string]bool{}
	for _, e := range path {
		l, ok := e.(*agentsession.LinkEntry)
		switch {
		case !ok:
		case calleeElsewhere(l):
			out = append(out, l)
		case l.Cancel != "" && l.Target != "":
			cancelled[l.Target] = true
		}
	}
	return out, cancelled
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

// copyCallees writes the callee links of the call callID that another
// branch holds onto this one, each whole, and after each one a cancel
// notice named, a notice of its own naming the copy: a notice's target
// is an entry ID, which the copy does not share with the link it
// copies. It writes only what this path lacks, so a copy an earlier
// attempt cut short, by a failed append or a crash before the answer,
// is finished rather than written twice: the call has no dispatch on
// this path, so a callee link of it here is such a copy, and is kept,
// with the notice it is owed written if it has none.
func (w *writer) copyCallees(ctx context.Context, callID string, links []agentsession.CalleeLink) error {
	used := map[*agentsession.LinkEntry]bool{}
	for _, c := range links {
		var l *agentsession.LinkEntry
		for _, have := range w.callees {
			if have.CallID == callID && !used[have] && sameCallee(have, c.LinkEntry) {
				l = have
				break
			}
		}
		if l == nil {
			cp := calleeCopy(c.LinkEntry, "")
			id, err := w.append(ctx, cp)
			if err != nil {
				return err
			}
			w.noteCallee(cp, id)
			l = w.callees[len(w.callees)-1]
		}
		used[l] = true
		if !c.Cancelled || w.cancelled[l.ID] {
			continue
		}
		n := agentsession.NewCancelSentLink(l)
		id, err := w.append(ctx, n)
		if err != nil {
			return err
		}
		w.noteCallee(n, id)
	}
	return nil
}
