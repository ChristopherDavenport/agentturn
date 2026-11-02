package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// MoveTo rewinds a recorded agent to entryID as one operation (#220): it
// moves the record's leaf there, as [Recorder.Rebase] does, and replaces
// a's transcript, pending calls, owed notes and reasoning attribution
// with what the record holds there, as [AgentOptions] reads them for a
// new agent, reserving every call ID the session holds. r must be the
// recorder that writes a's runs. The empty entry ID is the reset Rebase
// describes, which leaves a with an empty transcript.
//
// It is refused with agentturn.ErrRunning, with nothing changed, while
// a run of a is active, and no run of a starts until it returns: the
// check and the move are one step with a run's start, through
// agentturn.Agent.WhileIdle. A failure at any step leaves a and the
// record as they were: the agent is changed last, once everything it
// takes has been read, and a failure after the record moved puts the
// record back where it was, the error saying so only when that fails
// too.
func MoveTo(ctx context.Context, a *agentturn.Agent, r *Recorder, entryID string) error {
	return a.WhileIdle(func() error {
		s, err := r.store.Open(ctx, r.SessionID())
		if err != nil {
			return fmt.Errorf("session: move: %w", err)
		}
		from := s.Leaf()
		back := func(cause error) error {
			// The record is put back only when it moved: a rebase
			// refused before it changed anything leaves it where it was.
			now, err := r.store.Open(ctx, r.SessionID())
			if err == nil && now.Leaf() == from {
				return cause
			}
			if err == nil {
				err = r.Rebase(now, from)
			}
			if err != nil {
				return errors.Join(cause, fmt.Errorf("session: move: putting the record back at %q failed: %w", from, err))
			}
			return cause
		}
		if err := r.Rebase(s, entryID); err != nil {
			return back(fmt.Errorf("session: move: %w", err))
		}
		if s, err = r.store.Open(ctx, r.SessionID()); err != nil {
			return back(fmt.Errorf("session: move: %w", err))
		}
		items, models, pending, owed, ids, err := agentState(s, r.ReadOptions())
		if err != nil {
			return back(fmt.Errorf("session: move: %w", err))
		}
		// Nothing below fails while the agent is held: each setter
		// refuses only a running agent.
		for _, step := range []func() error{
			func() error { return a.SetTranscript(items) },
			func() error { return a.SetReasoningModels(models) },
			func() error { return a.SetPending(pending) },
			func() error { return a.SetNotes(owed) },
			func() error { return a.ReserveCallIDs(ids...) },
		} {
			if err := step(); err != nil {
				return fmt.Errorf("session: move: %w", err)
			}
		}
		return nil
	})
}

// agentState reads what an agent continuing s at its leaf holds, as
// [AgentOptions] does.
func agentState(s *agentsession.Session, opts []ReadOption) (items openresponses.Items, models agentturn.ReasoningModels, pending []agentturn.PendingCall, owed []agentturn.Note, ids []string, err error) {
	if items, models, err = TranscriptModels(s); err != nil {
		return
	}
	if pending, err = Pending(s, opts...); err != nil {
		return
	}
	if ids, err = CallIDs(s); err != nil {
		return
	}
	owed, err = OwedNotes(s)
	return
}
