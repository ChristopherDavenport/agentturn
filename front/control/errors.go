package control

import (
	"context"
	"errors"
	"slices"

	"github.com/ChristopherDavenport/agentturn"
)

// RemoteError is an error that crossed the wire: its text, and the
// agentturn sentinels it matched where it was raised, so errors.Is
// answers on this side as it did on the other (errors.Is(err,
// agentturn.ErrRunning), errors.Is(err, context.Canceled)). Nothing
// else of the original's chain travels.
type RemoteError struct {
	// Message is the error's text where it was raised.
	Message string
	// Codes name the sentinels it matched; see Is.
	Codes []string
}

func (e *RemoteError) Error() string { return e.Message }

// Is reports whether target is one of the sentinels the error matched
// where it was raised.
func (e *RemoteError) Is(target error) bool {
	for _, s := range sentinels {
		if s.err == target && slices.Contains(e.Codes, s.code) {
			return true
		}
	}
	return false
}

// sentinels are the errors whose identity crosses the wire, by code.
var sentinels = []struct {
	code string
	err  error
}{
	{"running", agentturn.ErrRunning},
	{"input_required", agentturn.ErrInputRequired},
	{"not_pending", agentturn.ErrNotPending},
	{"ambiguous_call", agentturn.ErrAmbiguousCall},
	{"call_answered", agentturn.ErrCallAnswered},
	{"no_question", agentturn.ErrNoQuestion},
	{"no_model", agentturn.ErrNoModel},
	{"cannot_continue", agentturn.ErrCannotContinue},
	{"no_prompt", agentturn.ErrNoPrompt},
	{"guard", agentturn.ErrGuard},
	{"trigger_extra", agentturn.ErrTriggerExtra},
	{"canceled", context.Canceled},
	{"deadline_exceeded", context.DeadlineExceeded},
	{"remote_tool", ErrRemoteTool},
	{"unknown_command", ErrUnknownCommand},
	{"unauthenticated", ErrUnauthenticated},
	{"forbidden", ErrForbidden},
}

type wireError struct {
	Message string   `json:"message"`
	Codes   []string `json:"codes,omitempty"`
}

func errorOut(err error) *wireError {
	if err == nil {
		return nil
	}
	w := &wireError{Message: err.Error()}
	for _, s := range sentinels {
		if errors.Is(err, s.err) {
			w.Codes = append(w.Codes, s.code)
		}
	}
	return w
}

func errorIn(w *wireError) error {
	if w == nil {
		return nil
	}
	return &RemoteError{Message: w.Message, Codes: w.Codes}
}
