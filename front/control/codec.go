package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// MarshalEvent writes an agentturn event in its JSON form: an object
// whose "type" is the event's EventType and whose other members are
// its fields, in snake case. Items, requests, responses and stream
// events are written as openresponses writes them. A field that is not
// data travels as its description: an error as its text (with the
// agentturn sentinels it matches, see [RemoteError]), a tool as its
// name, description, parameters and annotations, a duration as
// time.Duration's text, and a tool result's Details as its JSON, or
// not at all when it has none.
//
// The form is this front's, not the loop's: an event in process is a
// value with live fields, and the choices above are what crossing a
// wire costs. It is exported for a host that writes events somewhere
// else, a log or a queue, and reads them back with [UnmarshalEvent].
func MarshalEvent(ev agentturn.Event) ([]byte, error) {
	w, err := wireOf(ev)
	if err != nil {
		return nil, err
	}
	return json.Marshal(w)
}

// UnmarshalEvent reads an event [MarshalEvent] wrote. The value is the
// pointer type a subscriber in process gets (*agentturn.ItemEnd and so
// on); what travelled as a description comes back as one: an error is
// a *[RemoteError], a tool a *[RemoteTool], a result's Details a
// json.RawMessage. An unknown type is an error.
func UnmarshalEvent(data []byte) (agentturn.Event, error) {
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return nil, fmt.Errorf("control: event: %w", err)
	}
	decode, ok := decoders[head.Type]
	if !ok {
		return nil, fmt.Errorf("control: event: unknown type %q", head.Type)
	}
	ev, err := decode(data)
	if err != nil {
		return nil, fmt.Errorf("control: event %q: %w", head.Type, err)
	}
	return ev, nil
}

// wireOf is ev in its wire form.
func wireOf(ev agentturn.Event) (any, error) {
	switch e := ev.(type) {
	case *agentturn.RunStart:
		return wireRunStart{Type: e.EventType(), RunID: e.RunID, Source: string(e.Source), Trigger: triggerOut(e.Trigger)}, nil
	case *agentturn.TurnStart:
		inputs, err := itemsOut(e.Inputs)
		if err != nil {
			return nil, err
		}
		arrived, err := turnInputsOut(e.Arrived)
		if err != nil {
			return nil, err
		}
		req, err := json.Marshal(e.Request)
		if err != nil {
			return nil, err
		}
		return wireTurnStart{Type: e.EventType(), RunID: e.RunID, Turn: e.Turn, Request: req, Inputs: inputs, Arrived: arrived}, nil
	case *agentturn.ModelRetry:
		req, err := json.Marshal(e.Request)
		if err != nil {
			return nil, err
		}
		return wireModelRetry{Type: e.EventType(), RunID: e.RunID, Turn: e.Turn, Attempt: e.Attempt, Err: errorOut(e.Err), Delay: durationOut(e.Delay), Request: req}, nil
	case *agentturn.ModelBlocked:
		req, err := json.Marshal(e.Request)
		if err != nil {
			return nil, err
		}
		return wireModelBlocked{Type: e.EventType(), RunID: e.RunID, Turn: e.Turn, Request: req, Err: errorOut(e.Err)}, nil
	case *agentturn.ItemStart:
		item, err := itemOut(e.Item)
		if err != nil {
			return nil, err
		}
		return wireItemStart{Type: e.EventType(), RunID: e.RunID, Turn: e.Turn, Item: item, ResponseID: e.ResponseID, Hidden: e.Hidden}, nil
	case *agentturn.ItemUpdate:
		item, err := itemOut(e.Item)
		if err != nil {
			return nil, err
		}
		var stream json.RawMessage
		if e.Stream != nil {
			if stream, err = json.Marshal(e.Stream); err != nil {
				return nil, err
			}
		}
		return wireItemUpdate{Type: e.EventType(), RunID: e.RunID, Turn: e.Turn, Item: item, Stream: stream, ResponseID: e.ResponseID}, nil
	case *agentturn.ItemEnd:
		item, err := itemOut(e.Item)
		if err != nil {
			return nil, err
		}
		return wireItemEnd{Type: e.EventType(), RunID: e.RunID, Turn: e.Turn, Item: item, ResponseID: e.ResponseID, Hidden: e.Hidden, Trigger: triggerOut(e.Trigger), ModelCallID: e.ModelCallID}, nil
	case *agentturn.ResponseEnd:
		resp, err := responseOut(e.Response)
		if err != nil {
			return nil, err
		}
		return wireResponseEnd{Type: e.EventType(), RunID: e.RunID, Turn: e.Turn, Response: resp, Withheld: e.Withheld}, nil
	case *agentturn.ToolStart:
		return wireToolStart{Type: e.EventType(), RunID: e.RunID, Turn: e.Turn, CallID: e.CallID, Name: e.Name, Args: e.Args, Decision: decisionOut(e.Decision), Parent: e.Parent}, nil
	case *agentturn.ToolDispatch:
		return wireToolDispatch{Type: e.EventType(), RunID: e.RunID, Turn: e.Turn, CallID: e.CallID, Name: e.Name, Parent: e.Parent, IdempotencyKey: e.IdempotencyKey}, nil
	case *agentturn.ToolUpdate:
		partial, err := resultOut(e.Partial)
		if err != nil {
			return nil, err
		}
		return wireToolUpdate{Type: e.EventType(), RunID: e.RunID, Turn: e.Turn, CallID: e.CallID, Name: e.Name, Partial: partial}, nil
	case *agentturn.ToolEnd:
		result, err := resultOut(e.Result)
		if err != nil {
			return nil, err
		}
		return wireToolEnd{Type: e.EventType(), RunID: e.RunID, Turn: e.Turn, CallID: e.CallID, Name: e.Name, Result: result, Err: errorOut(e.Err),
			Blocked: e.Blocked, Deferred: e.Deferred, Reason: e.Reason, Parent: e.Parent}, nil
	case *agentturn.TurnEnd:
		resp, err := responseOut(e.Response)
		if err != nil {
			return nil, err
		}
		results := make([]wireResult, 0, len(e.ToolResults))
		for _, r := range e.ToolResults {
			w, err := resultOut(r)
			if err != nil {
				return nil, err
			}
			results = append(results, w)
		}
		return wireTurnEnd{Type: e.EventType(), RunID: e.RunID, Turn: e.Turn, Response: resp, ToolResults: results}, nil
	case *agentturn.RunEnd:
		w, err := runEndOut(e)
		if err != nil {
			return nil, err
		}
		return w, nil
	case *agentturn.Queued:
		item, err := itemOut(e.Item)
		if err != nil {
			return nil, err
		}
		return wireQueued{Type: e.EventType(), RunID: e.RunID, Item: item, Mode: string(e.Mode), Hidden: e.Hidden, Trigger: triggerOut(e.Trigger)}, nil
	case *agentturn.Question:
		return wireQuestion{Type: e.EventType(), ID: e.ID, RunID: e.RunID, CallID: e.CallID, Call: askedCallOut(e.Call), Elicitation: elicitationOut(e.Elicitation)}, nil
	case *agentturn.QuestionClosed:
		return wireQuestionClosed{Type: e.EventType(), ID: e.ID, RunID: e.RunID, Answer: elicitAnswerOut(e.Answer)}, nil
	}
	return nil, fmt.Errorf("control: event %T has no wire form", ev)
}

// decoders read each event type's wire form.
var decoders = map[string]func([]byte) (agentturn.Event, error){
	agentturn.EventRunStart: func(data []byte) (agentturn.Event, error) {
		var w wireRunStart
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return &agentturn.RunStart{RunID: w.RunID, Source: agentturn.Source(w.Source), Trigger: triggerIn(w.Trigger)}, nil
	},
	agentturn.EventTurnStart: func(data []byte) (agentturn.Event, error) {
		var w wireTurnStart
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		e := &agentturn.TurnStart{RunID: w.RunID, Turn: w.Turn}
		var err error
		if e.Request, err = requestIn(w.Request); err != nil {
			return nil, err
		}
		if e.Inputs, err = itemsIn(w.Inputs); err != nil {
			return nil, err
		}
		if e.Arrived, err = turnInputsIn(w.Arrived); err != nil {
			return nil, err
		}
		return e, nil
	},
	agentturn.EventModelRetry: func(data []byte) (agentturn.Event, error) {
		var w wireModelRetry
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		delay, err := durationIn(w.Delay)
		if err != nil {
			return nil, err
		}
		req, err := requestIn(w.Request)
		if err != nil {
			return nil, err
		}
		return &agentturn.ModelRetry{RunID: w.RunID, Turn: w.Turn, Attempt: w.Attempt, Err: errorIn(w.Err), Delay: delay, Request: req}, nil
	},
	agentturn.EventModelBlocked: func(data []byte) (agentturn.Event, error) {
		var w wireModelBlocked
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		req, err := requestIn(w.Request)
		if err != nil {
			return nil, err
		}
		return &agentturn.ModelBlocked{RunID: w.RunID, Turn: w.Turn, Request: req, Err: errorIn(w.Err)}, nil
	},
	agentturn.EventItemStart: func(data []byte) (agentturn.Event, error) {
		var w wireItemStart
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		item, err := itemIn(w.Item)
		if err != nil {
			return nil, err
		}
		return &agentturn.ItemStart{RunID: w.RunID, Turn: w.Turn, Item: item, ResponseID: w.ResponseID, Hidden: w.Hidden}, nil
	},
	agentturn.EventItemUpdate: func(data []byte) (agentturn.Event, error) {
		var w wireItemUpdate
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		item, err := itemIn(w.Item)
		if err != nil {
			return nil, err
		}
		e := &agentturn.ItemUpdate{RunID: w.RunID, Turn: w.Turn, Item: item, ResponseID: w.ResponseID}
		if !isNull(w.Stream) {
			if e.Stream, err = openresponses.DecodeEvent(w.Stream); err != nil {
				return nil, err
			}
		}
		return e, nil
	},
	agentturn.EventItemEnd: func(data []byte) (agentturn.Event, error) {
		var w wireItemEnd
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		item, err := itemIn(w.Item)
		if err != nil {
			return nil, err
		}
		return &agentturn.ItemEnd{RunID: w.RunID, Turn: w.Turn, Item: item, ResponseID: w.ResponseID, Hidden: w.Hidden, Trigger: triggerIn(w.Trigger), ModelCallID: w.ModelCallID}, nil
	},
	agentturn.EventResponseEnd: func(data []byte) (agentturn.Event, error) {
		var w wireResponseEnd
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		resp, err := responseIn(w.Response)
		if err != nil {
			return nil, err
		}
		return &agentturn.ResponseEnd{RunID: w.RunID, Turn: w.Turn, Response: resp, Withheld: w.Withheld}, nil
	},
	agentturn.EventToolStart: func(data []byte) (agentturn.Event, error) {
		var w wireToolStart
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		d, err := decisionIn(w.Decision)
		if err != nil {
			return nil, err
		}
		return &agentturn.ToolStart{RunID: w.RunID, Turn: w.Turn, CallID: w.CallID, Name: w.Name, Args: w.Args, Decision: d, Parent: w.Parent}, nil
	},
	agentturn.EventToolDispatch: func(data []byte) (agentturn.Event, error) {
		var w wireToolDispatch
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return &agentturn.ToolDispatch{RunID: w.RunID, Turn: w.Turn, CallID: w.CallID, Name: w.Name, Parent: w.Parent, IdempotencyKey: w.IdempotencyKey}, nil
	},
	agentturn.EventToolUpdate: func(data []byte) (agentturn.Event, error) {
		var w wireToolUpdate
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return &agentturn.ToolUpdate{RunID: w.RunID, Turn: w.Turn, CallID: w.CallID, Name: w.Name, Partial: resultIn(w.Partial)}, nil
	},
	agentturn.EventToolEnd: func(data []byte) (agentturn.Event, error) {
		var w wireToolEnd
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return &agentturn.ToolEnd{RunID: w.RunID, Turn: w.Turn, CallID: w.CallID, Name: w.Name, Result: resultIn(w.Result), Err: errorIn(w.Err),
			Blocked: w.Blocked, Deferred: w.Deferred, Reason: w.Reason, Parent: w.Parent}, nil
	},
	agentturn.EventTurnEnd: func(data []byte) (agentturn.Event, error) {
		var w wireTurnEnd
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		resp, err := responseIn(w.Response)
		if err != nil {
			return nil, err
		}
		e := &agentturn.TurnEnd{RunID: w.RunID, Turn: w.Turn, Response: resp}
		for _, r := range w.ToolResults {
			e.ToolResults = append(e.ToolResults, resultIn(r))
		}
		return e, nil
	},
	agentturn.EventRunEnd: func(data []byte) (agentturn.Event, error) {
		var w wireRunEnd
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return runEndIn(w)
	},
	agentturn.EventQueued: func(data []byte) (agentturn.Event, error) {
		var w wireQueued
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		item, err := itemIn(w.Item)
		if err != nil {
			return nil, err
		}
		return &agentturn.Queued{RunID: w.RunID, Item: item, Mode: agentturn.QueueMode(w.Mode), Hidden: w.Hidden, Trigger: triggerIn(w.Trigger)}, nil
	},
	agentturn.EventQuestion: func(data []byte) (agentturn.Event, error) {
		var w wireQuestion
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		call, err := askedCallIn(w.Call)
		if err != nil {
			return nil, err
		}
		return &agentturn.Question{ID: w.ID, RunID: w.RunID, CallID: w.CallID, Call: call, Elicitation: elicitationIn(w.Elicitation)}, nil
	},
	agentturn.EventQuestionClosed: func(data []byte) (agentturn.Event, error) {
		var w wireQuestionClosed
		if err := json.Unmarshal(data, &w); err != nil {
			return nil, err
		}
		return &agentturn.QuestionClosed{ID: w.ID, RunID: w.RunID, Answer: elicitAnswerIn(w.Answer)}, nil
	},
}

// The wire forms of the events.

type wireRunStart struct {
	Type    string       `json:"type"`
	RunID   string       `json:"run_id"`
	Source  string       `json:"source,omitempty"`
	Trigger *wireTrigger `json:"trigger,omitempty"`
}

type wireTurnStart struct {
	Type    string          `json:"type"`
	RunID   string          `json:"run_id"`
	Turn    int             `json:"turn"`
	Request json.RawMessage `json:"request"`
	Inputs  json.RawMessage `json:"inputs,omitempty"`
	Arrived []wireTurnInput `json:"arrived,omitempty"`
}

type wireModelRetry struct {
	Type    string          `json:"type"`
	RunID   string          `json:"run_id"`
	Turn    int             `json:"turn"`
	Attempt int             `json:"attempt"`
	Err     *wireError      `json:"error,omitempty"`
	Delay   string          `json:"delay,omitempty"`
	Request json.RawMessage `json:"request"`
}

type wireModelBlocked struct {
	Type    string          `json:"type"`
	RunID   string          `json:"run_id"`
	Turn    int             `json:"turn"`
	Request json.RawMessage `json:"request"`
	Err     *wireError      `json:"error,omitempty"`
}

type wireItemStart struct {
	Type       string          `json:"type"`
	RunID      string          `json:"run_id"`
	Turn       int             `json:"turn"`
	Item       json.RawMessage `json:"item"`
	ResponseID string          `json:"response_id,omitempty"`
	Hidden     bool            `json:"hidden,omitempty"`
}

type wireItemUpdate struct {
	Type       string          `json:"type"`
	RunID      string          `json:"run_id"`
	Turn       int             `json:"turn"`
	Item       json.RawMessage `json:"item"`
	Stream     json.RawMessage `json:"stream,omitempty"`
	ResponseID string          `json:"response_id,omitempty"`
}

type wireItemEnd struct {
	Type        string          `json:"type"`
	RunID       string          `json:"run_id"`
	Turn        int             `json:"turn"`
	Item        json.RawMessage `json:"item"`
	ResponseID  string          `json:"response_id,omitempty"`
	Hidden      bool            `json:"hidden,omitempty"`
	Trigger     *wireTrigger    `json:"trigger,omitempty"`
	ModelCallID string          `json:"model_call_id,omitempty"`
}

type wireResponseEnd struct {
	Type     string          `json:"type"`
	RunID    string          `json:"run_id"`
	Turn     int             `json:"turn"`
	Response json.RawMessage `json:"response,omitempty"`
	Withheld bool            `json:"withheld,omitempty"`
}

type wireToolStart struct {
	Type     string          `json:"type"`
	RunID    string          `json:"run_id"`
	Turn     int             `json:"turn"`
	CallID   string          `json:"call_id"`
	Name     string          `json:"name"`
	Args     json.RawMessage `json:"args,omitempty"`
	Decision *wireDecision   `json:"decision,omitempty"`
	Parent   string          `json:"parent,omitempty"`
}

type wireToolDispatch struct {
	Type           string `json:"type"`
	RunID          string `json:"run_id"`
	Turn           int    `json:"turn"`
	CallID         string `json:"call_id"`
	Name           string `json:"name"`
	Parent         string `json:"parent,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

type wireToolUpdate struct {
	Type    string     `json:"type"`
	RunID   string     `json:"run_id"`
	Turn    int        `json:"turn"`
	CallID  string     `json:"call_id"`
	Name    string     `json:"name"`
	Partial wireResult `json:"partial"`
}

type wireToolEnd struct {
	Type     string     `json:"type"`
	RunID    string     `json:"run_id"`
	Turn     int        `json:"turn"`
	CallID   string     `json:"call_id"`
	Name     string     `json:"name"`
	Result   wireResult `json:"result"`
	Err      *wireError `json:"error,omitempty"`
	Blocked  bool       `json:"blocked,omitempty"`
	Deferred bool       `json:"deferred,omitempty"`
	Reason   string     `json:"reason,omitempty"`
	Parent   string     `json:"parent,omitempty"`
}

type wireTurnEnd struct {
	Type        string          `json:"type"`
	RunID       string          `json:"run_id"`
	Turn        int             `json:"turn"`
	Response    json.RawMessage `json:"response,omitempty"`
	ToolResults []wireResult    `json:"tool_results,omitempty"`
}

type wireRunEnd struct {
	Type     string          `json:"type"`
	RunID    string          `json:"run_id"`
	Items    json.RawMessage `json:"items,omitempty"`
	Reason   string          `json:"reason"`
	Cause    string          `json:"cause,omitempty"`
	Err      *wireError      `json:"error,omitempty"`
	Pending  []wirePending   `json:"pending,omitempty"`
	Withheld bool            `json:"withheld,omitempty"`
}

type wireQueued struct {
	Type    string          `json:"type"`
	RunID   string          `json:"run_id,omitempty"`
	Item    json.RawMessage `json:"item"`
	Mode    string          `json:"mode"`
	Hidden  bool            `json:"hidden,omitempty"`
	Trigger *wireTrigger    `json:"trigger,omitempty"`
}

type wireQuestion struct {
	Type        string          `json:"type"`
	ID          string          `json:"id"`
	RunID       string          `json:"run_id,omitempty"`
	CallID      string          `json:"call_id,omitempty"`
	Call        *wireAskedCall  `json:"call,omitempty"`
	Elicitation wireElicitation `json:"elicitation"`
}

type wireQuestionClosed struct {
	Type   string            `json:"type"`
	ID     string            `json:"id"`
	RunID  string            `json:"run_id,omitempty"`
	Answer *wireElicitAnswer `json:"answer,omitempty"`
}

// The wire forms of the values the events and the methods carry.

type wireTrigger struct {
	Kind   string         `json:"kind,omitempty"`
	Ref    string         `json:"ref,omitempty"`
	Source string         `json:"source,omitempty"`
	Extra  map[string]any `json:"extra,omitempty"`
}

func triggerOut(t agentturn.Trigger) *wireTrigger {
	if t.IsZero() {
		return nil
	}
	return &wireTrigger{Kind: t.Kind, Ref: t.Ref, Source: t.Source, Extra: t.Extra}
}

func triggerIn(w *wireTrigger) agentturn.Trigger {
	if w == nil {
		return agentturn.Trigger{}
	}
	return agentturn.Trigger{Kind: w.Kind, Ref: w.Ref, Source: w.Source, Extra: w.Extra}
}

type wireTurnInput struct {
	Item    json.RawMessage `json:"item"`
	Mode    string          `json:"mode"`
	Trigger *wireTrigger    `json:"trigger,omitempty"`
}

func turnInputsOut(in []agentturn.TurnInput) ([]wireTurnInput, error) {
	var out []wireTurnInput
	for _, t := range in {
		item, err := itemOut(t.Item)
		if err != nil {
			return nil, err
		}
		out = append(out, wireTurnInput{Item: item, Mode: string(t.Mode), Trigger: triggerOut(t.Trigger)})
	}
	return out, nil
}

func turnInputsIn(in []wireTurnInput) ([]agentturn.TurnInput, error) {
	var out []agentturn.TurnInput
	for _, w := range in {
		item, err := itemIn(w.Item)
		if err != nil {
			return nil, err
		}
		out = append(out, agentturn.TurnInput{Item: item, Mode: agentturn.InputMode(w.Mode), Trigger: triggerIn(w.Trigger)})
	}
	return out, nil
}

// wireResult is an agenttool.Result. Details travels as its JSON, and
// is left out when it has none or cannot be written.
type wireResult struct {
	Output    openresponses.FunctionCallOutputData `json:"output"`
	Details   json.RawMessage                      `json:"details,omitempty"`
	Terminate bool                                 `json:"terminate,omitempty"`
}

func resultOut(r agenttool.Result) (wireResult, error) {
	w := wireResult{Output: r.Output, Terminate: r.Terminate}
	if r.Details != nil {
		if d, err := json.Marshal(r.Details); err == nil {
			w.Details = d
		}
	}
	return w, nil
}

func resultIn(w wireResult) agenttool.Result {
	r := agenttool.Result{Output: w.Output, Terminate: w.Terminate}
	if !isNull(w.Details) {
		r.Details = w.Details
	}
	return r
}

type wireDecision struct {
	Action    string          `json:"action"`
	Reason    string          `json:"reason,omitempty"`
	Terminate bool            `json:"terminate,omitempty"`
	Args      json.RawMessage `json:"args,omitempty"`
	By        string          `json:"by,omitempty"`
	Note      string          `json:"note,omitempty"`
	Held      bool            `json:"held,omitempty"`
	Subject   string          `json:"subject,omitempty"`
}

var actionNames = map[agentturn.ToolAction]string{agentturn.Allow: "allow", agentturn.Block: "block", agentturn.Defer: "defer"}

func decisionOut(d *agentturn.ToolDecision) *wireDecision {
	if d == nil {
		return nil
	}
	action, ok := actionNames[d.Action]
	if !ok {
		action = fmt.Sprintf("action(%d)", d.Action)
	}
	return &wireDecision{Action: action, Reason: d.Reason, Terminate: d.Terminate, Args: d.Args, By: d.By, Note: d.Note, Held: d.Held, Subject: d.Subject}
}

func decisionIn(w *wireDecision) (*agentturn.ToolDecision, error) {
	if w == nil {
		return nil, nil
	}
	d := &agentturn.ToolDecision{Reason: w.Reason, Terminate: w.Terminate, Args: w.Args, By: w.By, Note: w.Note, Held: w.Held, Subject: w.Subject}
	found := false
	for a, name := range actionNames {
		if name == w.Action {
			d.Action, found = a, true
		}
	}
	if !found {
		return nil, fmt.Errorf("decision: unknown action %q", w.Action)
	}
	return d, nil
}

type wirePending struct {
	Call           json.RawMessage `json:"call"`
	Reason         string          `json:"reason"`
	Tool           *wireTool       `json:"tool,omitempty"`
	Dispatched     bool            `json:"dispatched,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Args           json.RawMessage `json:"args,omitempty"`
	Ran            json.RawMessage `json:"ran,omitempty"`
	RanWhere       string          `json:"ran_where,omitempty"`
	Refused        string          `json:"refused,omitempty"`
	Decision       *wireDecision   `json:"decision,omitempty"`
}

func pendingOut(in []agentturn.PendingCall) ([]wirePending, error) {
	var out []wirePending
	for _, p := range in {
		w := wirePending{Reason: string(p.Reason), Tool: toolOut(p.Tool), Dispatched: p.Dispatched, IdempotencyKey: p.IdempotencyKey,
			Args: p.Args, RanWhere: p.RanWhere, Refused: p.Refused, Decision: decisionOut(p.Decision)}
		var err error
		if p.Call != nil {
			if w.Call, err = json.Marshal(p.Call); err != nil {
				return nil, err
			}
		}
		if p.Ran != nil {
			if w.Ran, err = json.Marshal(p.Ran); err != nil {
				return nil, err
			}
		}
		out = append(out, w)
	}
	return out, nil
}

func pendingIn(in []wirePending) ([]agentturn.PendingCall, error) {
	var out []agentturn.PendingCall
	for _, w := range in {
		p := agentturn.PendingCall{Reason: agentturn.PendingReason(w.Reason), Dispatched: w.Dispatched, IdempotencyKey: w.IdempotencyKey,
			Args: w.Args, RanWhere: w.RanWhere, Refused: w.Refused}
		if w.Tool != nil {
			p.Tool = w.Tool.tool()
		}
		var err error
		if p.Call, err = functionCallIn(w.Call); err != nil {
			return nil, err
		}
		if p.Ran, err = functionCallOutputIn(w.Ran); err != nil {
			return nil, err
		}
		if p.Decision, err = decisionIn(w.Decision); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// wireTool is a tool as its description: what a prompt about a pending
// call shows of it.
type wireTool struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Parameters  json.RawMessage        `json:"parameters,omitempty"`
	Annotations *agenttool.Annotations `json:"annotations,omitempty"`
}

func toolOut(t agenttool.Tool) *wireTool {
	if t == nil {
		return nil
	}
	w := &wireTool{Name: t.Name(), Description: t.Description(), Parameters: t.Parameters()}
	if a := agenttool.AnnotationsOf(t); a != (agenttool.Annotations{}) {
		w.Annotations = &a
	}
	return w
}

func (w *wireTool) tool() *RemoteTool {
	t := &RemoteTool{name: w.Name, description: w.Description, parameters: w.Parameters}
	if w.Annotations != nil {
		t.annotations = *w.Annotations
	}
	return t
}

// RemoteTool is a tool as a pending call names it across the wire: its
// name, description, parameters and annotations, so a front can show
// what a call asks to run and how the tool describes itself. It runs
// nowhere: the tool is where the agent is, and Execute fails.
type RemoteTool struct {
	name, description string
	parameters        json.RawMessage
	annotations       agenttool.Annotations
}

var _ agenttool.Tool = (*RemoteTool)(nil)

// Name implements agenttool.Tool.
func (t *RemoteTool) Name() string { return t.name }

// Description implements agenttool.Tool.
func (t *RemoteTool) Description() string { return t.description }

// Parameters implements agenttool.Tool.
func (t *RemoteTool) Parameters() json.RawMessage { return t.parameters }

// Annotations are the tool's, as agenttool.AnnotationsOf read them
// where the agent is.
func (t *RemoteTool) Annotations() agenttool.Annotations { return t.annotations }

// ErrRemoteTool is what a RemoteTool's Execute returns.
var ErrRemoteTool = errors.New("control: the tool runs where the agent is, not here")

// Execute implements agenttool.Tool, and fails with [ErrRemoteTool].
func (t *RemoteTool) Execute(context.Context, agenttool.Call) (agenttool.Result, error) {
	return agenttool.Result{}, ErrRemoteTool
}

type wireAskedCall struct {
	Parent   string          `json:"parent,omitempty"`
	CallID   string          `json:"call_id"`
	Name     string          `json:"name"`
	Args     json.RawMessage `json:"args,omitempty"`
	Decision *wireDecision   `json:"decision,omitempty"`
}

func askedCallOut(c *agentturn.AskedCall) *wireAskedCall {
	if c == nil {
		return nil
	}
	return &wireAskedCall{Parent: c.Parent, CallID: c.CallID, Name: c.Name, Args: c.Args, Decision: decisionOut(c.Decision)}
}

func askedCallIn(w *wireAskedCall) (*agentturn.AskedCall, error) {
	if w == nil {
		return nil, nil
	}
	d, err := decisionIn(w.Decision)
	if err != nil {
		return nil, err
	}
	return &agentturn.AskedCall{Parent: w.Parent, CallID: w.CallID, Name: w.Name, Args: w.Args, Decision: d}, nil
}

type wireElicitation struct {
	Message string          `json:"message"`
	Schema  json.RawMessage `json:"schema,omitempty"`
	URL     string          `json:"url,omitempty"`
}

func elicitationOut(q agenttool.Elicitation) wireElicitation {
	return wireElicitation{Message: q.Message, Schema: q.Schema, URL: q.URL}
}

func elicitationIn(w wireElicitation) agenttool.Elicitation {
	return agenttool.Elicitation{Message: w.Message, Schema: w.Schema, URL: w.URL}
}

type wireElicitAnswer struct {
	Action  string          `json:"action"`
	Content json.RawMessage `json:"content,omitempty"`
	Note    string          `json:"note,omitempty"`
}

func elicitAnswerOut(a *agenttool.Answer) *wireElicitAnswer {
	if a == nil {
		return nil
	}
	return &wireElicitAnswer{Action: string(a.Action), Content: a.Content, Note: a.Note}
}

func elicitAnswerIn(w *wireElicitAnswer) *agenttool.Answer {
	if w == nil {
		return nil
	}
	return &agenttool.Answer{Action: agenttool.Action(w.Action), Content: w.Content, Note: w.Note}
}

// wireAnswer is an agentturn.Answer, which Resume takes.
type wireAnswer struct {
	CallID         string          `json:"call_id"`
	Output         json.RawMessage `json:"output,omitempty"`
	Args           json.RawMessage `json:"args,omitempty"`
	Note           string          `json:"note,omitempty"`
	Terminate      bool            `json:"terminate,omitempty"`
	By             string          `json:"by,omitempty"`
	Reason         string          `json:"reason,omitempty"`
	Origin         string          `json:"origin,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	RunAgain       bool            `json:"run_again,omitempty"`
}

func answersOut(in []agentturn.Answer) ([]wireAnswer, error) {
	out := make([]wireAnswer, 0, len(in))
	for _, a := range in {
		w := wireAnswer{CallID: a.CallID, Args: a.Args, Note: a.Note, Terminate: a.Terminate, By: a.By, Reason: a.Reason,
			Origin: a.Origin, IdempotencyKey: a.IdempotencyKey, RunAgain: a.RunAgain}
		if a.Output != nil {
			var err error
			if w.Output, err = json.Marshal(a.Output); err != nil {
				return nil, err
			}
		}
		out = append(out, w)
	}
	return out, nil
}

func answersIn(in []wireAnswer) ([]agentturn.Answer, error) {
	out := make([]agentturn.Answer, 0, len(in))
	for _, w := range in {
		a := agentturn.Answer{CallID: w.CallID, Args: w.Args, Note: w.Note, Terminate: w.Terminate, By: w.By, Reason: w.Reason,
			Origin: w.Origin, IdempotencyKey: w.IdempotencyKey, RunAgain: w.RunAgain}
		var err error
		if a.Output, err = functionCallOutputIn(w.Output); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

func runEndOut(e *agentturn.RunEnd) (wireRunEnd, error) {
	items, err := itemsOut(e.Items)
	if err != nil {
		return wireRunEnd{}, err
	}
	pending, err := pendingOut(e.Pending)
	if err != nil {
		return wireRunEnd{}, err
	}
	return wireRunEnd{Type: e.EventType(), RunID: e.RunID, Items: items, Reason: string(e.Reason), Cause: string(e.Cause),
		Err: errorOut(e.Err), Pending: pending, Withheld: e.Withheld}, nil
}

func runEndIn(w wireRunEnd) (*agentturn.RunEnd, error) {
	e := &agentturn.RunEnd{RunID: w.RunID, Reason: agentturn.Reason(w.Reason), Cause: agentturn.StopCause(w.Cause), Err: errorIn(w.Err), Withheld: w.Withheld}
	var err error
	if e.Items, err = itemsIn(w.Items); err != nil {
		return nil, err
	}
	if e.Pending, err = pendingIn(w.Pending); err != nil {
		return nil, err
	}
	return e, nil
}

type wireState struct {
	Transcript json.RawMessage `json:"transcript,omitempty"`
	Running    bool            `json:"running,omitempty"`
	RunID      string          `json:"run_id,omitempty"`
	Turn       int             `json:"turn,omitempty"`
	Steering   int             `json:"steering,omitempty"`
	FollowUps  int             `json:"follow_ups,omitempty"`
	Steered    json.RawMessage `json:"steered,omitempty"`
	Queued     json.RawMessage `json:"queued,omitempty"`
	Pending    []wirePending   `json:"pending,omitempty"`
}

func stateOut(s agentturn.State) (wireState, error) {
	w := wireState{Running: s.Running, RunID: s.RunID, Turn: s.Turn, Steering: s.Steering, FollowUps: s.FollowUps}
	var err error
	if w.Transcript, err = itemsOut(s.Transcript); err != nil {
		return w, err
	}
	if w.Steered, err = itemsOut(s.Steered); err != nil {
		return w, err
	}
	if w.Queued, err = itemsOut(s.Queued); err != nil {
		return w, err
	}
	w.Pending, err = pendingOut(s.Pending)
	return w, err
}

func stateIn(w wireState) (agentturn.State, error) {
	s := agentturn.State{Running: w.Running, RunID: w.RunID, Turn: w.Turn, Steering: w.Steering, FollowUps: w.FollowUps}
	var err error
	if s.Transcript, err = itemsIn(w.Transcript); err != nil {
		return s, err
	}
	if s.Steered, err = itemsIn(w.Steered); err != nil {
		return s, err
	}
	if s.Queued, err = itemsIn(w.Queued); err != nil {
		return s, err
	}
	s.Pending, err = pendingIn(w.Pending)
	return s, err
}

// Items, requests and responses, as openresponses writes them.

func isNull(raw json.RawMessage) bool { return len(raw) == 0 || string(raw) == "null" }

func itemOut(item openresponses.Item) (json.RawMessage, error) {
	if item == nil {
		return nil, nil
	}
	return json.Marshal(item)
}

func itemIn(raw json.RawMessage) (openresponses.Item, error) {
	if isNull(raw) {
		return nil, nil
	}
	return openresponses.UnmarshalItem(raw)
}

func itemsOut(items openresponses.Items) (json.RawMessage, error) {
	if items == nil {
		return nil, nil
	}
	for i, item := range items {
		if item == nil {
			return nil, fmt.Errorf("items: item %d is nil", i)
		}
	}
	return json.Marshal(items)
}

func itemsIn(raw json.RawMessage) (openresponses.Items, error) {
	if isNull(raw) {
		return nil, nil
	}
	var items openresponses.Items
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, err
	}
	return items, nil
}

func functionCallIn(raw json.RawMessage) (*openresponses.FunctionCall, error) {
	item, err := itemIn(raw)
	if err != nil || item == nil {
		return nil, err
	}
	call, ok := item.(*openresponses.FunctionCall)
	if !ok {
		return nil, fmt.Errorf("want a function_call, got %q", item.ItemType())
	}
	return call, nil
}

func functionCallOutputIn(raw json.RawMessage) (*openresponses.FunctionCallOutput, error) {
	item, err := itemIn(raw)
	if err != nil || item == nil {
		return nil, err
	}
	out, ok := item.(*openresponses.FunctionCallOutput)
	if !ok {
		return nil, fmt.Errorf("want a function_call_output, got %q", item.ItemType())
	}
	return out, nil
}

func requestIn(raw json.RawMessage) (openresponses.Request, error) {
	var req openresponses.Request
	if isNull(raw) {
		return req, nil
	}
	err := json.Unmarshal(raw, &req)
	return req, err
}

func responseOut(r *openresponses.Response) (json.RawMessage, error) {
	if r == nil {
		return nil, nil
	}
	return json.Marshal(r)
}

func responseIn(raw json.RawMessage) (*openresponses.Response, error) {
	if isNull(raw) {
		return nil, nil
	}
	var r openresponses.Response
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func durationOut(d time.Duration) string {
	if d == 0 {
		return ""
	}
	return d.String()
}

func durationIn(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}
