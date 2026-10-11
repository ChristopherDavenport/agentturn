package agentturn

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// Control is how the human or autonomous plane drives an agent: a
// person at a client, or a controller that answers by rule. It is the
// third contract a turn meets, beside [Model] for inference and the
// tool contract for execution, and it names the loop's own surface:
// prompt, answer the calls a run left pending, queue input into a run,
// abort, read the state, follow the events, and answer the questions a
// running call asks. *Agent is its in-process implementation; a front
// that serves it over a wire, and a host that puts its own bookkeeping
// around an agent (a policy engine's release of the calls it held, a
// recorder on every run's context), implement it too.
//
// The events a subscriber gets are the agent's own, unnarrowed: what a
// client needs that the loop knows is on them, and a question asked
// while a call runs is one of them ([Question]). A subscriber observes:
// an error it returns is a failure, such as a recorder that could not
// write, and ends the run with [ReasonError], except on a nested call's
// tool_dispatch, whose failure goes to the invoking tool from [Invoke].
// It is not a decision, and
// a view must not use it as one: refusing or holding a call is
// [Config.BeforeToolCall]'s, whose verdict the record keeps, and asking
// a person is a [Question] answered with Reply. A product's controls,
// such as switching the model, are not the loop's vocabulary and are
// not in the contract.
//
// Queue stands for [Agent.Steer] and [Agent.FollowUp]: it carries a
// context and an error, which a wire needs and which a recorder that
// writes a queued input before the agent takes it has to return.
// Reply carries a context for the same reason, and so that who answered
// a question can be known as who prompted is: the answerer the replier
// put on it ([ContextWithAnswerer]) is named on [QuestionClosed] and to
// a recorder ([WatchAnswerer]).
type Control interface {
	Prompt(ctx context.Context, items ...openresponses.Item) (*RunEnd, error)
	Resume(ctx context.Context, answers ...Answer) (*RunEnd, error)
	Queue(ctx context.Context, mode QueueMode, items ...openresponses.Item) error
	Abort()
	State() State
	Subscribe(fn func(context.Context, Event) error) (unsubscribe func())
	Reply(ctx context.Context, id string, answer agenttool.Answer) error
}

var _ Control = (*Agent)(nil)

// Question event type names.
const (
	EventQuestion       = "question"
	EventQuestionClosed = "question_closed"
)

// Question is asked while a call runs and waits for an answer: a
// tool's own question, an MCP server's elicitation among them, or a
// nested call its hook deferred, which [Ask] puts to the user. The
// agent's [Agent.QuestionElicitor] raises it, when a host installs
// that as [Config.ToolElicitor]; [Agent.Reply] answers it, and
// [QuestionClosed] follows either way.
//
// It is delivered through the agent's barrier like every event, from
// the goroutine of the call that asks, so it falls between the run's
// other events: after the asking call's tool_dispatch and before its
// tool_end, and for a nested call [Ask] puts to the user, before that
// call's tool_start, which carries the decision the answer made. Calls
// of a batch that run beside the asking one go on and raise their own
// events while it waits. A subscriber that attaches while a question
// waits is sent it before any other event, since it still wants an
// answer; no other past event is replayed.
//
// A subscriber returns from it at once: the barrier holds every other
// event until it does. It answers with Reply, inside the event or
// later, from any goroutine.
type Question struct {
	// ID names the question for [Agent.Reply].
	ID string
	// RunID is the run whose call asks, and empty when the call is not
	// this agent's run's: a job on [RunContext] after its run ended, or
	// a child agent's call that asks through the parent's elicitor.
	RunID string
	// CallID is the call whose tool asks, from agenttool.CallFrom, and
	// empty when the question carries none.
	CallID string
	// Call is the call the question is about, for a nested call [Ask]
	// puts to the user, and nil for a tool's own question.
	Call *AskedCall
	// Elicitation is the question as the tool, or Ask, put it.
	Elicitation agenttool.Elicitation
}

// EventType returns "question".
func (*Question) EventType() string { return EventQuestion }

// QuestionClosed reports that a [Question] no longer waits. Answer is
// what [Agent.Reply] gave it, and nil when the call gave up first: its
// context ended, by an abort among other things, or the question could
// not be delivered. By is who answered: the answerer the replier put on
// Reply's context with [ContextWithAnswerer], such as front/control's
// principal, and zero when it named none or nothing answered.
type QuestionClosed struct {
	ID     string
	RunID  string
	Answer *agenttool.Answer
	By     Trigger
}

// EventType returns "question_closed".
func (*QuestionClosed) EventType() string { return EventQuestionClosed }

var (
	_ Event = (*Question)(nil)
	_ Event = (*QuestionClosed)(nil)
)

// ErrNoQuestion is returned by [Agent.Reply] for an ID that names no
// waiting question: never asked, already answered, or given up.
var ErrNoQuestion = errors.New("agentturn: no such question is waiting")

// asking is a question waiting for its answer.
type asking struct {
	reply chan replied
}

// replied is an answer [Agent.Reply] gave and who gave it.
type replied struct {
	answer agenttool.Answer
	by     Trigger
}

type answererKey struct{}

type watchKey struct{}

// ContextWithAnswerer returns ctx naming by as who answers the questions
// replied to under it, for [Agent.Reply]. front/control names its
// principal so; an in-process client names itself. It is set apart
// from the run's trigger on purpose: a [Question] is delivered on the
// asking call's context, which carries the trigger of whoever started
// the run, and a subscriber replying with that context has not said
// who it is.
func ContextWithAnswerer(ctx context.Context, by Trigger) context.Context {
	return context.WithValue(ctx, answererKey{}, by)
}

// AnswererFrom returns the answerer [ContextWithAnswerer] put on ctx.
func AnswererFrom(ctx context.Context) (Trigger, bool) {
	by, ok := ctx.Value(answererKey{}).(Trigger)
	return by, ok
}

// answerer is where the asking side of a question learns who answered
// it; each one installed under ctx hears of the answer.
type answerer struct {
	mu     sync.Mutex
	by     Trigger
	parent *answerer
}

// WatchAnswerer returns ctx and a function that reports who answered a
// question asked under it through [Agent.Reply]: the answerer the
// replier named with [ContextWithAnswerer]. An elicitor that wraps the
// agent's [Agent.QuestionElicitor], such as a session recorder's, asks
// the inner one under the returned context and calls the function once
// it has returned, to write who answered beside the answer. The
// function returns the zero Trigger when nothing answered through Reply
// or the replier named nobody.
func WatchAnswerer(ctx context.Context) (context.Context, func() Trigger) {
	parent, _ := ctx.Value(watchKey{}).(*answerer)
	h := &answerer{parent: parent}
	return context.WithValue(ctx, watchKey{}, h), func() Trigger {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.by
	}
}

// tellAnswerer passes who answered to every [WatchAnswerer] under ctx.
func tellAnswerer(ctx context.Context, by Trigger) {
	for h, _ := ctx.Value(watchKey{}).(*answerer); h != nil; h = h.parent {
		h.mu.Lock()
		h.by = by
		h.mu.Unlock()
	}
}

// QuestionElicitor returns an agenttool.Elicitor that asks through the
// agent's subscribers: it delivers a [Question], waits for
// [Agent.Reply] with its ID or for the asking call's context to end,
// delivers [QuestionClosed], and returns the answer, or the context's
// error when the call gave up. A subscriber that fails on the question
// gives it up too, and the elicitor returns that error.
//
// A host turns it on by installing it as [Config.ToolElicitor], with
// [Agent.SetConfig] after [New] since the elicitor is the agent's; the
// loop never installs it by itself, because a run inside an MCP tool
// relies on the elicitor already on its context. A host with nobody to
// answer leaves it out, and its tools' questions go unasked as before.
// A session recorder's Elicitor wraps it like any elicitor, so the
// question and its answer are written under the call.
//
// It must not be called from inside one of the agent's subscribers,
// which holds the barrier the question is delivered through.
func (a *Agent) QuestionElicitor() agenttool.Elicitor {
	return a.ask
}

func (a *Agent) ask(ctx context.Context, q agenttool.Elicitation) (agenttool.Answer, error) {
	if err := ctx.Err(); err != nil {
		return agenttool.Answer{}, err
	}
	ev := &Question{ID: openresponses.NewID("question"), Elicitation: q}
	ev.RunID, _ = ctx.Value(inRunKey{a}).(string)
	if call, ok := agenttool.CallFrom(ctx); ok {
		ev.CallID = call.ID
	}
	if call, ok := AskedCallFrom(ctx); ok {
		ev.Call = &call
	}
	w := &asking{reply: make(chan replied, 1)}
	a.mu.Lock()
	if a.asks == nil {
		a.asks = map[string]*asking{}
	}
	a.asks[ev.ID] = w
	a.mu.Unlock()

	// The question and its close reach subscribers that write durable
	// state after an abort, as every event does.
	dctx := context.WithoutCancel(ctx)
	err := a.deliver(dctx, ev)
	if err == nil {
		select {
		case r := <-w.reply:
			tellAnswerer(ctx, r.by)
			_ = a.deliver(dctx, &QuestionClosed{ID: ev.ID, RunID: ev.RunID, Answer: &r.answer, By: r.by})
			return r.answer, nil
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	// Given up: unless a reply won the race, nothing can answer it now.
	a.mu.Lock()
	_, waiting := a.asks[ev.ID]
	delete(a.asks, ev.ID)
	a.mu.Unlock()
	if !waiting {
		r := <-w.reply
		tellAnswerer(ctx, r.by)
		_ = a.deliver(dctx, &QuestionClosed{ID: ev.ID, RunID: ev.RunID, Answer: &r.answer, By: r.by})
		return r.answer, nil
	}
	_ = a.deliver(dctx, &QuestionClosed{ID: ev.ID, RunID: ev.RunID})
	return agenttool.Answer{}, err
}

// Reply answers the [Question] id with answer: an accept, with the
// content a form asked for, a decline, or a cancel. The answerer on ctx
// ([ContextWithAnswerer]) names who answered: [QuestionClosed] carries
// it, and so does the [WatchAnswerer] the question was asked under.
// Nothing else on ctx names the answerer, the run's trigger least of
// all, which the question's own event context carries: an answer whose
// replier named nobody is unattributed. Only ctx's values are read;
// Reply does not wait on it. It
// returns [ErrNoQuestion] when id names no waiting question, one
// already answered or given up among them. It never blocks, so a
// subscriber may reply from inside the question's own event.
func (a *Agent) Reply(ctx context.Context, id string, answer agenttool.Answer) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	w, ok := a.asks[id]
	if !ok {
		return fmt.Errorf("%w: %q", ErrNoQuestion, id)
	}
	delete(a.asks, id)
	by, _ := AnswererFrom(ctx)
	w.reply <- replied{answer: answer, by: by}
	return nil
}
