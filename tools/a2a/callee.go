package a2a

import (
	"context"
	"sync"

	"github.com/a2aproject/a2a-go/a2aclient"
)

// The namespaces of the records a call writes through
// agenttool.WriteRecord while it runs. A session recorder that knows
// them turns each into a link entry of the call: a [Callee] into a link
// with rel "subsession" or "peer" naming the callee in another system,
// a [CancelSent] into the notice that a cancel was sent for it. One that
// does not writes each as a custom entry beside the call, as it writes
// any record. The JSON of each type is the contract, so a recorder reads
// it without importing this module or a2a-go.
const (
	CalleeNS = "agentturn:a2a_callee"
	CancelNS = "agentturn:a2a_cancel"
)

// The relations a [Callee] names, the values of the session format's
// link relations of the same name. The link follows the callee's
// lifetime: a task the call cancels when it is given up is the call's
// own work, a subsession, which is the tool's default; a task that
// outlives the call under [WithoutCancelOnAbort] is a peer's, which
// keeps its own record.
const (
	RelationSubsession = "subsession"
	RelationPeer       = "peer"
)

// Callee is the record of the callee a call reached, written as soon as
// the call learns it: when the remote agent names the task, or, for an
// answer that is a plain message with no task, when the answer arrives.
// A call dispatched again, after a restart, reaches a new task and
// writes a new record.
type Callee struct {
	// URL is the endpoint the call was sent to, the one the client
	// connected to, never a name the agent's card claims.
	URL string `json:"url"`
	// Task and Context are the remote agent's task and context IDs. Task
	// is empty for an answer that was a plain message.
	Task    string `json:"task,omitempty"`
	Context string `json:"context,omitempty"`
	// Relation is [RelationSubsession] or [RelationPeer].
	Relation string `json:"relation"`
}

// RecordNS names the record's namespace, [CalleeNS].
func (Callee) RecordNS() string { return CalleeNS }

// CancelSent is the record that the call asked the remote agent to stop
// a task, written when the call was given up before the task ended or
// ended with the task in input-required or auth-required: tasks/cancel
// was sent, and the
// call did not wait for the task to reach canceled, so it says the stop
// was asked for, not that the work stopped. A peer's task under [WithoutCancelOnAbort] is never
// cancelled and has none.
type CancelSent struct {
	URL  string `json:"url"`
	Task string `json:"task"`
}

// RecordNS names the record's namespace, [CancelNS].
func (CancelSent) RecordNS() string { return CancelNS }

// endpoint is filled, per call, with the URL the client sent the call's
// requests to.
type endpoint struct {
	mu  sync.Mutex
	url string
}

func (e *endpoint) set(url string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.url == "" {
		e.url = url
	}
}

func (e *endpoint) get() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.url
}

type endpointKey struct{}

// ObserveEndpoint returns the client interceptor that tells a call the
// URL each of its requests goes to: the endpoint the client chose among
// the card's interfaces, which is the one the [Callee] record names.
// Install it when building the client, so [New] need not change it:
//
//	client, _ := a2aclient.NewFromCard(ctx, card, a2aclient.WithInterceptors(a2a.ObserveEndpoint()))
//
// It reads only the calls of this package's tools, through their
// contexts, so one client can serve several tools and other callers.
func ObserveEndpoint() a2aclient.CallInterceptor { return endpointWatch{} }

type endpointWatch struct {
	a2aclient.PassthroughInterceptor
}

func (endpointWatch) Before(ctx context.Context, req *a2aclient.Request) (context.Context, error) {
	if e, ok := ctx.Value(endpointKey{}).(*endpoint); ok && req != nil {
		e.set(req.BaseURL)
	}
	return ctx, nil
}
