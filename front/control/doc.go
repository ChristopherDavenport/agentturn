// Package control serves agentturn's Control contract over HTTP and
// dials it: the human plane, a person at a client or a controller that
// answers by rule, driving an agent that runs elsewhere. [Handler]
// serves any agentturn.Control, an *agentturn.Agent or a host's wrapper
// of one; [Dial] returns a [Client], itself an agentturn.Control, so a
// front written against the contract drives an agent in this process
// or behind the wire alike.
//
// It is the third of agentturn's fronts. front/responses serves an
// agent as a model, and front/a2a as a peer, both lossy by design; this
// one carries the loop's own events, unnarrowed, and the questions a
// running call asks, as [agentturn.Question] events a client answers
// with Reply.
//
// What is committed is the record's to carry, not this stream's: a
// client that connects late, or whose stream broke, catches up from
// the session store (agentsession's store protocol, which a host mounts
// beside this handler), and the stream replays nothing but a question
// still waiting.
//
// A run belongs to the agent, not to the request that started it: a
// connection that drops does not abort it, since over a wire that is
// not the person's intent, and only an abort does. A [Client] keeps the
// in-process meaning of its caller's context: cancelling it sends the
// abort, and a connection lost while the caller still waits returns
// [ErrConnectionLost], with the run perhaps still going.
//
// Every request is authenticated ([WithAuthenticator], or
// [WithInsecureNoAuth] for tests) and each method needs a scope:
// read, control, answer (which no other scope implies, since it lets a
// call run that a policy asked a person about), or command, for a
// product's own controls ([WithCommands]). Transport security is the
// host's http.Server's.
package control
