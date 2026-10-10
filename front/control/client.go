package control

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// Client drives an agent a [Handler] serves: it is an
// agentturn.Control, so a front written against the contract drives a
// remote agent as it drives one in process. What the contract cannot
// carry over a wire, and what this does instead:
//
//   - State and Abort return nothing, so a failure of either goes to
//     the [WithStreamErrors] function; State then returns the zero
//     State. [Client.FetchState] returns the error.
//   - A subscriber's error does not reach the agent: a remote
//     subscriber cannot veto an event, as one in process can.
//   - Subscribe returns once the stream is in place, once the first
//     attempt failed, or after ten seconds of neither. A stream that breaks is opened again after a
//     pause, and the events in between are not sent: the client
//     catches up on what was committed from the record. A question
//     still waiting is sent again, as the agent sends it to every new
//     subscriber. Each break goes to the WithStreamErrors function,
//     wrapping [ErrStreamBroken], so a view knows to catch up.
//   - Errors are *[RemoteError]s: their text, and errors.Is for the
//     agentturn sentinels they matched.
//   - Prompt and Resume keep the caller's intent, not the connection's
//     fate. A caller that cancels its context aborts the run, as in
//     process: the client sends Abort and returns the context's error.
//     A connection that fails while the caller still waits does not
//     abort it, since the server runs it apart from the request: the
//     call returns an error wrapping [ErrConnectionLost], the run may
//     still be going, and the caller follows it with Subscribe and
//     State, or the record.
type Client struct {
	base     *url.URL
	hc       *http.Client
	token    string
	onError  func(error)
	retry    time.Duration
	wait     time.Duration
	maxFrame int
	maxBody  int64
}

var _ agentturn.Control = (*Client)(nil)

// DialOption configures a Client.
type DialOption func(*Client)

// WithHTTPClient sets the client requests go through; the default is
// http.DefaultClient. A Prompt or Resume lasts as long as its run, so
// a client with a Timeout cuts long runs off.
func WithHTTPClient(hc *http.Client) DialOption { return func(c *Client) { c.hc = hc } }

// WithBearerToken sends token as "Authorization: Bearer <token>", as
// [BearerTokens] reads it.
func WithBearerToken(token string) DialOption { return func(c *Client) { c.token = token } }

// WithStreamErrors is told of what the contract has no error for: a
// failed State or Abort, a stream that broke or could not open, an
// event that could not be read.
func WithStreamErrors(fn func(error)) DialOption { return func(c *Client) { c.onError = fn } }

// WithReconnectDelay sets the pause before a broken stream is opened
// again. The default is a second.
func WithReconnectDelay(d time.Duration) DialOption { return func(c *Client) { c.retry = d } }

// WithMaxResponseBytes bounds a response body and an event frame. The
// default is 64 MiB: a run's end carries its transcript.
func WithMaxResponseBytes(n int64) DialOption {
	return func(c *Client) { c.maxBody, c.maxFrame = n, int(n) }
}

// ErrConnectionLost wraps the transport's failure when a Prompt or a
// Resume lost its connection while its caller still waited: the run
// was not aborted and may still be going on the agent.
var ErrConnectionLost = errors.New("control: the connection was lost; the run may still be going")

// lostError marks a transport failure, as opposed to an answer.
type lostError struct{ err error }

func (e *lostError) Error() string { return e.err.Error() }
func (e *lostError) Unwrap() error { return e.err }

// ErrStreamBroken wraps what a broken event stream reports to the
// [WithStreamErrors] function: events may have been missed.
var ErrStreamBroken = errors.New("control: the event stream broke; events may have been missed")

// Dial checks that baseURL serves this protocol, to the client's
// credentials, and returns a client for it.
func Dial(ctx context.Context, baseURL string, opts ...DialOption) (*Client, error) {
	u, err := url.Parse(strings.TrimSuffix(baseURL, "/") + "/")
	if err != nil {
		return nil, fmt.Errorf("control: dial: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("control: dial: scheme %q is not http or https", u.Scheme)
	}
	c := &Client{base: u, hc: http.DefaultClient, retry: time.Second, wait: 10 * time.Second, maxBody: 64 << 20, maxFrame: 64 << 20}
	for _, o := range opts {
		o(c)
	}
	var info struct {
		Protocol string `json:"protocol"`
	}
	if err := c.call(ctx, http.MethodGet, "", nil, &info); err != nil {
		return nil, fmt.Errorf("control: dial: %w", err)
	}
	if info.Protocol != Protocol {
		return nil, fmt.Errorf("control: dial: %s serves %q, not %q", u.Redacted(), info.Protocol, Protocol)
	}
	return c, nil
}

func (c *Client) report(err error) {
	if c.onError != nil && err != nil {
		c.onError(err)
	}
}

// call sends a request and decodes a 2xx answer into out; any other
// status is the error its body carries.
func (c *Client) call(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base.JoinPath(path).String(), rd)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	c.authorize(req)
	resp, err := c.hc.Do(req)
	if err != nil {
		return &lostError{err}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, c.maxBody+1))
	if err != nil {
		return &lostError{err}
	}
	if int64(len(data)) > c.maxBody {
		return fmt.Errorf("control: response over %d bytes", c.maxBody)
	}
	if resp.StatusCode/100 != 2 {
		var e errReply
		if json.Unmarshal(data, &e) == nil && e.Err != nil {
			return errorIn(e.Err)
		}
		return fmt.Errorf("control: %s %s: %s", method, path, resp.Status)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}

func (c *Client) authorize(req *http.Request) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
}

func (c *Client) run(ctx context.Context, path string, body any) (*agentturn.RunEnd, error) {
	var out runReply
	if err := c.call(ctx, http.MethodPost, path, body, &out); err != nil {
		var lost *lostError
		switch {
		case ctx.Err() != nil:
			// The caller's intent: abort the run, which the server keeps
			// going past a closed request.
			c.Abort()
			return nil, ctx.Err()
		case errors.As(err, &lost):
			return nil, fmt.Errorf("%w: %w", ErrConnectionLost, lost.err)
		}
		return nil, err
	}
	var end *agentturn.RunEnd
	if out.End != nil {
		var err error
		if end, err = runEndIn(*out.End); err != nil {
			return nil, err
		}
	}
	return end, errorIn(out.Err)
}

// Prompt implements agentturn.Control. Cancelling ctx aborts the run,
// as it does in process; a lost connection does not (see [Client]).
func (c *Client) Prompt(ctx context.Context, items ...openresponses.Item) (*agentturn.RunEnd, error) {
	raw, err := itemsOut(items)
	if err != nil {
		return nil, err
	}
	return c.run(ctx, "prompt", struct {
		Items json.RawMessage `json:"items"`
	}{raw})
}

// Resume implements agentturn.Control.
func (c *Client) Resume(ctx context.Context, answers ...agentturn.Answer) (*agentturn.RunEnd, error) {
	w, err := answersOut(answers)
	if err != nil {
		return nil, err
	}
	return c.run(ctx, "resume", struct {
		Answers []wireAnswer `json:"answers"`
	}{w})
}

// Queue implements agentturn.Control.
func (c *Client) Queue(ctx context.Context, mode agentturn.QueueMode, items ...openresponses.Item) error {
	raw, err := itemsOut(items)
	if err != nil {
		return err
	}
	var out errReply
	if err := c.call(ctx, http.MethodPost, "queue", struct {
		Mode  string          `json:"mode"`
		Items json.RawMessage `json:"items"`
	}{string(mode), raw}, &out); err != nil {
		return err
	}
	return errorIn(out.Err)
}

// Abort implements agentturn.Control. A failure goes to the
// [WithStreamErrors] function.
func (c *Client) Abort() {
	ctx, cancel := context.WithTimeout(context.Background(), c.wait)
	defer cancel()
	c.report(c.call(ctx, http.MethodPost, "abort", nil, nil))
}

// State implements agentturn.Control. A failure goes to the
// [WithStreamErrors] function and returns the zero State;
// [Client.FetchState] returns it.
func (c *Client) State() agentturn.State {
	ctx, cancel := context.WithTimeout(context.Background(), c.wait)
	defer cancel()
	s, err := c.FetchState(ctx)
	c.report(err)
	return s
}

// FetchState is State with its error.
func (c *Client) FetchState(ctx context.Context) (agentturn.State, error) {
	var w wireState
	if err := c.call(ctx, http.MethodGet, "state", nil, &w); err != nil {
		return agentturn.State{}, err
	}
	return stateIn(w)
}

// Reply implements agentturn.Control. A question that is not waiting
// is an error that matches agentturn.ErrNoQuestion.
func (c *Client) Reply(id string, answer agenttool.Answer) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.wait)
	defer cancel()
	var out errReply
	if err := c.call(ctx, http.MethodPost, "reply", struct {
		ID     string            `json:"id"`
		Answer *wireElicitAnswer `json:"answer"`
	}{id, elicitAnswerOut(&answer)}, &out); err != nil {
		return err
	}
	return errorIn(out.Err)
}

// CommandInfo describes a command the handler serves.
type CommandInfo struct {
	Name        string
	Description string
	Schema      json.RawMessage
}

// Commands lists the product's commands the handler serves.
func (c *Client) Commands(ctx context.Context) ([]CommandInfo, error) {
	var w []commandInfo
	if err := c.call(ctx, http.MethodGet, "commands", nil, &w); err != nil {
		return nil, err
	}
	out := make([]CommandInfo, 0, len(w))
	for _, i := range w {
		out = append(out, CommandInfo(i))
	}
	return out, nil
}

// Command runs the product's command name with args, a JSON value or
// nil, and returns its result. A name the handler does not serve is an
// error that matches [ErrUnknownCommand].
func (c *Client) Command(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	var body any
	if args != nil {
		body = args
	}
	var out commandReply
	if err := c.call(ctx, http.MethodPost, "commands/"+url.PathEscape(name), body, &out); err != nil {
		return nil, err
	}
	return out.Result, errorIn(out.Err)
}

// Subscribe implements agentturn.Control: fn is given the agent's
// events in order, from one goroutine of the client's, until
// unsubscribe. See [Client] for what a broken stream does.
func (c *Client) Subscribe(fn func(context.Context, agentturn.Event) error) (unsubscribe func()) {
	ctx, cancel := context.WithCancel(context.Background())
	var stopped atomic.Bool
	ready := make(chan struct{})
	var once sync.Once
	markReady := func() { once.Do(func() { close(ready) }) }
	go c.follow(ctx, func(ev agentturn.Event) {
		if !stopped.Load() {
			_ = fn(ctx, ev)
		}
	}, markReady)
	select {
	case <-ready:
	case <-time.After(c.wait):
	}
	return func() {
		stopped.Store(true)
		cancel()
	}
}

// follow keeps a stream open until ctx ends, calling ready once the
// first is in place or has failed.
func (c *Client) follow(ctx context.Context, deliver func(agentturn.Event), ready func()) {
	for {
		err := c.stream(ctx, deliver, ready)
		ready()
		if ctx.Err() != nil {
			return
		}
		c.report(fmt.Errorf("%w: %w", ErrStreamBroken, err))
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.retry):
		}
	}
}

// errStreamEnded is a stream the server closed.
var errStreamEnded = errors.New("the server closed the stream")

// stream opens one event stream and delivers its events until it ends.
func (c *Client) stream(ctx context.Context, deliver func(agentturn.Event), ready func()) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base.JoinPath("events").String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	c.authorize(req)
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		var e errReply
		if json.Unmarshal(data, &e) == nil && e.Err != nil {
			return errorIn(e.Err)
		}
		return fmt.Errorf("control: events: %s", resp.Status)
	}
	frames := newFrameReader(resp.Body, c.maxFrame)
	for {
		event, data, err := frames.next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return errStreamEnded
			}
			return err
		}
		switch event {
		case "ready":
			ready()
			continue
		case "overflow":
			return fmt.Errorf("control: events: %s", data)
		case "", "message":
		default:
			continue
		}
		ev, err := UnmarshalEvent(data)
		if err != nil {
			c.report(err)
			continue
		}
		deliver(ev)
	}
}

// frameReader reads server-sent events: the event name and the data of
// each, the data lines joined.
type frameReader struct {
	r   *bufio.Reader
	max int
}

func newFrameReader(r io.Reader, max int) *frameReader {
	return &frameReader{r: bufio.NewReaderSize(r, 64<<10), max: max}
}

func (f *frameReader) next() (event string, data []byte, err error) {
	var buf bytes.Buffer
	seen := false
	for {
		line, err := f.line()
		if err != nil {
			return "", nil, err
		}
		if len(line) == 0 {
			if seen {
				return event, buf.Bytes(), nil
			}
			event = ""
			continue
		}
		field, value, _ := strings.Cut(string(line), ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "":
			// A comment: a ping.
		case "event":
			event = value
		case "data":
			if seen {
				buf.WriteByte('\n')
			}
			buf.WriteString(value)
			seen = true
			if buf.Len() > f.max {
				return "", nil, fmt.Errorf("control: an event over %d bytes", f.max)
			}
		}
	}
}

// line reads one line, without its ending, bounded by max.
func (f *frameReader) line() ([]byte, error) {
	var out []byte
	for {
		chunk, isPrefix, err := f.r.ReadLine()
		if err != nil {
			return nil, err
		}
		out = append(out, chunk...)
		if len(out) > f.max {
			return nil, fmt.Errorf("control: a line over %d bytes", f.max)
		}
		if !isPrefix {
			return out, nil
		}
	}
}
