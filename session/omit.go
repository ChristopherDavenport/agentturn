package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// rebuiltItem is one item of the input the stored path rebuilds for a
// request: the item as the path holds it, which is the item the request
// hash covers, and the item the loop handed the writer for it when the
// request carries that one as it is, so an input holding the very item
// matches it without encoding either.
type rebuiltItem struct {
	item openresponses.Item
	// orig is the writer's own item, nil when a placeholder replaced
	// part of it for this request.
	orig openresponses.Item
	// at is the index of the item among the writer's, -1 for a fold's
	// summary or pins.
	at int
}

// rebuild is the input the stored path rebuilds for a request to model
// under omit, as the context algorithm reads it: the summary of the
// last fold recorded and the items that fold pinned, then the items
// written as item entries from the fold's first kept one on, less the
// ones omit leaves out, with the outputs and the image parts it
// reaches sent as its placeholders. It reports false when the writer
// cannot rebuild it: a fold that keeps more items than the writer
// holds, or an images member that names an entry the writer does not
// place.
func (w *writer) rebuild(model string, omit agentsession.Omit) ([]rebuiltItem, bool) {
	from := 0
	var out []rebuiltItem
	if w.foldSet {
		if w.foldSplit > len(w.values) {
			return nil, false
		}
		from = w.foldSplit
		out = append(out, w.foldHead()...)
	}
	listed := idSet(omit.Items)
	for i := from; i < len(w.values); i++ {
		if w.custom[i] || !w.meta[i].folded && listed[w.items[i]] || w.leftOut(i, model, omit) {
			continue
		}
		out = append(out, w.rebuiltAt(i))
	}
	if omit.Outputs != nil {
		for k, r := range out {
			if r.at < 0 || w.meta[r.at].folded {
				continue
			}
			text, listed := omit.Outputs.TextFor(w.items[r.at])
			if !listed {
				continue
			}
			if o, ok := w.recordedItem(r.at).(*openresponses.FunctionCallOutput); ok {
				out[k].item, out[k].orig = outputPlaceholder(o, text), nil
			}
		}
	}
	im := omit.Images
	if im == nil {
		return out, true
	}
	// reached says whether the n-th image part of the item entries left,
	// counted in request order, at the k-th item, is sent as the
	// placeholder.
	var reached func(k, n int) bool
	if im.Before != "" {
		before := slices.Index(w.items, im.Before)
		if before < 0 || w.meta[before].folded {
			return nil, false
		}
		reached = func(k, _ int) bool { return out[k].at < before }
	} else {
		total := 0
		for _, r := range out {
			if r.at >= 0 && !w.meta[r.at].folded {
				total += len(imageParts(r.item))
			}
		}
		reached = func(_, n int) bool { return n < total-im.Keep }
	}
	n := 0
	for k, r := range out {
		if r.at < 0 || w.meta[r.at].folded {
			continue
		}
		replace := map[int]bool{}
		for _, p := range imageParts(r.item) {
			if reached(k, n) {
				replace[p] = true
			}
			n++
		}
		if len(replace) > 0 {
			out[k].item, out[k].orig = withPlaceholders(w.recordedItem(r.at), replace, im.Placeholder), nil
		}
	}
	return out, true
}

// foldHead is the summary and the pins of the last fold recorded, which
// open the input the path rebuilds.
func (w *writer) foldHead() []rebuiltItem {
	head := append(openresponses.Items{w.foldSummary}, w.foldPinned...)
	out := make([]rebuiltItem, len(head))
	for k, item := range head {
		out[k] = rebuiltItem{item: item, orig: item, at: -1 - k}
		if k < len(w.foldRecorded) {
			out[k].item = w.foldRecorded[k]
		}
	}
	return out
}

// canon returns the canonical JSON of r's item as the path holds it:
// for an item as recorded, the bytes kept when it was written, so an
// item the loop or a hook edits in place afterwards is compared with
// what the record holds rather than with itself; for an item a
// placeholder edited, encoded now. A seeded item, which nothing else
// holds, is encoded the first time it is asked for.
func (w *writer) canon(r rebuiltItem) ([]byte, bool) {
	if r.orig == nil {
		return canonicalValue(r.item)
	}
	var slot *[]byte
	switch {
	case r.at >= 0:
		slot = &w.meta[r.at].canon
	default:
		k := -1 - r.at
		if len(w.foldCanon) != len(w.foldPinned)+1 {
			w.foldCanon = make([][]byte, len(w.foldPinned)+1)
		}
		slot = &w.foldCanon[k]
	}
	if *slot == nil {
		c, ok := canonicalValue(r.item)
		if !ok {
			return nil, false
		}
		*slot = c
	}
	return *slot, true
}

// sentInput is a request's input as the writer compares and hashes it:
// each item in the form the session records it, encoded at most once a
// turn and only when asked for, so the hash always covers what was
// sent, an item edited in place since it was recorded included (#282).
type sentInput struct {
	items openresponses.Items
	canon [][]byte
	enc   []bool
}

func newSentInput(items openresponses.Items) *sentInput {
	return &sentInput{items: items, canon: make([][]byte, len(items)), enc: make([]bool, len(items))}
}

// canonAt is the canonical JSON of the k-th item sent.
func (w *writer) canonAt(s *sentInput, k int) ([]byte, bool) {
	if !s.enc[k] {
		s.canon[k], _ = canonicalValue(w.recordForm(s.items[k]))
		s.enc[k] = true
	}
	return s.canon[k], s.canon[k] != nil
}

// hashOf is the request hash of req with the input sent, built by
// [requestHashOf] from each item's canonical form as the turn encoded
// it.
func (w *writer) hashOf(req openresponses.Request, sent *sentInput) (string, error) {
	canon := make([][]byte, len(sent.items))
	for k := range sent.items {
		c, ok := w.canonAt(sent, k)
		if !ok {
			return "", fmt.Errorf("session: encode request item %d", k)
		}
		canon[k] = c
	}
	return requestHashOf(req, canon)
}

// recordedItem is the writer's i-th item as its entry holds it: the
// item itself, unless it was edited in place since it was written, in
// which case it is decoded from the bytes kept then. What a placeholder
// stands in for is the recorded item, not the edited one.
func (w *writer) recordedItem(i int) openresponses.Item {
	r := w.rebuiltAt(i)
	kept, ok := w.canon(r)
	if !ok {
		return r.item
	}
	if now, ok := canonicalValue(r.item); ok && bytes.Equal(now, kept) {
		return r.item
	}
	if item, err := openresponses.UnmarshalItem(kept); err == nil {
		return item
	}
	return r.item
}

// rebuiltAt is the writer's i-th item as the input the path rebuilds
// holds it, before any placeholder.
func (w *writer) rebuiltAt(i int) rebuiltItem {
	r := rebuiltItem{item: w.values[i], orig: w.values[i], at: i}
	if rec := w.meta[i].recorded; rec != nil {
		r.item = rec
	}
	return r
}

// recordedPins is the pins of a fold as its entry holds them: recorded,
// or pinned itself when there are none, so an entry with no pins
// encodes as it did.
func recordedPins(pinned, recorded openresponses.Items) openresponses.Items {
	if len(pinned) == 0 {
		return pinned
	}
	return recorded
}

// matches reports whether sent, a request's input, is the input
// rebuilt, item for item.
func (w *writer) matches(sent *sentInput, rebuilt []rebuiltItem) bool {
	if len(sent.items) != len(rebuilt) {
		return false
	}
	for k, r := range rebuilt {
		if !w.same(sent, k, r) {
			return false
		}
	}
	return true
}

// same reports whether the k-th item sent encodes, as the session
// records it, to what r encodes to as the path holds it. Identity is
// not enough: an item may have been edited in place since it was
// recorded.
func (w *writer) same(sent *sentInput, k int, r rebuiltItem) bool {
	have, ok := w.canonAt(sent, k)
	if !ok {
		return false
	}
	want, ok := w.canon(r)
	return ok && bytes.Equal(have, want)
}

// describe returns the omit setting under which the stored path
// rebuilds sent, a request's input to model, and reports whether there
// is one: the input differs from what the path holds only by item
// entries left out, their order kept, function call outputs sent as
// placeholder texts, one text to a group of format 0.16's
// omit.outputs, and the image parts of all but the last few sent as one
// placeholder, which is what the format's omit can say (#233). It
// looks first for a setting that adds to the one in force, which a
// config delta carries, and then for one that starts afresh, which only
// a replace does. A setting it finds is checked by rebuilding the input
// under it, so what it returns is one the path rebuilds sent from.
func (w *writer) describe(sent *sentInput, model string) (agentsession.Omit, []rebuiltItem, bool) {
	base := w.omitFor(model)
	if want, got, ok := w.describeOver(sent, model, base); ok {
		return want, got, true
	}
	return w.describeOver(sent, model, agentsession.Omit{Reasoning: base.Reasoning})
}

// describeOver is describe for a setting that adds to base.
func (w *writer) describeOver(in *sentInput, model string, base agentsession.Omit) (agentsession.Omit, []rebuiltItem, bool) {
	sent := in.items
	from, j := 0, 0
	if w.foldSet {
		if w.foldSplit > len(w.values) {
			return agentsession.Omit{}, nil, false
		}
		// A fold's summary and pins are no item entry omit reaches: the
		// request opens with them as they are.
		from = w.foldSplit
		head := w.foldHead()
		if len(sent) < len(head) {
			return agentsession.Omit{}, nil, false
		}
		for k, r := range head {
			if !w.same(in, k, r) {
				return agentsession.Omit{}, nil, false
			}
		}
		j = len(head)
	}
	listed := idSet(base.Items)
	// outText is the text the outputs in force send an entry's output
	// with, and whether they list it.
	outText := func(id string) (string, bool) {
		if base.Outputs == nil {
			return "", false
		}
		return base.Outputs.TextFor(id)
	}
	want := agentsession.Omit{Reasoning: base.Reasoning, Items: slices.Clone(base.Items)}
	// newOuts are the outputs sent as a placeholder that the outputs in
	// force do not list, by text, the texts in the order first sent.
	newOuts := map[string][]string{}
	var newTexts []string
	// images lists, in request order, whether each image part of the
	// item entries kept was sent as a placeholder, and placeholder is
	// the text that stood in for those that were.
	var images []bool
	placeholder := ""
	for i := from; i < len(w.values); i++ {
		if w.custom[i] {
			continue
		}
		if w.meta[i].folded {
			// Items a compaction contributes, seeded from a path that
			// holds it, are the fold's summary and pins.
			if j < len(sent) && w.same(in, j, w.rebuiltAt(i)) {
				j++
				continue
			}
			return agentsession.Omit{}, nil, false
		}
		if listed[w.items[i]] || w.leftOut(i, model, base) {
			continue
		}
		at := w.rebuiltAt(i)
		at.item = w.recordedItem(i)
		rec := at.item
		out, isOutput := rec.(*openresponses.FunctionCallOutput)
		text, cleared := outText(w.items[i])
		cleared = cleared && isOutput
		if cleared {
			rec = outputPlaceholder(out, text)
			at = rebuiltItem{item: rec, at: i}
		}
		if j < len(sent) {
			if w.same(in, j, at) {
				if !cleared {
					images = append(images, make([]bool, len(imageParts(rec)))...)
				}
				j++
				continue
			}
			conv := w.recordForm(sent[j])
			if text, ok := sentAsPlaceholder(conv, rec); ok && !cleared {
				if _, seen := newOuts[text]; !seen {
					newTexts = append(newTexts, text)
				}
				newOuts[text] = append(newOuts[text], w.items[i])
				j++
				continue
			}
			if replaced, text, ok := imagesAsPlaceholder(conv, rec); ok && (placeholder == "" || text == placeholder) {
				placeholder = text
				images = append(images, replaced...)
				j++
				continue
			}
		}
		want.Items = append(want.Items, w.items[i])
	}
	if j != len(sent) {
		return agentsession.Omit{}, nil, false
	}
	if base.Outputs != nil || len(newTexts) > 0 {
		want.Outputs = withOutputs(base.Outputs, newTexts, newOuts)
	}
	// The image parts sent as a placeholder are the first of them, all
	// but the last few: the keep form. The one in force stays when it
	// says the same.
	r := 0
	for r < len(images) && images[r] {
		r++
	}
	if slices.Contains(images[r:], true) {
		return agentsession.Omit{}, nil, false
	}
	switch {
	case r > 0:
		want.Images = &agentsession.OmitImages{Keep: len(images) - r, Placeholder: placeholder}
		if base.Images != nil && base.Images.Before == "" && base.Images.Placeholder == placeholder && len(images)-base.Images.Keep == r {
			want.Images = base.Images
		}
	case base.Images != nil:
		want.Images = base.Images
	}
	got, ok := w.rebuild(model, want)
	if !ok || !w.matches(in, got) {
		return agentsession.Omit{}, nil, false
	}
	return want, got, true
}

// withOutputs is the outputs member in force, have, with the outputs
// add lists sent with each text of texts: those of the default group's
// text in the default group, those of a named group's text in that
// group, and those of another text in a group of their own, the first
// text in the default group when have holds none, so a request whose
// outputs are sent with one text is described as format 0.14 describes
// it (#233).
func withOutputs(have *agentsession.OmitOutputs, texts []string, add map[string][]string) *agentsession.OmitOutputs {
	o := cloneOutputs(have)
	if o == nil {
		o = &agentsession.OmitOutputs{}
	}
	hasDefault := have != nil && (have.Text != "" || len(have.Items) > 0 || len(have.Groups) == 0)
	for _, text := range texts {
		group := slices.IndexFunc(o.Groups, func(g agentsession.OmitOutputGroup) bool { return g.Text == text })
		switch {
		case hasDefault && text == o.Text:
			o.Items = append(o.Items, add[text]...)
		case group >= 0:
			o.Groups[group].Items = append(o.Groups[group].Items, add[text]...)
		case !hasDefault:
			o.Text, o.Items, hasDefault = text, append(o.Items, add[text]...), true
		default:
			o.Groups = append(o.Groups, agentsession.OmitOutputGroup{Text: text, Items: slices.Clone(add[text])})
		}
	}
	return o
}

// cloneOutputs is a copy of o that shares no list with it, nil for nil.
func cloneOutputs(o *agentsession.OmitOutputs) *agentsession.OmitOutputs {
	if o == nil {
		return nil
	}
	cp := *o
	cp.Items = slices.Clone(o.Items)
	cp.Groups = nil
	for _, g := range o.Groups {
		cp.Groups = append(cp.Groups, agentsession.OmitOutputGroup{Text: g.Text, Items: slices.Clone(g.Items)})
	}
	return &cp
}

// outputPlaceholder is out sent with text in place of its output.
func outputPlaceholder(out *openresponses.FunctionCallOutput, text string) *openresponses.FunctionCallOutput {
	sent := *out
	sent.Output = openresponses.FunctionCallOutputData{Text: text}
	return &sent
}

// sentAsPlaceholder reports whether sent is rec, a function call
// output, with a text in place of its output, and returns the text.
func sentAsPlaceholder(sent, rec openresponses.Item) (string, bool) {
	s, ok := sent.(*openresponses.FunctionCallOutput)
	r, ok2 := rec.(*openresponses.FunctionCallOutput)
	if !ok || !ok2 || s.CallID != r.CallID || s.Output.Parts != nil {
		return "", false
	}
	return s.Output.Text, equalJSON(s, outputPlaceholder(r, s.Output.Text))
}

// imagesAsPlaceholder reports whether sent is rec with some of its
// image parts sent as one input_text placeholder, and returns, for each
// image part of rec in order, whether it was, and the text.
func imagesAsPlaceholder(sent, rec openresponses.Item) ([]bool, string, bool) {
	have, want := contentOf(sent), contentOf(rec)
	parts := imageParts(rec)
	if len(parts) == 0 || len(have) != len(want) || sent.ItemType() != rec.ItemType() {
		return nil, "", false
	}
	text := ""
	replace := map[int]bool{}
	replaced := make([]bool, len(parts))
	for k, p := range parts {
		t, ok := have[p].(*openresponses.InputText)
		if !ok {
			continue
		}
		if text != "" && t.Text != text {
			return nil, "", false
		}
		text = t.Text
		replace[p], replaced[k] = true, true
	}
	if len(replace) == 0 || !equalJSON(sent, withPlaceholders(rec, replace, text)) {
		return nil, "", false
	}
	return replaced, text, true
}

// contentOf returns the parts of an item that may hold an input_image:
// a message's content, and a function call output's output when it is
// a list of parts.
func contentOf(item openresponses.Item) openresponses.Contents {
	switch v := item.(type) {
	case *openresponses.Message:
		return v.Content
	case *openresponses.FunctionCallOutput:
		return v.Output.Parts
	}
	return nil
}

// imageParts returns the indexes of the input_image parts of an item,
// in order.
func imageParts(item openresponses.Item) []int {
	var out []int
	for i, p := range contentOf(item) {
		if _, ok := p.(*openresponses.InputImage); ok {
			out = append(out, i)
		}
	}
	return out
}

// withPlaceholders returns a copy of item whose parts at the indexes
// replace are input_text parts holding text.
func withPlaceholders(item openresponses.Item, replace map[int]bool, text string) openresponses.Item {
	parts := slices.Clone(contentOf(item))
	for i := range parts {
		if replace[i] {
			parts[i] = &openresponses.InputText{Text: text}
		}
	}
	switch v := item.(type) {
	case *openresponses.Message:
		sent := *v
		sent.Content = parts
		return &sent
	case *openresponses.FunctionCallOutput:
		sent := *v
		sent.Output = openresponses.FunctionCallOutputData{Parts: parts}
		return &sent
	}
	return item
}

// idSet is the set of the non-empty IDs in ids.
func idSet(ids []string) map[string]bool {
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		if id != "" {
			set[id] = true
		}
	}
	return set
}

// ErrNotOmittable is returned by [Recorder.Omit] for an ID that names no
// item entry of the context at the leaf.
var ErrNotOmittable = errors.New("session: not an item entry of the context")

// Omit records that the requests from here on leave out the item
// entries ids names, as a host that trims the transcript between runs
// does with [agentturn.Agent.SetTranscript], so the record says what it
// trimmed rather than leaving the recorder to find it: a config entry
// whose omit lists them, written at the leaf of the session of the run
// on ctx, or the root's. [Recorder.EntryOf] names the entry of an item
// the host holds. The recorder finds a trim it is not told of when the
// next request is sent, as long as the omit setting can describe it;
// naming the entries decides which of two equal items was left out.
// An ID that names no item entry of the context, one a fold replaced
// or a custom entry, is refused with [ErrNotOmittable] and nothing is
// written; one already left out is skipped. It is refused with
// [ErrRunActive] while a model call is in flight.
func (r *Recorder) Omit(ctx context.Context, ids ...string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	w := r.writerOf(ctx)
	if w.inFlight || w.settleReq != nil {
		return fmt.Errorf("session: omit during a model call: %w", ErrRunActive)
	}
	from := 0
	if w.foldSet {
		from = min(w.foldSplit, len(w.values))
	}
	want := w.settings.Omit
	want.Items = slices.Clone(want.Items)
	listed := idSet(want.Items)
	for _, id := range ids {
		i := slices.Index(w.items, id)
		if i < from || w.custom[i] || w.meta[i].folded {
			return fmt.Errorf("session: omit %q: %w", id, ErrNotOmittable)
		}
		if !listed[id] {
			listed[id] = true
			want.Items = append(want.Items, id)
		}
	}
	d, ok := w.settings.OmitDelta(want)
	if !ok {
		return fmt.Errorf("session: omit setting %+v is not one config entry", want)
	}
	if d == nil {
		return nil
	}
	entry := &agentsession.ConfigEntry{Omit: d}
	id, err := w.append(ctx, entry)
	if err != nil {
		return err
	}
	entry.ID = id
	w.settings = w.settings.Apply(entry)
	return nil
}

// quarantinedIn returns the items of t the omit setting in force at
// the writer's leaf says a quarantine left out, for an agent attached
// with t: each item entry it sends as [agentturn.QuarantinedOutput],
// and each it lists in items but a reasoning item, which the
// other-models rule and a prompt's attribution leave out for one model
// and not another. An entry's item in t is matched as
// agentturn.QuarantineSet.Bind matches one, the writer's own or every
// copy equal to it in content, so an agent seeded from a store that
// keeps the conversation whole, as front/a2a's is, quarantines what the
// record does, and what a front binds agrees. An agent seeded from the
// record has the context's items, which omit already shaped, and finds
// none.
func (w *writer) quarantinedIn(t agentturn.Transcript) openresponses.Items {
	if len(t) == 0 {
		return nil
	}
	set := w.quarantineSet()
	if set == nil {
		return nil
	}
	return w.bindQuarantine(set, t)
}

// quarantineSet returns what the omit setting in force at the writer's
// leaf says a quarantine left out, as [writer.quarantinedIn] reads it,
// each item in the agent's form and the record's, or nil when it says
// nothing is.
func (w *writer) quarantineSet() *agentturn.QuarantineSet {
	o := w.settings.Omit
	listed := idSet(o.Items)
	outs := map[string]bool{}
	if o.Outputs != nil {
		// The outputs the quarantine's text is sent for, in whichever
		// group lists them.
		for i := range w.values {
			if text, ok := o.Outputs.TextFor(w.items[i]); ok && text == agentturn.QuarantinedOutput {
				outs[w.items[i]] = true
			}
		}
	}
	if len(listed) == 0 && len(outs) == 0 {
		return nil
	}
	var recorded openresponses.Items
	for i := range w.values {
		if w.custom[i] || w.meta[i].folded {
			continue
		}
		_, reasoning := w.values[i].(*openresponses.ReasoningItem)
		if !outs[w.items[i]] && (!listed[w.items[i]] || reasoning) {
			continue
		}
		recorded = append(recorded, w.values[i], w.recordedItem(i))
	}
	if len(recorded) == 0 {
		return nil
	}
	set, err := agentturn.NewQuarantineSet(recorded...)
	if err != nil {
		return nil
	}
	return set
}

// bindQuarantine returns the items of t set holds.
func (w *writer) bindQuarantine(set *agentturn.QuarantineSet, t agentturn.Transcript) openresponses.Items {
	// In a sidecar session the record holds media as sidecar:
	// references and the agent as data URLs: t is bound in the form the
	// record holds, and each match is the agent's own item.
	conv := make(agentturn.Transcript, len(t))
	back := map[openresponses.Item]openresponses.Item{}
	for k, item := range t {
		conv[k] = w.recordForm(item)
		if item != nil && reflect.TypeOf(item).Comparable() && reflect.TypeOf(conv[k]).Comparable() && conv[k] != item {
			back[conv[k]] = item
		}
	}
	bound := set.Bind(conv).Items()
	for k, item := range bound {
		if orig, ok := back[item]; ok {
			bound[k] = orig
		}
	}
	return bound
}

// quarantineOver returns base, the omit setting in force, with the
// item entries of the path that quarantined, the run's quarantine as
// turn_start or model_retry carries it, holds added: a function call
// output to the outputs sent as [agentturn.QuarantinedOutput], a call
// and its outputs, and any other item, to the items left out. It is
// what the settle writes for a request whose input the format cannot
// describe, a hook's addition or a run on a root of its own that sends
// items the path does not hold among them: the response goes unhashed,
// and the quarantine in force is recorded all the same, so a resume, an
// agent attached after one and a spawn read it from the record (RFC
// 0001 The record). An entry is matched as agentturn.QuarantineSet.Bind
// matches an item, itself or a copy equal in content, so a writer
// seeded from the store finds the items the agent quarantined. Content
// matching errs on the side of leaving out: two entries alike but for
// id and status, two user messages saying "yes", are both listed when
// one is quarantined, so the record leaves out one the request sent,
// and the requests after it go unhashed until a config replaces the
// omit; nothing quarantined is ever recorded as sent.
func (w *writer) quarantineOver(base agentsession.Omit, quarantined openresponses.Items, model string) agentsession.Omit {
	if len(quarantined) == 0 {
		return base
	}
	set, err := agentturn.NewQuarantineSet(quarantined...)
	if err != nil {
		return base
	}
	held := map[openresponses.Item]bool{}
	for _, item := range set.Bind(agentturn.Transcript(w.values)).Items() {
		held[item] = true
	}
	if len(held) == 0 {
		return base
	}
	calls := map[string]bool{}
	for item := range held {
		if c, ok := item.(*openresponses.FunctionCall); ok {
			calls[c.CallID] = true
		}
	}
	listed := idSet(base.Items)
	want := agentsession.Omit{Reasoning: base.Reasoning, Items: slices.Clone(base.Items), Outputs: base.Outputs, Images: base.Images}
	var outs []string
	for i, v := range w.values {
		if w.custom[i] || w.meta[i].folded || listed[w.items[i]] || w.leftOut(i, model, base) {
			continue
		}
		out, isOutput := v.(*openresponses.FunctionCallOutput)
		switch {
		case isOutput && calls[out.CallID]:
			want.Items = append(want.Items, w.items[i])
		case isOutput && held[v]:
			if base.Outputs != nil {
				if _, ok := base.Outputs.TextFor(w.items[i]); ok {
					continue
				}
			}
			outs = append(outs, w.items[i])
		case held[v]:
			want.Items = append(want.Items, w.items[i])
		}
	}
	if len(outs) > 0 {
		want.Outputs = withOutputs(base.Outputs, []string{agentturn.QuarantinedOutput}, map[string][]string{agentturn.QuarantinedOutput: outs})
	}
	return want
}

// recordQuarantine writes the quarantine in force, as a run's end or a
// refused request carries it, when the omit in force does not already
// say it: a config entry carrying the omit delta alone, which changes
// no setting, the form settle writes when only what is left out moved.
// It follows the last request's entries, so no response is hashed
// under it, and the next request is (RFC 0001 The record).
func (w *writer) recordQuarantine(ctx context.Context, quarantined openresponses.Items) error {
	if len(quarantined) == 0 || !w.wroteConfig {
		return nil
	}
	want := w.quarantineOver(w.settings.Omit, quarantined, w.settings.Model)
	omit, ok := w.settings.OmitDelta(want)
	if !ok || omit == nil {
		return nil
	}
	entry := &agentsession.ConfigEntry{Omit: omit}
	id, err := w.append(ctx, entry)
	if err != nil {
		return err
	}
	entry.ID = id
	w.settings = w.settings.Apply(entry)
	w.forgetLeftOut()
	return nil
}
