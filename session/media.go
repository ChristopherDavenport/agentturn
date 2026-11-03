package session

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/openresponses"
)

// sidecarScheme precedes a blob's hash in the reference a sidecar
// session's items carry in place of inline media.
const sidecarScheme = "sidecar:"

// WithMedia gives the recorder the blob writer a sidecar session's media
// is stored with. Without it the recorder uses its store, when the store
// is an agentsession.BlobWriter, as every agentsession store is; a store
// that cannot hold blobs refuses to create a sidecar session at all.
// The option is for a host that keeps blobs somewhere its store does
// not reach. It changes nothing for an inline session.
func WithMedia(bw agentsession.BlobWriter) Option {
	return func(r *Recorder) { r.blobs = bw }
}

// blobWriter is the writer the recorder stores blobs with, or nil.
func (r *Recorder) blobWriter() agentsession.BlobWriter {
	if r.blobs != nil {
		return r.blobs
	}
	bw, _ := r.store.(agentsession.BlobWriter)
	return bw
}

// blobReader is the reader a sidecar session's blobs are read back
// through: the writer [WithMedia] names when it reads too, its bytes
// with the type the store holds for the session, else the store when
// it is an agentsession.BlobReader, as every agentsession store is; nil
// when neither reads.
func (r *Recorder) blobReader() agentsession.BlobReader {
	if br, ok := r.blobs.(agentsession.BlobReader); ok {
		typer, _ := r.store.(agentsession.SessionBlobTyper)
		return mediaReader{br: br, types: typer}
	}
	br, _ := r.store.(agentsession.BlobReader)
	return br
}

// mediaReader reads the blobs a [WithMedia] writer keeps apart from the
// store as a session holds them: the bytes, and the type, from the
// writer, and the session's own type from the store, which holds the
// session, when it holds one. The writer is not asked for the session,
// which it need not hold: an RFC 0003 server answers a session it does
// not hold with no blob.
type mediaReader struct {
	br    agentsession.BlobReader
	types agentsession.SessionBlobTyper
}

// Blob implements agentsession.BlobReader.
func (m mediaReader) Blob(ctx context.Context, hash string) ([]byte, error) {
	return m.br.Blob(ctx, hash)
}

// SessionBlob implements agentsession.SessionBlobReader.
func (m mediaReader) SessionBlob(ctx context.Context, session, hash string) ([]byte, string, error) {
	var data []byte
	var mt string
	var err error
	if tr, ok := m.br.(agentsession.TypedBlobReader); ok {
		data, mt, err = tr.TypedBlob(ctx, hash)
	} else {
		data, err = m.br.Blob(ctx, hash)
	}
	if err != nil {
		return nil, "", err
	}
	if m.types != nil {
		held, err := m.types.SessionBlobTypes(ctx, session)
		if err != nil && !errors.Is(err, agentsession.ErrNoSession) {
			return nil, "", fmt.Errorf("session: blob types: %w", err)
		}
		if t, ok := held[hash]; ok {
			mt = t
		}
	}
	return data, mt, nil
}

// blob is one inline medium a sidecar session stores: its bytes, and
// the media type its data URL carried, "" for none.
type blob struct {
	data      []byte
	mediaType string
}

// sidecar returns item as the writer's session records it: in a sidecar
// session, with each inline image or file among its parts stored as a
// blob, before the entry that names it, and named by its sidecar:
// reference; item itself in an inline session and for an item with no
// inline media. The item the loop holds is not changed (#284). A blob
// is stored with the media type of the data URL it replaced, through
// agentsession.TypedBlobWriter when the writer is one, and the session
// is given that type for it when the store holds types per session
// (agentsession.SessionBlobTyper), or else the blob is put again the
// first time the session names it, so a resume reads each reference
// back as the data URL it was.
func (w *writer) sidecar(ctx context.Context, item openresponses.Item) (openresponses.Item, error) {
	if w.media != agentsession.MediaSidecar || item == nil {
		return item, nil
	}
	conv, blobs := sidecarForm(item)
	if len(blobs) == 0 {
		return item, nil
	}
	bw := w.rec.blobWriter()
	if bw == nil {
		return nil, fmt.Errorf("session: media of a sidecar session: %w", agentsession.ErrNoBlobWriter)
	}
	// A store that holds no types per session, an RFC 0003 client, gives
	// a session the type of a blob its writer uploaded, and lets the
	// session reach the blob, only while the upload's grace lasts: a
	// session that has not named the blob yet, a child naming its
	// parent's, has it put again for it.
	typer, perSession := w.rec.store.(agentsession.SessionBlobTyper)
	var types map[string]string
	for _, b := range blobs {
		hash := agentsession.HashBytes(b.data)
		_, named := types[hash]
		if !w.rec.stored[hash] || !perSession && !w.typed[hash] && !named {
			var got string
			var err error
			if tw, ok := bw.(agentsession.TypedBlobWriter); ok {
				got, err = tw.PutTypedBlob(ctx, b.data, b.mediaType)
			} else {
				got, err = bw.PutBlob(ctx, b.data)
			}
			if err != nil {
				return nil, fmt.Errorf("session: store blob: %w", err)
			}
			if got != hash {
				return nil, fmt.Errorf("session: store blob: stored as %s, its bytes hash to %s", got, hash)
			}
			w.rec.stored[hash] = true
		}
		if !w.typed[hash] && !named {
			if types == nil {
				types = map[string]string{}
			}
			types[hash] = b.mediaType
		}
	}
	// The session's own type for each blob, held by the store that
	// holds the session, the blobs its own or a [WithMedia] writer's: a
	// type a store holds per blob is the first it was given, which
	// another session may have given these bytes.
	if perSession && len(types) > 0 {
		if err := typer.SetSessionBlobTypes(ctx, w.id, types); err != nil {
			return nil, fmt.Errorf("session: blob types: %w", err)
		}
	}
	for hash := range types {
		if w.typed == nil {
			w.typed = map[string]bool{}
		}
		w.typed[hash] = true
	}
	return conv, nil
}

// recordForm is item as the writer's session records it, as [sidecar]
// returns it, with no blob stored: what a request's item is compared
// with, and hashed as, in a sidecar session.
func (w *writer) recordForm(item openresponses.Item) openresponses.Item {
	if w.media != agentsession.MediaSidecar || item == nil {
		return item
	}
	conv, _ := sidecarForm(item)
	return conv
}

// mediaKeys are the members that carry a medium by URL or by data in
// an item's JSON: an image's, a video's and a file's.
var mediaKeys = map[string]bool{"image_url": true, "video_url": true, "file_url": true, "file_data": true}

// sidecarForm returns a copy of item with each inline medium it carries,
// a data URL in an image, video or file part or in any member those
// names give an item type the library does not model, replaced by the
// sidecar: reference to its bytes, and those bytes with their types, in
// order; item itself and no blobs when it carries none. A data URL
// whose type is not one a store keeps stays inline.
func sidecarForm(item openresponses.Item) (openresponses.Item, []blob) {
	var blobs []blob
	conv, changed, _ := mapMedia(item, "data:", func(url string) (string, bool, error) {
		b, ok := inlineBlob(url)
		if !ok {
			return url, false, nil
		}
		blobs = append(blobs, b)
		return sidecarScheme + agentsession.HashBytes(b.data), true, nil
	})
	if !changed {
		return item, nil
	}
	return conv, blobs
}

// mapMedia returns a copy of item with f applied to each medium it
// carries, the URL or data of an image, video or file part of a message
// or a function call output, or of any member [mediaKeys] names at any
// depth of an item or part type the library does not model, which is
// read when its JSON holds marker; item itself and false when f
// changed none. f returns the value to carry and whether it differs.
func mapMedia(item openresponses.Item, marker string, f func(string) (string, bool, error)) (openresponses.Item, bool, error) {
	switch v := item.(type) {
	case *openresponses.Message:
		parts, changed, err := mapParts(v.Content, marker, f)
		if err != nil || !changed {
			return item, false, err
		}
		cp := *v
		cp.Content = parts
		return &cp, true, nil
	case *openresponses.FunctionCallOutput:
		if v.Output.Parts == nil {
			return item, false, nil
		}
		parts, changed, err := mapParts(v.Output.Parts, marker, f)
		if err != nil || !changed {
			return item, false, err
		}
		cp := *v
		cp.Output = openresponses.FunctionCallOutputData{Parts: parts}
		return &cp, true, nil
	}
	// Any other item, a provider's computer_call_output among them, by
	// its JSON: the members that carry media are named alike.
	data, err := json.Marshal(item)
	if err != nil || !bytes.Contains(data, []byte(`"`+marker)) {
		return item, false, nil
	}
	raw, changed, err := mapJSON(data, marker, f)
	if err != nil || !changed {
		return item, false, err
	}
	conv, err := openresponses.UnmarshalItem(raw)
	if err != nil {
		return item, false, nil
	}
	return conv, true, nil
}

// mapParts is parts with f applied to each medium, as mapMedia does,
// and false when f changed none.
func mapParts(parts openresponses.Contents, marker string, f func(string) (string, bool, error)) (openresponses.Contents, bool, error) {
	var out openresponses.Contents
	for k, p := range parts {
		var conv openresponses.Content
		switch v := p.(type) {
		case *openresponses.InputImage:
			u, ok, err := f(v.ImageURL)
			if err != nil {
				return parts, false, err
			}
			if ok {
				cp := *v
				cp.ImageURL = u
				conv = &cp
			}
		case *openresponses.InputVideo:
			u, ok, err := f(v.VideoURL)
			if err != nil {
				return parts, false, err
			}
			if ok {
				cp := *v
				cp.VideoURL = u
				conv = &cp
			}
		case *openresponses.InputFile:
			cp := *v
			d, okData, err := f(v.FileData)
			if err != nil {
				return parts, false, err
			}
			u, okURL, err := f(v.FileURL)
			if err != nil {
				return parts, false, err
			}
			if okData || okURL {
				cp.FileData, cp.FileURL = d, u
				conv = &cp
			}
		case *openresponses.UnknownContent:
			raw, ok, err := mapJSON(v.Raw, marker, f)
			if err != nil {
				return parts, false, err
			}
			if ok {
				conv = &openresponses.UnknownContent{Type: v.Type, Raw: raw}
			}
		}
		if conv == nil {
			continue
		}
		if out == nil {
			out = slices.Clone(parts)
		}
		out[k] = conv
	}
	if out == nil {
		return parts, false, nil
	}
	return out, true, nil
}

// mapJSON is a JSON value with f applied to each string a media member
// holds, at any depth, in document order, and false when f changed
// none or the value is not one marker is found in.
func mapJSON(raw []byte, marker string, f func(string) (string, bool, error)) ([]byte, bool, error) {
	if !bytes.Contains(raw, []byte(`"`+marker)) {
		return raw, false, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return raw, false, nil
	}
	changed := false
	var walk func(any) error
	walk = func(v any) error {
		switch x := v.(type) {
		case map[string]any:
			for _, k := range sortedKeys(x) {
				if s, ok := x[k].(string); ok && mediaKeys[k] {
					u, ok, err := f(s)
					if err != nil {
						return err
					}
					if ok {
						x[k], changed = u, true
					}
					continue
				}
				if err := walk(x[k]); err != nil {
					return err
				}
			}
		case []any:
			for _, e := range x {
				if err := walk(e); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(v); err != nil {
		return raw, false, err
	}
	if !changed {
		return raw, false, nil
	}
	out, err := json.Marshal(v)
	if err != nil {
		return raw, false, nil
	}
	return out, true, nil
}

// inlineBlob returns the medium a data URL carries, base64 or
// percent-encoded, with its media type as a store keeps it: the type
// before ";base64", "text/plain;charset=US-ASCII" when it names none,
// as RFC 2397 has it, and "" for application/octet-stream. It reports
// false for anything else, and for a data URL whose type a store
// refuses, which stays inline, as agentsession RFC 0002 has an item
// that needs a type of its own do.
func inlineBlob(s string) (blob, bool) {
	rest, ok := strings.CutPrefix(s, "data:")
	if !ok {
		return blob{}, false
	}
	meta, payload, ok := strings.Cut(rest, ",")
	if !ok {
		return blob{}, false
	}
	var data []byte
	if mt, ok := strings.CutSuffix(meta, ";base64"); ok {
		d, err := base64.StdEncoding.DecodeString(payload)
		if err != nil {
			return blob{}, false
		}
		data, meta = d, mt
	} else {
		d, err := url.PathUnescape(payload)
		if err != nil {
			return blob{}, false
		}
		data = []byte(d)
	}
	switch {
	case meta == "":
		meta = "text/plain;charset=US-ASCII"
	case strings.HasPrefix(meta, ";"):
		meta = "text/plain" + meta
	}
	mt, err := agentsession.BlobMediaType(meta)
	if err != nil {
		return blob{}, false
	}
	return blob{data: data, mediaType: mt}, true
}

// ErrNoBlobReader is returned when a sidecar session's items are read
// to hand to an agent, by [AgentOptions], [Transcript] and the rest, and
// one names a blob with no blob reader given to resolve it: an agent
// handed a sidecar: URL would send it to a provider, which cannot
// resolve it. [WithBlobs], or [Recorder.ReadOptions], gives one.
var ErrNoBlobReader = errors.New("session: a sidecar: reference and no blob reader to resolve it")

// WithBlobs has [AgentOptions], [Transcript], [TranscriptModels],
// [Pending], [OwedNotes] and [ReplayAnswers] read a sidecar session's
// blobs through br, resolving each sidecar: reference among the items
// they hand back to a base64 data URL of the blob's media type, as the
// session holds it ([agentsession.ReadSessionBlob]), and of
// application/octet-stream for a blob held with none, so no sidecar:
// URL reaches the agent or a provider. Without it they refuse a
// sidecar session's item that names a blob, with [ErrNoBlobReader].
// [Recorder.ReadOptions] carries the recorder's reader.
func WithBlobs(br agentsession.BlobReader) ReadOption {
	return ReadOption{apply: func(o *origins) { o.blobs = br }}
}

// mediaResolver resolves the sidecar: references of a sidecar
// session's items to data URLs, each blob read once.
type mediaResolver struct {
	ctx  context.Context
	br   agentsession.BlobReader
	urls map[string]string
}

// resolverFor is the resolver for s read with opts, nil for a session
// whose media is inline, whose items name no blob.
func resolverFor(s *agentsession.Session, opts []ReadOption) *mediaResolver {
	if s == nil || s.Header().Media != agentsession.MediaSidecar {
		return nil
	}
	o := &origins{ctx: context.Background()}
	for _, opt := range opts {
		if opt.apply != nil {
			opt.apply(o)
		}
	}
	return &mediaResolver{ctx: o.ctx, br: o.blobs, urls: map[string]string{}}
}

// item is item with each sidecar: reference it carries resolved as
// session holds the blob; item itself when it carries none, and for a
// nil resolver.
func (m *mediaResolver) item(session string, item openresponses.Item) (openresponses.Item, error) {
	if m == nil || item == nil {
		return item, nil
	}
	conv, _, err := mapMedia(item, sidecarScheme, func(ref string) (string, bool, error) {
		hash, ok := strings.CutPrefix(ref, sidecarScheme)
		if !ok {
			return ref, false, nil
		}
		u, err := m.dataURL(session, hash)
		return u, err == nil, err
	})
	return conv, err
}

// items resolves each of items in place, as item does.
func (m *mediaResolver) items(session string, items openresponses.Items) error {
	for i, item := range items {
		conv, err := m.item(session, item)
		if err != nil {
			return err
		}
		items[i] = conv
	}
	return nil
}

// dataURL is the data URL the blob hash resolves to as session holds it.
func (m *mediaResolver) dataURL(session, hash string) (string, error) {
	key := session + "\x00" + hash
	if u, ok := m.urls[key]; ok {
		return u, nil
	}
	if m.br == nil {
		return "", fmt.Errorf("%w: %s%s in session %s", ErrNoBlobReader, sidecarScheme, hash, session)
	}
	data, mt, err := agentsession.ReadSessionBlob(m.ctx, m.br, session, hash)
	if err != nil {
		return "", fmt.Errorf("session: blob %s of session %s: %w", hash, session, err)
	}
	if got := agentsession.HashBytes(data); got != hash {
		return "", fmt.Errorf("session: blob %s of session %s hashes to %s: %w", hash, session, got, agentsession.ErrBlobMismatch)
	}
	if mt, err = agentsession.BlobMediaType(mt); err != nil {
		return "", fmt.Errorf("session: blob %s of session %s: %w", hash, session, err)
	}
	if mt == "" {
		mt = agentsession.DefaultMediaType
	}
	u := "data:" + mt + ";base64," + base64.StdEncoding.EncodeToString(data)
	m.urls[key] = u
	return u, nil
}
