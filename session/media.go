package session

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
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

// sidecar returns item as the writer's session records it: in a sidecar
// session, with each inline image or file among its parts stored as a
// blob, before the entry that names it, and named by its sidecar:
// reference; item itself in an inline session and for an item with no
// inline media. The item the loop holds is not changed (#284).
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
	for _, b := range blobs {
		hash := agentsession.HashBytes(b)
		if w.rec.stored[hash] {
			continue
		}
		got, err := bw.PutBlob(ctx, b)
		if err != nil {
			return nil, fmt.Errorf("session: store blob: %w", err)
		}
		if got != hash {
			return nil, fmt.Errorf("session: store blob: stored as %s, its bytes hash to %s", got, hash)
		}
		w.rec.stored[hash] = true
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
// sidecar: reference to its bytes, and those bytes, in order; item
// itself and no bytes when it carries none.
func sidecarForm(item openresponses.Item) (openresponses.Item, [][]byte) {
	switch v := item.(type) {
	case *openresponses.Message:
		parts, blobs := sidecarParts(v.Content)
		if blobs == nil {
			return item, nil
		}
		cp := *v
		cp.Content = parts
		return &cp, blobs
	case *openresponses.FunctionCallOutput:
		if v.Output.Parts == nil {
			return item, nil
		}
		parts, blobs := sidecarParts(v.Output.Parts)
		if blobs == nil {
			return item, nil
		}
		cp := *v
		cp.Output = openresponses.FunctionCallOutputData{Parts: parts}
		return &cp, blobs
	}
	// Any other item, a provider's computer_call_output among them, by
	// its JSON: the members that carry media are named alike.
	data, err := json.Marshal(item)
	if err != nil || !bytes.Contains(data, []byte(`"data:`)) {
		return item, nil
	}
	raw, blobs := sidecarJSON(data)
	if blobs == nil {
		return item, nil
	}
	conv, err := openresponses.UnmarshalItem(raw)
	if err != nil {
		return item, nil
	}
	return conv, blobs
}

// sidecarParts is parts with each inline medium replaced, as
// sidecarForm does, and nil bytes when none was.
func sidecarParts(parts openresponses.Contents) (openresponses.Contents, [][]byte) {
	var out openresponses.Contents
	var blobs [][]byte
	ref := func(url string) (string, bool) {
		data, ok := dataURLBytes(url)
		if !ok {
			return url, false
		}
		blobs = append(blobs, data)
		return sidecarScheme + agentsession.HashBytes(data), true
	}
	for k, p := range parts {
		var conv openresponses.Content
		switch v := p.(type) {
		case *openresponses.InputImage:
			if u, ok := ref(v.ImageURL); ok {
				cp := *v
				cp.ImageURL = u
				conv = &cp
			}
		case *openresponses.InputVideo:
			if u, ok := ref(v.VideoURL); ok {
				cp := *v
				cp.VideoURL = u
				conv = &cp
			}
		case *openresponses.InputFile:
			cp := *v
			d, okData := ref(v.FileData)
			u, okURL := ref(v.FileURL)
			if okData || okURL {
				cp.FileData, cp.FileURL = d, u
				conv = &cp
			}
		case *openresponses.UnknownContent:
			if raw, b := sidecarJSON(v.Raw); b != nil {
				blobs = append(blobs, b...)
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
		return parts, nil
	}
	return out, blobs
}

// sidecarJSON is a JSON value with each data URL a media member holds,
// at any depth, replaced by its sidecar: reference, and the bytes, in
// document order; the value as given and nil when it holds none.
func sidecarJSON(raw []byte) ([]byte, [][]byte) {
	if !bytes.Contains(raw, []byte(`"data:`)) {
		return raw, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return raw, nil
	}
	var blobs [][]byte
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			for _, k := range sortedKeys(x) {
				if s, ok := x[k].(string); ok && mediaKeys[k] {
					if data, ok := dataURLBytes(s); ok {
						x[k] = sidecarScheme + agentsession.HashBytes(data)
						blobs = append(blobs, data)
					}
					continue
				}
				walk(x[k])
			}
		case []any:
			for _, e := range x {
				walk(e)
			}
		}
	}
	walk(v)
	if blobs == nil {
		return raw, nil
	}
	out, err := json.Marshal(v)
	if err != nil {
		return raw, nil
	}
	return out, blobs
}

// dataURLBytes returns the bytes a data URL carries, base64 or
// percent-encoded, and false for anything else.
func dataURLBytes(s string) ([]byte, bool) {
	rest, ok := strings.CutPrefix(s, "data:")
	if !ok {
		return nil, false
	}
	meta, payload, ok := strings.Cut(rest, ",")
	if !ok {
		return nil, false
	}
	if strings.HasSuffix(meta, ";base64") {
		data, err := base64.StdEncoding.DecodeString(payload)
		if err != nil {
			return nil, false
		}
		return data, true
	}
	data, err := url.PathUnescape(payload)
	if err != nil {
		return nil, false
	}
	return []byte(data), true
}
