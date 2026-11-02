package session

import (
	"context"
	"encoding/base64"
	"fmt"
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

// sidecarForm returns a copy of item with each inline image or file
// among its parts, a base64 data URL, replaced by the sidecar:
// reference to its bytes, and those bytes, in order; item itself and no
// bytes when it holds none.
func sidecarForm(item openresponses.Item) (openresponses.Item, [][]byte) {
	parts := contentOf(item)
	var out openresponses.Contents
	var blobs [][]byte
	for k, p := range parts {
		var conv openresponses.Content
		switch v := p.(type) {
		case *openresponses.InputImage:
			if data, ok := dataURLBytes(v.ImageURL); ok {
				cp := *v
				cp.ImageURL = sidecarScheme + agentsession.HashBytes(data)
				conv, blobs = &cp, append(blobs, data)
			}
		case *openresponses.InputFile:
			if data, ok := dataURLBytes(v.FileData); ok {
				cp := *v
				cp.FileData = sidecarScheme + agentsession.HashBytes(data)
				conv, blobs = &cp, append(blobs, data)
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
		return item, nil
	}
	switch v := item.(type) {
	case *openresponses.Message:
		cp := *v
		cp.Content = out
		return &cp, blobs
	case *openresponses.FunctionCallOutput:
		cp := *v
		cp.Output = openresponses.FunctionCallOutputData{Parts: out}
		return &cp, blobs
	}
	return item, nil
}

// dataURLBytes returns the bytes a base64 data URL carries, and false
// for anything else.
func dataURLBytes(s string) ([]byte, bool) {
	rest, ok := strings.CutPrefix(s, "data:")
	if !ok {
		return nil, false
	}
	meta, payload, ok := strings.Cut(rest, ",")
	if !ok || !strings.HasSuffix(meta, ";base64") {
		return nil, false
	}
	data, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, false
	}
	return data, true
}
