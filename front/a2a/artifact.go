package a2a

import (
	"context"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv/eventqueue"
)

// DefaultChunkSize is the number of bytes of assistant text the artifact
// writer collects before sending a chunk.
const DefaultChunkSize = 256

// artifactWriter streams one assistant message as one artifact,
// coalescing text deltas into chunks: the first chunk creates the
// artifact, later ones append, and close sends the remainder with
// lastChunk set.
type artifactWriter struct {
	queue     eventqueue.Queue
	info      a2a.TaskInfoProvider
	chunkSize int

	id      a2a.ArtifactID
	buf     []byte
	started bool
	total   []byte
}

func newArtifactWriter(q eventqueue.Queue, info a2a.TaskInfoProvider, chunkSize int) *artifactWriter {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &artifactWriter{queue: q, info: info, chunkSize: chunkSize, id: a2a.NewArtifactID()}
}

func (w *artifactWriter) write(ctx context.Context, delta string) error {
	w.buf = append(w.buf, delta...)
	w.total = append(w.total, delta...)
	if len(w.buf) < w.chunkSize {
		return nil
	}
	return w.flush(ctx, false)
}

// close sends what is buffered with lastChunk. A message with no text
// produces no artifact at all.
func (w *artifactWriter) close(ctx context.Context) error {
	if !w.started && len(w.buf) == 0 {
		return nil
	}
	return w.flush(ctx, true)
}

func (w *artifactWriter) flush(ctx context.Context, last bool) error {
	part := a2a.TextPart{Text: string(w.buf)}
	w.buf = w.buf[:0]
	var ev *a2a.TaskArtifactUpdateEvent
	if w.started {
		ev = a2a.NewArtifactUpdateEvent(w.info, w.id, part)
	} else {
		ev = a2a.NewArtifactEvent(w.info, part)
		ev.Artifact.ID = w.id
		ev.Artifact.Name = "response"
		w.started = true
	}
	ev.LastChunk = last
	return w.queue.Write(ctx, ev)
}
