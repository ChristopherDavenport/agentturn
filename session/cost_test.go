package session

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// countedText is an input_text part that counts how often it is
// encoded.
type countedText struct {
	text string
	n    *atomic.Int64
}

func (countedText) ContentType() string { return openresponses.ContentTypeInputText }

func (c countedText) MarshalJSON() ([]byte, error) {
	c.n.Add(1)
	return json.Marshal(&openresponses.InputText{Text: c.text})
}

// answers answers every request with a message, encoding nothing.
type answers struct{}

func (answers) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	msg, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if err := msg.Text("ok"); err != nil {
		return err
	}
	return em.Complete()
}

// TestRecordingATurnDoesNotEncodeTheTranscriptAgain pins #282: the
// recorder compares and hashes each turn's request without encoding the
// items it recorded at earlier turns again, so a turn costs what its
// settings and its new items cost, not the whole transcript. An item is
// encoded when it is appended and once more when a request first
// carries it; later turns encode it no more.
func TestRecordingATurnDoesNotEncodeTheTranscriptAgain(t *testing.T) {
	ctx := context.Background()
	rec, s, err := Start(ctx, agentsession.NewMemoryStore(), agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: answers{}, ModelName: "m"})
	defer rec.Attach(a)()
	var n atomic.Int64
	big := openresponses.UserMessage(countedText{text: strings.Repeat("x", 1<<16), n: &n})
	if _, err := a.Prompt(ctx, big); err != nil {
		t.Fatal(err)
	}
	after := n.Load()
	for i := range 5 {
		if _, err := a.Prompt(ctx, openresponses.UserText(fmt.Sprint("turn ", i))); err != nil {
			t.Fatal(err)
		}
	}
	if got := n.Load() - after; got != 0 {
		t.Errorf("five later turns encoded the first prompt %d more times, want 0", got)
	}
	verifyAll(t, s)
}

// TestRequestHashOfMatchesRequestHash checks the hash the recorder
// computes from each item's kept canonical form against RequestHash,
// over the golden vectors that carry an input.
func TestRequestHashOfMatchesRequestHash(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "hash", "vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vectors []hashVector
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, v := range vectors {
		var req openresponses.Request
		if err := json.Unmarshal(v.Request, &req); err != nil {
			t.Fatal(err)
		}
		canon := make([][]byte, len(req.Input))
		for i, item := range req.Input {
			c, ok := canonicalValue(item)
			if !ok {
				t.Fatalf("%s: item %d does not encode", v.Name, i)
			}
			canon[i] = c
		}
		got, err := requestHashOf(req, canon)
		if err != nil {
			t.Fatal(err)
		}
		if got != v.Hash {
			t.Errorf("%s: %s, want %s", v.Name, got, v.Hash)
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("no vectors")
	}
}

// BenchmarkRecordingATurn records one turn over a transcript of n large
// items: with each item's canonical form kept, the time per turn stays
// close whatever n is (#282).
func BenchmarkRecordingATurn(b *testing.B) {
	for _, n := range []int{10, 100} {
		b.Run(fmt.Sprintf("items=%d", n), func(b *testing.B) {
			ctx := context.Background()
			rec, _, err := Start(ctx, agentsession.NewMemoryStore(), agentsession.Header{})
			if err != nil {
				b.Fatal(err)
			}
			var seed agentturn.Transcript
			for i := range n {
				seed = append(seed, openresponses.UserText(fmt.Sprintf("%d %s", i, strings.Repeat("x", 1<<15))), openresponses.AssistantText("ok"))
			}
			a := agentturn.New(agentturn.Config{Model: answers{}, ModelName: "m"})
			defer rec.Attach(a)()
			if _, err := a.Prompt(ctx, seed...); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := a.Prompt(ctx, openresponses.UserText("next")); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
