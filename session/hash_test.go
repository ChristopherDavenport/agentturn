package session

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/openresponses"
)

type hashVector struct {
	Name    string          `json:"name"`
	Request json.RawMessage `json:"request"`
	Hash    string          `json:"hash"`
}

// TestRequestHashVectors checks this module's hash against the golden
// vectors agentsession publishes, and against agentsession's own
// implementation, so the two never drift.
func TestRequestHashVectors(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "hash", "vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vectors []hashVector
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	if len(vectors) == 0 {
		t.Fatal("no vectors")
	}
	for _, v := range vectors {
		t.Run(v.Name, func(t *testing.T) {
			var req openresponses.Request
			if err := json.Unmarshal(v.Request, &req); err != nil {
				t.Fatal(err)
			}
			got, err := RequestHash(req)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := HashJSON(v.Request)
			if err != nil {
				t.Fatal(err)
			}
			if got != v.Hash || raw != v.Hash {
				t.Errorf("typed %s raw %s, want %s", got, raw, v.Hash)
			}
			theirs, err := agentsession.RequestHash(req)
			if err != nil {
				t.Fatal(err)
			}
			if theirs != got {
				t.Errorf("agentsession hashes %s, this module %s", theirs, got)
			}
		})
	}
}

func TestCanonicalForm(t *testing.T) {
	cases := []struct{ in, want string }{
		{`{"b":1,"a":2}`, `{"a":2,"b":1}`},
		{`{"a": [1.0, 1e21, 1e-7, 0.000001, -0, 100]}`, `{"a":[1,1e+21,1e-7,0.000001,0,100]}`},
		{`{"s":"é\n\u001f\"\\/"}`, `{"s":"é\n\u001f\"\\/"}`},
		{`{"😀":1,"z":2,"é":3}`, `{"z":2,"é":3,"😀":1}`},
		{`[true,false,null]`, `[true,false,null]`},
	}
	for _, tc := range cases {
		got, err := canonicalize([]byte(tc.in))
		if err != nil {
			t.Errorf("%s: %v", tc.in, err)
			continue
		}
		if string(got) != tc.want {
			t.Errorf("%s: got %s, want %s", tc.in, got, tc.want)
		}
	}
	if _, err := canonicalize([]byte(`{} {}`)); err == nil {
		t.Error("trailing data accepted")
	}
	if _, err := canonicalize([]byte(`{"n":1e999}`)); err == nil {
		t.Error("overflowing number accepted")
	}
}
