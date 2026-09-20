package session

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
		got, err := canonicalJSON([]byte(tc.in))
		if err != nil {
			t.Errorf("%s: %v", tc.in, err)
			continue
		}
		if string(got) != tc.want {
			t.Errorf("%s: got %s, want %s", tc.in, got, tc.want)
		}
	}
	if _, err := canonicalJSON([]byte(`{} {}`)); err == nil {
		t.Error("trailing data accepted")
	}
	if _, err := canonicalJSON([]byte(`{"n":1e999}`)); err == nil {
		t.Error("overflowing number accepted")
	}
}

// TestFormatNumberRFC8785 checks the number renderings against the
// values RFC 8785 Appendix B lists, one IEEE 754 bit pattern each, so a
// regression inside one branch of the formatter is localised.
func TestFormatNumberRFC8785(t *testing.T) {
	cases := []struct {
		bits uint64
		want string
	}{
		{0x0000000000000000, "0"},
		{0x8000000000000000, "0"},
		{0x0000000000000001, "5e-324"},
		{0x7fefffffffffffff, "1.7976931348623157e+308"},
		{0x4340000000000000, "9007199254740992"},
		{0x4430000000000000, "295147905179352830000"},
		{0x44b52d02c7e14af5, "9.999999999999997e+22"},
		{0x44b52d02c7e14af6, "1e+23"},
		{0x44b52d02c7e14af7, "1.0000000000000001e+23"},
		{0x444b1ae4d6e2ef4e, "999999999999999700000"},
		{0x444b1ae4d6e2ef4f, "999999999999999900000"},
		{0x444b1ae4d6e2ef50, "1e+21"},
		{0x3eb0c6f7a0b5ed8c, "9.999999999999997e-7"},
		{0x3eb0c6f7a0b5ed8d, "0.000001"},
		{0x41b3de4355555553, "333333333.3333332"},
		{0x41b3de4355555554, "333333333.33333325"},
		{0x41b3de4355555555, "333333333.3333333"},
		{0x41b3de4355555556, "333333333.3333334"},
		{0x41b3de4355555557, "333333333.33333343"},
		{0xbecbf647612f3696, "-0.0000033333333333333333"},
		{0x43143ff3c1cb0959, "1424953923781206.2"},
		{0x3ff0000000000000, "1"},
		{0xbff0000000000000, "-1"},
		{0x3fb999999999999a, "0.1"},
		{0x412e848000000000, "1000000"},
		{0x4415af1d78b58c40, "100000000000000000000"},
		{0x3ddb7cdfd9d7bdbb, "1e-10"},
		{0x3e112e0be826d695, "1e-9"},
		{0x3e45798ee2308c3a, "1e-8"},
		{0x3eb0c6f7a0b5ed8d, "0.000001"},
		{0x3e7ad7f29abcaf48, "1e-7"},
		{math.Float64bits(1.5e-10), "1.5e-10"},
		{math.Float64bits(-1.5e-7), "-1.5e-7"},
		{math.Float64bits(123456789.125), "123456789.125"},
	}
	for _, tc := range cases {
		got, err := formatNumber(math.Float64frombits(tc.bits))
		if err != nil || got != tc.want {
			t.Errorf("%#x: got %q err=%v, want %q", tc.bits, got, err, tc.want)
		}
	}
	for _, bits := range []uint64{0x7fffffffffffffff, 0x7ff0000000000000, 0xfff0000000000000} {
		if _, err := formatNumber(math.Float64frombits(bits)); err == nil {
			t.Errorf("%#x: NaN or infinity accepted", bits)
		}
	}
}

// FuzzFormatNumber checks that every rendering parses back to the
// float it came from, which is what the shortest-digits rule promises.
func FuzzFormatNumber(f *testing.F) {
	for _, seed := range []float64{0, 1, -1, 0.1, 1e21, 1e-7, 5e-324, math.MaxFloat64, 123456789.123456789} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, v float64) {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return
		}
		s, err := formatNumber(v)
		if err != nil {
			t.Fatal(err)
		}
		back, err := strconv.ParseFloat(s, 64)
		if err != nil {
			t.Fatalf("%q does not parse: %v", s, err)
		}
		if back != v && !(v == 0 && back == 0) {
			t.Fatalf("%v rendered as %q which parses to %v", v, s, back)
		}
		if strings.Contains(s, "E") || strings.HasSuffix(s, ".") || strings.Contains(s, ".e") {
			t.Fatalf("%v rendered as %q", v, s)
		}
	})
}
