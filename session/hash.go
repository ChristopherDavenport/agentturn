package session

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf16"

	"github.com/ChristopherDavenport/openresponses"
)

// HashPrefix precedes the hexadecimal digest in a request hash.
const HashPrefix = "sha256:"

// RequestHash computes the request hash the session format defines: the
// request in RFC 8785 canonical JSON, digested with SHA-256, rendered as
// "sha256:" plus lowercase hex. It is this module's own implementation,
// tested against the golden vectors agentsession publishes, so the two
// libraries agree by specification rather than by sharing code.
func RequestHash(req openresponses.Request) (string, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("session: encode request: %w", err)
	}
	return HashJSON(data)
}

// HashJSON computes the request hash of an already-encoded request.
// Member order and whitespace do not matter. Numbers are parsed as
// float64 on the way, as RFC 8785 requires, so an integer above 2^53
// hashes as its nearest double.
func HashJSON(data []byte) (string, error) {
	canonical, err := canonicalJSON(data)
	if err != nil {
		return "", fmt.Errorf("session: canonicalize request: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return HashPrefix + hex.EncodeToString(sum[:]), nil
}

// canonicalJSON returns the RFC 8785 form of a JSON document: members
// sorted by UTF-16 code units, no whitespace, minimal string escapes and
// numbers as ECMAScript renders them. It is unrelated to [Canonical],
// which strips transport members from a request.
func canonicalJSON(data []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, errors.New("trailing data after the document")
	}
	var buf bytes.Buffer
	if err := writeCanonical(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeCanonical(buf *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		buf.WriteString(strconv.FormatBool(x))
	case json.Number:
		f, err := strconv.ParseFloat(string(x), 64)
		if err != nil {
			return fmt.Errorf("number %q: %w", x, err)
		}
		s, err := formatNumber(f)
		if err != nil {
			return err
		}
		buf.WriteString(s)
	case string:
		writeCanonicalString(buf, x)
	case []any:
		buf.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonical(buf, e); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		keys := sortedKeys(x)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			writeCanonicalString(buf, k)
			buf.WriteByte(':')
			if err := writeCanonical(buf, x[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		return fmt.Errorf("unexpected value of type %T", v)
	}
	return nil
}

// sortedKeys returns the member names ordered by UTF-16 code units
// (RFC 8785 section 3.2.3), encoding each name once.
func sortedKeys(m map[string]any) []string {
	type key struct {
		s string
		u []uint16
	}
	keys := make([]key, 0, len(m))
	for k := range m {
		keys = append(keys, key{s: k, u: utf16.Encode([]rune(k))})
	}
	slices.SortFunc(keys, func(a, b key) int { return slices.Compare(a.u, b.u) })
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = k.s
	}
	return out
}

// writeCanonicalString applies the escapes of RFC 8785 section 3.2.2.2:
// the short escapes for the characters that have them, \u00xx for other
// control characters, everything else literally.
func writeCanonicalString(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	for _, r := range s {
		switch r {
		case '\b':
			buf.WriteString(`\b`)
		case '\t':
			buf.WriteString(`\t`)
		case '\n':
			buf.WriteString(`\n`)
		case '\f':
			buf.WriteString(`\f`)
		case '\r':
			buf.WriteString(`\r`)
		case '"':
			buf.WriteString(`\"`)
		case '\\':
			buf.WriteString(`\\`)
		default:
			if r < 0x20 {
				fmt.Fprintf(buf, `\u%04x`, r)
			} else {
				buf.WriteRune(r)
			}
		}
	}
	buf.WriteByte('"')
}

// formatNumber renders f as ECMAScript Number.prototype.toString does,
// the form RFC 8785 requires: the shortest round-tripping digits, plain
// notation for decimal exponents in [-6, 21) and exponential notation
// outside it. Negative zero is "0".
func formatNumber(f float64) (string, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "", errors.New("NaN and infinity are not representable")
	}
	if f == 0 {
		return "0", nil
	}
	var sb strings.Builder
	if f < 0 {
		sb.WriteByte('-')
		f = -f
	}
	mant, expStr, _ := strings.Cut(strconv.FormatFloat(f, 'e', -1, 64), "e")
	exp, err := strconv.Atoi(expStr)
	if err != nil {
		return "", err
	}
	digits := strings.Replace(mant, ".", "", 1)
	k := len(digits)
	n := exp + 1
	switch {
	case k <= n && n <= 21:
		sb.WriteString(digits)
		sb.WriteString(strings.Repeat("0", n-k))
	case 0 < n && n <= 21:
		sb.WriteString(digits[:n])
		sb.WriteByte('.')
		sb.WriteString(digits[n:])
	case -6 < n && n <= 0:
		sb.WriteString("0.")
		sb.WriteString(strings.Repeat("0", -n))
		sb.WriteString(digits)
	default:
		sb.WriteByte(digits[0])
		if k > 1 {
			sb.WriteByte('.')
			sb.WriteString(digits[1:])
		}
		sb.WriteByte('e')
		if n-1 >= 0 {
			sb.WriteByte('+')
		}
		sb.WriteString(strconv.Itoa(n - 1))
	}
	return sb.String(), nil
}
