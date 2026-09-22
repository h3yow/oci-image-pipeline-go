package digest

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestParseAcceptsOnlyCanonicalSHA256(t *testing.T) {
	cases := []struct {
		in      string
		wantErr error
	}{
		{"sha256:0000000000000000000000000000000000000000000000000000000000000000", nil},
		{"sha256:" + strings.Repeat("a", 64), nil},
		{"sha512:" + strings.Repeat("a", 128), ErrUnsupportedAlgorithm},
		{"sha256:" + strings.Repeat("A", 64), ErrInvalidEncoding}, // 拒绝大写
		{"sha256:" + strings.Repeat("a", 63), ErrInvalidEncoding}, // 长度不足
		{"sha256:zz" + strings.Repeat("a", 62), ErrInvalidEncoding},
		{"noseparator", ErrInvalidEncoding},
		{"", ErrInvalidEncoding},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			t.Logf("input=%q wantErr=%v", tc.in, tc.wantErr)
			d, err := Parse(tc.in)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("expected accept, got %v", err)
				}
				if got := d.String(); got != tc.in {
					t.Fatalf("round trip mismatch: got %q want %q", got, tc.in)
				}
			} else {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("want %v, got %v", tc.wantErr, err)
				}
				t.Logf("rejected as %v: %v", tc.wantErr, err)
			}
		})
	}
}

func TestFromBytesStableAndEqual(t *testing.T) {
	content := []byte("deterministic-content")
	d1 := FromBytes(content)
	d2 := FromBytes(bytes.Clone(content))
	t.Logf("content=%q digest=%s", content, d1)
	if !d1.Equal(d2) {
		t.Fatalf("same content produced different digests: %s vs %s", d1, d2)
	}
	if d3 := FromBytes(append(bytes.Clone(content), 'x')); d1.Equal(d3) {
		t.Fatal("different content produced same digest")
	}
}

func TestVerifyDistinguishesMismatch(t *testing.T) {
	d := FromBytes([]byte("original"))
	if err := d.VerifyBytes([]byte("original")); err != nil {
		t.Fatalf("matching content rejected: %v", err)
	}
	err := d.VerifyBytes([]byte("tampered"))
	t.Logf("verifying tampered bytes against %s -> %v", d, err)
	if !errors.Is(err, ErrMismatch) {
		t.Fatalf("want ErrMismatch, got %v", err)
	}
	var zero Digest
	if err := zero.VerifyBytes([]byte("x")); !errors.Is(err, ErrEmpty) {
		t.Fatalf("zero digest want ErrEmpty, got %v", err)
	}
}

func TestJSONTextRoundTrip(t *testing.T) {
	d := FromBytes([]byte(`{"x":1}`))
	b, err := d.MarshalText()
	if err != nil {
		t.Fatal(err)
	}
	var back Digest
	if err := back.UnmarshalText(b); err != nil {
		t.Fatal(err)
	}
	if !back.Equal(d) {
		t.Fatalf("round trip %s -> %s", d, back)
	}
	if err := back.UnmarshalText([]byte("sha1:xx")); !errors.Is(err, ErrUnsupportedAlgorithm) {
		t.Fatalf("want unsupported, got %v", err)
	}
}
