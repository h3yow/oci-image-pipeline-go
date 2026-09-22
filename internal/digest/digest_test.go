package digest

import (
	"errors"
	"testing"
)

func TestParseAcceptanceAndRejection(t *testing.T) {
	good := FromBytes([]byte("hello"))
	cases := []struct {
		name string
		in   string
		ok   bool
	}{
		{"canonical sha256", string(good), true},
		{"empty", "", false},
		{"missing separator", "deadbeef", false},
		{"unsupported algorithm", "sha512:" + repeat("a", 128), false},
		{"wrong hex length", "sha256:" + repeat("a", 63), false},
		{"non hex", "sha256:" + repeat("z", 64), false},
		{"uppercase hex rejected", "sha256:" + upper(repeat("a", 64)), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("input=%q wantOk=%v", tc.in, tc.ok)
			d, err := Parse(tc.in)
			if tc.ok {
				if err != nil {
					t.Fatalf("expected accept, got %v (判定: err=%v)", err, err)
				}
				if d.Algorithm() != AlgorithmSHA256 || len(d.Hex()) != hexLen {
					t.Fatalf("parsed %q malformed", d)
				}
			} else {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("expected ErrInvalid, got %v (判定依据: errors.Is)", err)
				}
			}
		})
	}
}

func TestFromBytesDeterministicAndDistinct(t *testing.T) {
	inputs := [][]byte{[]byte("same"), []byte("same"), []byte("different")}
	a := FromBytes(inputs[0])
	b := FromBytes(inputs[1])
	c := FromBytes(inputs[2])
	t.Logf("输入 %q 与 %q -> %s; 输入 %q -> %s", inputs[0], inputs[1], a, inputs[2], c)
	if a != b {
		t.Fatalf("相同内容摘要不一致: %s != %s", a, b)
	}
	if a == c {
		t.Fatalf("不同内容摘要未被区分: %s", a)
	}
	if err := a.VerifyBytes(inputs[0]); err != nil {
		t.Fatalf("自校验失败: %v", err)
	}
	if err := a.VerifyBytes(inputs[2]); !errors.Is(err, ErrMismatch) {
		t.Fatalf("内容被篡改应 ErrMismatch,  got %v (判定依据: errors.Is ErrMismatch)", err)
	}
}

func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}

func upper(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'a' && b[i] <= 'z' {
			b[i] -= 32
		}
	}
	return string(b)
}
