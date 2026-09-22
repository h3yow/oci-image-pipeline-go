package manifest

import (
	"bytes"
	"errors"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/descriptor"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
)

func mustDesc(_ *testing.T, media descriptor.MediaType, content string) descriptor.Descriptor {
	d := digest.FromBytes([]byte(content))
	return descriptor.Descriptor{MediaType: media, Digest: d, Size: int64(len(content))}
}

func sampleManifest() *Manifest {
	cfg := descFor(descriptor.MediaTypeImageConfig, `{"os":"linux"}`)
	layer := descFor(descriptor.MediaTypeLayerTar, "tar-bytes")
	return &Manifest{
		Version:     Version1,
		Config:      &cfg,
		Layers:      []descriptor.Descriptor{layer},
		Annotations: map[string]string{"a": "1"},
	}
}

func TestEncodeV1Deterministic(t *testing.T) {
	m := sampleManifest()
	b1, err := EncodeV1(m)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := EncodeV1(sampleManifest())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("canonical manifest bytes: %s", b1)
	if string(b1) != string(b2) {
		t.Fatalf("non-deterministic encoding:\n%s\n%s", b1, b2)
	}
	// 键序固定为升序：annotations 在 config 之前。
	if !containsInOrder(b1, []byte(`"annotations"`), []byte(`"config"`)) {
		t.Fatalf("keys not sorted ascending: %s", b1)
	}
	d1, d2 := DigestOf(b1), DigestOf(b2)
	if !d1.Equal(d2) {
		t.Fatalf("canonical bytes diverged in digest: %s vs %s", d1, d2)
	}

	// 注解变化必须改变摘要。
	m3 := sampleManifest()
	m3.Annotations = map[string]string{"a": "2"}
	b3, _ := EncodeV1(m3)
	if DigestOf(b3).Equal(d1) {
		t.Fatal("manifest change did not change digest")
	}
}

func descFor(media descriptor.MediaType, content string) descriptor.Descriptor {
	d := digest.FromBytes([]byte(content))
	return descriptor.Descriptor{MediaType: media, Digest: d, Size: int64(len(content))}
}

func containsInOrder(haystack []byte, earlier, later []byte) bool {
	i := indexOf(haystack, earlier)
	j := indexOf(haystack, later)
	return i >= 0 && j >= 0 && i < j
}

func indexOf(h, n []byte) int {
	for i := 0; i+len(n) <= len(h); i++ {
		if string(h[i:i+len(n)]) == string(n) {
			return i
		}
	}
	return -1
}

func TestV0ReadOnlyCompatibility(t *testing.T) {
	// 旧格式：无 config、annotations 缺失。
	v0wire := []byte(`{"manifestVersion":0,"layers":[]}`)
	m, err := Decode(descriptor.MediaTypeLegacyManifest, v0wire)
	if err != nil {
		t.Fatalf("legacy manifest should be readable: %v", err)
	}
	t.Logf("decoded v0: version=%d config=%v annotations=%v",
		m.Version, m.Config, m.Annotations)
	if m.Version != Version0 || m.Config != nil {
		t.Fatalf("unexpected v0 decode: %+v", m)
	}
	if m.Annotations == nil {
		t.Fatal("missing annotations must default to empty map, got nil")
	}
	if _, err := EncodeV1(m); !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("re-encoding v0 must be rejected, got %v", err)
	}

	// mediaType 缺省时按字节内 manifestVersion=0 识别。
	m2, err := Decode("", v0wire)
	if err != nil {
		t.Fatalf("version-sniffed v0 decode failed: %v", err)
	}
	if m2.Version != Version0 {
		t.Fatalf("want v0, got %d", m2.Version)
	}
}

func TestUnsupportedFormatsRejected(t *testing.T) {
	layer := mustDesc(t, descriptor.MediaTypeLayerTar, "x")
	cfg := mustDesc(t, descriptor.MediaTypeImageConfig, "c")

	cases := []struct {
		name  string
		media descriptor.MediaType
		raw   string
		want  error
	}{
		{"unknown version", descriptor.MediaTypeImageManifest,
			`{"manifestVersion":9,"config":null,"layers":[]}`, ErrUnsupportedFormat},
		{"unknown mediaType", "application/vnd.example.bogus",
			`{"manifestVersion":1}`, ErrUnsupportedFormat},
		{"v1 missing config", descriptor.MediaTypeImageManifest,
			`{"manifestVersion":1,"layers":[]}`, ErrMissingField},
		{"v1 mediaType mismatch", descriptor.MediaTypeImageIndex,
			`{"manifestVersion":1,"config":{},"layers":[]}`, ErrUnsupportedFormat},
		{"invalid json", descriptor.MediaTypeImageManifest, `{not json`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decode(tc.media, []byte(tc.raw))
			t.Logf("input media=%q raw=%s -> %v", tc.media, tc.raw, err)
			if err == nil {
				t.Fatal("expected rejection, got nil")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
	_ = layer
	_ = cfg
}

func TestUnknownFieldsSurviveRoundTrip(t *testing.T) {
	cfg := mustDesc(t, descriptor.MediaTypeImageConfig, "cfg")
	layer := mustDesc(t, descriptor.MediaTypeLayerTar, "lay")
	m := &Manifest{Version: Version1, Config: &cfg, Layers: []descriptor.Descriptor{layer}}
	b, err := EncodeV1(m)
	if err != nil {
		t.Fatal(err)
	}
	// 模拟未来版本写入的未知字段。
	withFuture := insertKey(t, b, []byte("futureFeature"), []byte(`"on"`))
	got, err := Decode(descriptor.MediaTypeImageManifest, withFuture)
	if err != nil {
		t.Fatal(err)
	}
	re, err := EncodeV1(got)
	if err != nil {
		t.Fatal(err)
	}
	got2, err := Decode(descriptor.MediaTypeImageManifest, re)
	if err != nil {
		t.Fatal(err)
	}
	re2, err := EncodeV1(got2)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input=%s\ncanon=%s\ncanon2=%s", withFuture, re, re2)
	if string(re) != string(re2) {
		t.Fatalf("canonical re-encode not idempotent:\n%s\n%s", re, re2)
	}
	if !bytes.Contains(re, []byte(`"futureFeature":"on"`)) {
		t.Fatalf("unknown field lost after round trip: %s", re)
	}
}

func insertKey(t *testing.T, obj []byte, key, val []byte) []byte {
	t.Helper()
	// 在首个 { 后插入 "key":val, 构造语法合法、键序任意的输入，
	// 验证规范化后仍能稳定表达。
	out := make([]byte, 0, len(obj)+len(key)+len(val)+4)
	out = append(out, obj[0])
	out = append(out, '"')
	out = append(out, key...)
	out = append(out, '"')
	out = append(out, ':')
	out = append(out, val...)
	out = append(out, ',')
	out = append(out, obj[1:]...)
	return out
}

func TestIndexValidation(t *testing.T) {
	man1 := mustDesc(t, descriptor.MediaTypeImageManifest, "m1")
	man2 := mustDesc(t, descriptor.MediaTypeImageManifest, "m2")

	t.Run("missing platform rejected", func(t *testing.T) {
		idx := &Index{Manifests: []IndexEntry{
			{Descriptor: man1, Platform: nil},
		}}
		_, err := EncodeIndex(idx)
		t.Logf("entry without platform -> %v", err)
		if !errors.Is(err, ErrPlatformMissing) {
			t.Fatalf("want ErrPlatformMissing, got %v", err)
		}
	})

	t.Run("duplicate platform rejected", func(t *testing.T) {
		p := descriptor.Platform{OS: "linux", Architecture: "amd64"}
		idx := &Index{Manifests: []IndexEntry{
			{Descriptor: man1, Platform: &p},
			{Descriptor: man2, Platform: &p},
		}}
		_, err := EncodeIndex(idx)
		t.Logf("duplicate platforms -> %v", err)
		if !errors.Is(err, ErrDuplicatePlatform) {
			t.Fatalf("want ErrDuplicatePlatform, got %v", err)
		}
	})

	t.Run("case normalization sees duplicate", func(t *testing.T) {
		p1 := descriptor.Platform{OS: "LINUX", Architecture: "AMD64"}
		p2 := descriptor.Platform{OS: "linux", Architecture: "amd64"}
		idx := &Index{Manifests: []IndexEntry{
			{Descriptor: man1, Platform: &p1},
			{Descriptor: man2, Platform: &p2},
		}}
		if _, err := EncodeIndex(idx); !errors.Is(err, ErrDuplicatePlatform) {
			t.Fatalf("case variants must collide, got %v", err)
		}
	})
}

func TestSelectPlatformSemantics(t *testing.T) {
	entry := func(d content, osName, arch, variant string) IndexEntry {
		desc := descFor(descriptor.MediaTypeImageManifest, string(d))
		p := descriptor.Platform{OS: osName, Architecture: arch, Variant: variant}
		return IndexEntry{Descriptor: desc, Platform: &p}
	}

	t.Run("unique exact", func(t *testing.T) {
		idx := &Index{Manifests: []IndexEntry{
			entry("a", "linux", "amd64", ""),
			entry("b", "linux", "arm64", ""),
		}}
		got, err := SelectPlatform(idx, descriptor.Platform{OS: "linux", Architecture: "arm64"})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("request linux/arm64 -> %s", got.Descriptor.Digest)
		if got.Descriptor.Digest != digest.FromBytes([]byte("b")) {
			t.Fatal("wrong manifest selected")
		}
	})

	t.Run("not found", func(t *testing.T) {
		idx := &Index{Manifests: []IndexEntry{entry("a", "linux", "amd64", "")}}
		_, err := SelectPlatform(idx, descriptor.Platform{OS: "linux", Architecture: "s390x"})
		t.Logf("missing arch -> %v", err)
		if !errors.Is(err, ErrPlatformNotFound) {
			t.Fatalf("want ErrPlatformNotFound, got %v", err)
		}
	})

	t.Run("variant ambiguity rejected, no arbitrary choice", func(t *testing.T) {
		idx := &Index{Manifests: []IndexEntry{
			entry("a", "linux", "arm", "v7"),
			entry("b", "linux", "arm", "v8"),
		}}
		_, err := SelectPlatform(idx, descriptor.Platform{OS: "linux", Architecture: "arm"})
		t.Logf("wildcard request over v7/v8 -> %v", err)
		if !errors.Is(err, ErrAmbiguousPlatform) {
			t.Fatalf("want ErrAmbiguousPlatform, got %v", err)
		}
	})

	t.Run("explicit variant resolves uniquely", func(t *testing.T) {
		idx := &Index{Manifests: []IndexEntry{
			entry("a", "linux", "arm", "v7"),
			entry("b", "linux", "arm", "v8"),
		}}
		got, err := SelectPlatform(idx, descriptor.Platform{OS: "linux", Architecture: "arm", Variant: "v8"})
		if err != nil {
			t.Fatal(err)
		}
		if got.Descriptor.Digest != digest.FromBytes([]byte("b")) {
			t.Fatal("variant v8 should uniquely select entry b")
		}
	})

	t.Run("single variant candidate is usable", func(t *testing.T) {
		idx := &Index{Manifests: []IndexEntry{
			entry("a", "linux", "arm", "v7"),
		}}
		got, err := SelectPlatform(idx, descriptor.Platform{OS: "linux", Architecture: "arm"})
		if err != nil {
			t.Fatalf("single candidate must resolve, got %v", err)
		}
		if got.Descriptor.Digest != digest.FromBytes([]byte("a")) {
			t.Fatal("wrong single candidate")
		}
	})
}

type content string
