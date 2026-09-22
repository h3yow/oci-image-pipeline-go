package registry

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/descriptor"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/store"
)

func setup(t *testing.T) (*store.Store, *Registry, string) {
	t.Helper()
	root := t.TempDir()
	st, err := store.Open(filepath.Join(root, "cas"))
	if err != nil {
		t.Fatal(err)
	}
	reg, err := Open(filepath.Join(root, "registry"), st)
	if err != nil {
		t.Fatal(err)
	}
	return st, reg, root
}

func TestParseRef(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr error
	}{
		{"app", "app:latest", nil},
		{"app:v1", "app:v1", nil},
		{"lib/sub:1.0-rc_1", "lib/sub:1.0-rc_1", nil},
		{"APP:v1", "", ErrInvalidRef},
		{"app:", "", ErrInvalidRef},
		{":v1", "", ErrInvalidRef},
		{"app:bad/tag", "", ErrInvalidRef},
	}
	for _, tc := range cases {
		ref, err := ParseRef(tc.in)
		t.Logf("input=%q -> ref=%q err=%v", tc.in, ref, err)
		if tc.wantErr != nil {
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("%s: want %v, got %v", tc.in, tc.wantErr, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		if ref.String() != tc.want {
			t.Fatalf("%s: got %q want %q", tc.in, ref, tc.want)
		}
	}
}

func TestPushResolvePullRoundTrip(t *testing.T) {
	st, reg, _ := setup(t)
	ctx := context.Background()
	ctxDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(ctxDir, "f"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	desc := buildManifest(t, st, ctxDir, "linux", "amd64")
	ref, _ := ParseRef("app:v1")

	if err := reg.PushManifest(ctx, ref, desc); err != nil {
		t.Fatal(err)
	}
	rec, err := reg.Resolve(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("resolved %s kind=%s digest=%s", ref, rec.Kind, rec.Descriptor.Digest)
	if rec.Kind != KindManifest || rec.Descriptor.Digest != desc.Digest {
		t.Fatalf("bad record: %+v", rec)
	}
	m, _, err := reg.PullManifest(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != manifest.Version1 {
		t.Fatalf("want v1, got %d", m.Version)
	}
	// 未知标签。
	if _, err := reg.Resolve(ctx, Ref{Name: "app", Tag: "ghost"}); !errors.Is(err, ErrUnknownTag) {
		t.Fatalf("want ErrUnknownTag, got %v", err)
	}
}

func TestKindMismatchRejected(t *testing.T) {
	st, reg, _ := setup(t)
	ctx := context.Background()

	ctxDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(ctxDir, "f"), []byte("x"), 0o644)
	realMan := buildManifest(t, st, ctxDir, "linux", "amd64")
	p := descriptor.Platform{OS: "linux", Architecture: "amd64"}
	idxDesc, err := reg.BuildAndPushIndex(ctx, Ref{Name: "multi", Tag: "v1"},
		[]manifest.IndexEntry{{Descriptor: realMan, Platform: &p}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("index digest=%s", idxDesc.Digest)

	// 把索引当清单拉取必须失败，且错误可归类为 kind mismatch。
	_, _, err = reg.PullManifest(ctx, Ref{Name: "multi", Tag: "v1"})
	t.Logf("pull index as manifest -> %v", err)
	if !errors.Is(err, ErrKindMismatch) {
		t.Fatalf("want ErrKindMismatch, got %v", err)
	}
}

func TestLegacyV0IsReadOnly(t *testing.T) {
	st, reg, _ := setup(t)
	ctx := context.Background()

	v0 := []byte(`{"manifestVersion":0,"layers":[]}`)
	dgst, size, err := st.Put(ctx, bytes.NewReader(v0))
	if err != nil {
		t.Fatal(err)
	}
	desc := descriptor.Descriptor{MediaType: descriptor.MediaTypeLegacyManifest, Digest: dgst, Size: size}

	// 直接 Push 旧 mediaType 必须被拒绝。
	err = reg.PushManifest(ctx, Ref{Name: "old", Tag: "v1"}, desc)
	t.Logf("push v0 -> %v", err)
	if !errors.Is(err, ErrLegacyRejected) {
		t.Fatalf("want ErrLegacyRejected, got %v", err)
	}

	// 但历史遗留的标签文件（手工放置的旧引用）必须能被透明读出。
	if err := reg.writeRecord(Ref{Name: "old", Tag: "v1"},
		TagRecord{Descriptor: desc, Kind: KindManifest}); err != nil {
		t.Fatal(err)
	}
	m, rec, err := reg.PullManifest(ctx, Ref{Name: "old", Tag: "v1"})
	if err != nil {
		t.Fatalf("legacy tag must remain readable: %v", err)
	}
	t.Logf("pull legacy -> version=%d config=%v kind=%s", m.Version, m.Config, rec.Kind)
	if m.Version != manifest.Version0 {
		t.Fatalf("want v0, got %d", m.Version)
	}
}

func TestCorruptReferencedObjectClassified(t *testing.T) {
	st, reg, _ := setup(t)
	ctx := context.Background()
	ctxDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(ctxDir, "f"), []byte("abc"), 0o644)
	desc := buildManifest(t, st, ctxDir, "linux", "amd64")
	ref := Ref{Name: "app", Tag: "v1"}
	if err := reg.PushManifest(ctx, ref, desc); err != nil {
		t.Fatal(err)
	}

	blob := filepath.Join(st.Root(), "blobs", "sha256", desc.Digest.Hex())
	original, err := os.ReadFile(blob)
	if err != nil {
		t.Fatal(err)
	}

	// 同长度损坏：摘要不符。
	tampered := bytes.Clone(original)
	tampered[0] ^= 0xFF
	if err := os.WriteFile(blob, tampered, 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, err = reg.PullManifest(ctx, ref)
	t.Logf("same-size corruption -> %v", err)
	if !errors.Is(err, store.ErrDigestMismatch) {
		t.Fatalf("want ErrDigestMismatch, got %v", err)
	}

	// 截断：声明大小不匹配。
	if err := os.WriteFile(blob, original[:1], 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, err = reg.PullManifest(ctx, ref)
	t.Logf("truncation -> %v", err)
	if !errors.Is(err, store.ErrSizeMismatch) {
		t.Fatalf("want ErrSizeMismatch, got %v", err)
	}

	// 删除：未命中。
	_ = os.Remove(blob)
	_, _, err = reg.PullManifest(ctx, ref)
	t.Logf("missing -> %v", err)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestConcurrentTagWritesNeverHalfVisible(t *testing.T) {
	_, reg, _ := setup(t)
	ctx := context.Background()

	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d, _ := digestParse(t, "body-"+itoa(i))
			// 用 PushManifest 校验对象存在性会失败（对象不在 CAS），
			// 这里直接走 writeRecord 压测原子可见性（记录级并发）。
			err := reg.writeRecord(Ref{Name: "race", Tag: "t" + itoa(i)}, TagRecord{
				Descriptor: descriptor.Descriptor{
					MediaType: descriptor.MediaTypeImageManifest,
					Digest:    d,
					Size:      int64(len("body-" + itoa(i))),
				},
				Kind: KindManifest,
			})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	tags, err := reg.Tags(ctx, "race")
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) != n {
		t.Fatalf("want %d tags, got %d", n, len(tags))
	}
	// 每个记录必须能解析为结构完整的 JSON（无半写入）。
	for _, tag := range tags {
		rec, err := reg.Resolve(ctx, Ref{Name: "race", Tag: tag})
		if err != nil {
			t.Fatalf("tag %s half-visible: %v", tag, err)
		}
		if rec.Kind != KindManifest {
			t.Fatalf("tag %s wrong kind", tag)
		}
	}
}

func putBytes(t *testing.T, st *store.Store, media descriptor.MediaType, b []byte) descriptor.Descriptor {
	t.Helper()
	dgst, size, err := st.Put(context.Background(), bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	return descriptor.Descriptor{MediaType: media, Digest: dgst, Size: size}
}
