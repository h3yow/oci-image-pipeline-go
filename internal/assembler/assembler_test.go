package assembler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/h3yow/oci-image-pipeline-go/internal/store"
)

func fixedTime() time.Time { return time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC) }

func writeContext(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		full := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestRepeatAssemblyIsIdentical(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"z-last.txt": "zebra",
		"a-first":    "apple",
		"sub/mid":    "middle",
	}
	dir := writeContext(t, files)
	a := New(st, fixedTime)
	ctx := context.Background()

	r1, err := a.Assemble(ctx, dir, Options{OS: "linux", Architecture: "amd64"})
	if err != nil {
		t.Fatal(err)
	}
	// 篡改上下文文件的 mtime：不应影响层摘要（tar 内时间被固定）。
	if err := os.Chtimes(filepath.Join(dir, "a-first"), time.Now(), time.Now()); err != nil {
		t.Fatal(err)
	}
	r2, err := a.Assemble(ctx, dir, Options{OS: "linux", Architecture: "amd64"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("layer1=%s layer2=%s", r1.LayerDescriptors[0].Digest, r2.LayerDescriptors[0].Digest)
	t.Logf("image1=%s image2=%s", r1.ManifestDescriptor.Digest, r2.ManifestDescriptor.Digest)
	if r1.LayerDescriptors[0].Digest != r2.LayerDescriptors[0].Digest {
		t.Fatal("layer digest changed across repeated assembly")
	}
	if r1.ConfigDescriptor.Digest != r2.ConfigDescriptor.Digest {
		t.Fatal("config digest changed across repeated assembly")
	}
	if r1.ManifestDescriptor.Digest != r2.ManifestDescriptor.Digest {
		t.Fatal("image digest changed across repeated assembly")
	}
}

func TestExistingLayerNotRewritten(t *testing.T) {
	root := t.TempDir()
	st, _ := store.Open(filepath.Join(root, "cas"))
	dir := writeContext(t, map[string]string{"only.txt": "payload"})
	a := New(st, fixedTime)
	ctx := context.Background()

	r1, err := a.Assemble(ctx, dir, Options{OS: "linux", Architecture: "amd64"})
	if err != nil {
		t.Fatal(err)
	}
	blobsBefore := countBlobs(t, st)
	r2, err := a.Assemble(ctx, dir, Options{OS: "linux", Architecture: "arm64"})
	if err != nil {
		t.Fatal(err)
	}
	blobsAfter := countBlobs(t, st)
	t.Logf("blobs before=%d after=%d; layer reused=%v; new bytes=%d",
		blobsBefore, blobsAfter,
		r1.LayerDescriptors[0].Digest == r2.LayerDescriptors[0].Digest, r2.NewLayerBytes)
	if r1.LayerDescriptors[0].Digest != r2.LayerDescriptors[0].Digest {
		t.Fatal("same context must yield same layer across architectures")
	}
	// 层不重复写入：只新增 config+manifest（2 个对象），层文件复用。
	if blobsAfter-blobsBefore != 2 {
		t.Fatalf("expected exactly 2 new blobs (config+manifest), got %d", blobsAfter-blobsBefore)
	}
}

func TestDifferentContentDistinguished(t *testing.T) {
	st, _ := store.Open(filepath.Join(t.TempDir(), "cas"))
	a := New(st, fixedTime)
	ctx := context.Background()

	d1 := writeContext(t, map[string]string{"f": "content-one"})
	d2 := writeContext(t, map[string]string{"f": "content-two"})
	r1, err := a.Assemble(ctx, d1, Options{OS: "linux", Architecture: "amd64"})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := a.Assemble(ctx, d2, Options{OS: "linux", Architecture: "amd64"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("content-one -> layer=%s image=%s", r1.LayerDescriptors[0].Digest, r1.ManifestDescriptor.Digest)
	t.Logf("content-two -> layer=%s image=%s", r2.LayerDescriptors[0].Digest, r2.ManifestDescriptor.Digest)
	if r1.LayerDescriptors[0].Digest == r2.LayerDescriptors[0].Digest {
		t.Fatal("different content must produce different layer digest")
	}
	if r1.ManifestDescriptor.Digest == r2.ManifestDescriptor.Digest {
		t.Fatal("different content must produce different image digest")
	}
}

func TestMissingContextRejected(t *testing.T) {
	st, _ := store.Open(filepath.Join(t.TempDir(), "cas"))
	a := New(st, fixedTime)
	_, err := a.Assemble(context.Background(), filepath.Join(t.TempDir(), "nope"),
		Options{OS: "linux", Architecture: "amd64"})
	if err != ErrContextMissing {
		t.Fatalf("want ErrContextMissing, got %v", err)
	}
}

func countBlobs(t *testing.T, st *store.Store) int {
	t.Helper()
	ds, err := st.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return len(ds)
}
