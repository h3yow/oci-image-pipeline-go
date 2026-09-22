package gc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/oci-image-pipeline-go/internal/assembler"
	"github.com/h3yow/oci-image-pipeline-go/internal/descriptor"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
	"github.com/h3yow/oci-image-pipeline-go/internal/store"
)

var gcClock = time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

type stack struct {
	st  *store.Store
	reg *registry.Registry
}

func newStack(t *testing.T) *stack {
	t.Helper()
	root := t.TempDir()
	st, err := store.Open(filepath.Join(root, "cas"))
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.Open(filepath.Join(root, "registry"), st)
	if err != nil {
		t.Fatal(err)
	}
	return &stack{st: st, reg: reg}
}

func (s *stack) build(t *testing.T, dir, osName, arch string) descriptor.Descriptor {
	t.Helper()
	a := assembler.New(s.st, func() time.Time { return gcClock })
	desc, err := s.reg.Stage(context.Background(), func() (descriptor.Descriptor, error) {
		res, err := a.Assemble(context.Background(), dir,
			assembler.Options{OS: osName, Architecture: arch})
		if err != nil {
			return descriptor.Descriptor{}, err
		}
		return res.ManifestDescriptor, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return desc
}

func ctxDir(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func blobCount(t *testing.T, st *store.Store) int {
	t.Helper()
	ds, err := st.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return len(ds)
}

func TestRefusedWhenNoTags(t *testing.T) {
	s := newStack(t)
	// 有孤儿对象但没有任何标签。
	_, _, _ = s.st.Put(context.Background(), bytes.NewReader([]byte("orphan")))
	_, err := New(s.st, s.reg).Run(context.Background())
	if !errors.Is(err, ErrNoTags) {
		t.Fatalf("want ErrNoTags, got %v", err)
	}
}

func TestReachableClosagePreservedAndOrphansSwept(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()

	// 同一标签先指向 v1，再被 v2 覆盖：v1 失去引用成为垃圾。
	d1 := s.build(t, ctxDir(t, "version-one"), "linux", "amd64")
	refLatest, _ := registry.ParseRef("app:latest")
	if err := s.reg.PushManifest(ctx, refLatest, d1); err != nil {
		t.Fatal(err)
	}
	d2 := s.build(t, ctxDir(t, "version-two-different"), "linux", "amd64")
	if err := s.reg.PushManifest(ctx, refLatest, d2); err != nil {
		t.Fatal(err)
	}

	before := blobCount(t, s.st)
	rep, err := New(s.st, s.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	after := blobCount(t, s.st)
	t.Logf("blobs before=%d after=%d deleted=%d freed=%d",
		before, after, len(rep.Deleted), rep.FreedBytes)

	// v2 闭包（manifest+config+layer）必须存活。
	for _, d := range []descriptor.Descriptor{d2} {
		if _, err := s.st.Stat(ctx, d.Digest); err != nil {
			t.Fatalf("referenced object %s deleted: %v", d.Digest, err)
		}
	}
	// v1（未被任何标签引用）应被清除。
	if _, err := s.st.Stat(ctx, d1.Digest); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unreachable manifest %s survived", d1.Digest)
	}
	// v2 仍可完整拉取，证明层没有被误删。
	m, _, err := s.reg.PullManifest(ctx, refLatest)
	if err != nil {
		t.Fatalf("referenced manifest unreadable after gc: %v", err)
	}
	rc, _, err := s.st.OpenExpected(ctx, m.Layers[0].Digest, m.Layers[0].Size)
	if err != nil {
		t.Fatalf("referenced layer deleted: %v", err)
	}
	rc.Close()
}

func TestSharedLayerNotDeletedWhileReferenced(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()
	dir := ctxDir(t, "shared-body")

	// 两个架构共享同一层（内容相同），各自有 config+manifest。
	amd := s.build(t, dir, "linux", "amd64")
	arm := s.build(t, dir, "linux", "arm64")
	if err := s.reg.PushManifest(ctx, ref("app", "amd"), amd); err != nil {
		t.Fatal(err)
	}
	if err := s.reg.PushManifest(ctx, ref("app", "arm"), arm); err != nil {
		t.Fatal(err)
	}

	// 删除 amd 标签后 GC：共享层仍被 arm 引用，必须保留。
	amdTagPath := filepath.Join(s.reg.Dir(), "tags", "app", "amd.json")
	if err := os.Remove(amdTagPath); err != nil {
		t.Fatal(err)
	}
	if _, err := New(s.st, s.reg).Run(ctx); err != nil {
		t.Fatal(err)
	}
	m, _, err := s.reg.PullManifest(ctx, ref("app", "arm"))
	if err != nil {
		t.Fatalf("arm manifest lost after gc: %v", err)
	}
	if _, err := s.st.Stat(ctx, m.Layers[0].Digest); err != nil {
		t.Fatalf("shared layer deleted while still referenced: %v", err)
	}
	t.Logf("shared layer %s preserved", m.Layers[0].Digest)
}

func TestCorruptReferencedObjectAbortsGC(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()
	d := s.build(t, ctxDir(t, "body"), "linux", "amd64")
	if err := s.reg.PushManifest(ctx, ref("app", "v1"), d); err != nil {
		t.Fatal(err)
	}
	// 损坏被引用的 manifest：GC 必须中止而非把它当垃圾或静默忽略。
	blob := filepath.Join(s.st.Root(), "blobs", "sha256", d.Digest.Hex())
	orig, err := os.ReadFile(blob)
	if err != nil {
		t.Fatal(err)
	}
	tampered := bytes.Clone(orig)
	tampered[0] ^= 0xFF
	if err := os.WriteFile(blob, tampered, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = New(s.st, s.reg).Run(ctx)
	t.Logf("gc with corrupt referenced manifest -> %v", err)
	if !errors.Is(err, store.ErrDigestMismatch) {
		t.Fatalf("want abort with ErrDigestMismatch, got %v", err)
	}
}

func TestSpaceBoundedAcrossLongHistory(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()

	// 模拟长期写入：不断构建新版本并移动同一个 latest 标签。
	var peak, finalCount int
	for i := 0; i < 12; i++ {
		d := s.build(t, ctxDir(t, fmt.Sprintf("body-%d", i)), "linux", "amd64")
		if err := s.reg.PushManifest(ctx, ref("app", "latest"), d); err != nil {
			t.Fatal(err)
		}
		// 每隔几版回收一次。
		if i%3 == 2 {
			if _, err := New(s.st, s.reg).Run(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if c := blobCount(t, s.st); c > peak {
			peak = c
		}
	}
	if _, err := New(s.st, s.reg).Run(ctx); err != nil {
		t.Fatal(err)
	}
	finalCount = blobCount(t, s.st)
	t.Logf("peak live blobs during history=%d final after gc=%d", peak, finalCount)
	// 单个 latest 闭包恰为 3 个对象（manifest/config/layer）。
	if finalCount != 3 {
		t.Fatalf("space grew with history: final blobs=%d, want 3", finalCount)
	}
}

func TestConcurrentGCAndWriters(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()

	// 先建立稳定标签，保证 GC 永远有根。
	base := s.build(t, ctxDir(t, "base-body"), "linux", "amd64")
	if err := s.reg.PushManifest(ctx, ref("app", "base"), base); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	const rounds = 20
	errCh := make(chan error, rounds*2)

	// 写者：持续构建并在新标签上发布，然后删除标签（制造垃圾）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			dir := ctxDir(t, fmt.Sprintf("w-%d", i))
			r := ref("scratch", fmt.Sprintf("t%d", i))
			a := assembler.New(s.st, func() time.Time { return gcClock })
			if err := s.reg.Publish(ctx, r, func() (descriptor.Descriptor, error) {
				res, err := a.Assemble(ctx, dir,
					assembler.Options{OS: "linux", Architecture: "amd64"})
				if err != nil {
					return descriptor.Descriptor{}, err
				}
				return res.ManifestDescriptor, nil
			}); err != nil {
				errCh <- err
				return
			}
			// 发布完成后再删除标签（模拟取消发布）；此时 GC 只能看到
			// "有标签"或"无标签"两种稳定状态之一。
			if err := os.Remove(filepath.Join(s.reg.Dir(), "tags", "scratch", fmt.Sprintf("t%d.json", i))); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// 回收者：不断 GC；base 闭包在任何时刻都必须存活。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			if _, err := New(s.st, s.reg).Run(ctx); err != nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// 最终 base 必须仍可完整拉取（未被任何一轮 GC 误删）。
	m, _, err := s.reg.PullManifest(ctx, ref("app", "base"))
	if err != nil {
		t.Fatalf("base manifest lost during concurrent gc: %v", err)
	}
	rc, _, err := s.st.OpenExpected(ctx, m.Layers[0].Digest, m.Layers[0].Size)
	if err != nil {
		t.Fatalf("base layer lost during concurrent gc: %v", err)
	}
	rc.Close()
	t.Logf("base survived concurrent gc; manifest=%s layer=%s",
		base.Digest, m.Layers[0].Digest)
}

func TestIndexClosageMarked(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()
	amd := s.build(t, ctxDir(t, "a"), "linux", "amd64")
	arm := s.build(t, ctxDir(t, "b"), "linux", "arm64")
	pa := descriptor.Platform{OS: "linux", Architecture: "amd64"}
	pb := descriptor.Platform{OS: "linux", Architecture: "arm64"}
	idxDesc, err := s.reg.BuildAndPushIndex(ctx, ref("multi", "v1"), []manifest.IndexEntry{
		{Descriptor: amd, Platform: &pa},
		{Descriptor: arm, Platform: &pb},
	})
	if err != nil {
		t.Fatal(err)
	}
	rep, err := New(s.st, s.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct{ name string; d interface{ String() string } }{
		{"index", idxDesc.Digest}, {"amd manifest", amd.Digest}, {"arm manifest", arm.Digest},
	} {
		if _, ok := rep.Reachable[toDigest(want.d)]; !ok {
			t.Fatalf("%s %s not marked reachable through index", want.name, want.d)
		}
	}
	t.Logf("index closure marked: index=%s amd=%s arm=%s",
		idxDesc.Digest, amd.Digest, arm.Digest)
}

func ref(name, tag string) registry.Ref { return registry.Ref{Name: name, Tag: tag} }
