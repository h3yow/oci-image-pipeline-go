package gc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
)

type env struct {
	store *cas.Store
	reg   *registry.Registry
	dir   string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	store, err := cas.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.New(store)
	if err != nil {
		t.Fatal(err)
	}
	return &env{store: store, reg: reg, dir: dir}
}

func putBlob(t *testing.T, e *env, content string) digest.Digest {
	t.Helper()
	d, err := e.store.PutBytes(context.Background(), []byte(content))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestGCDeletesUnreferencedKeepsReferenced(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	orphan := putBlob(t, e, "orphan-layer")
	orphan2 := putBlob(t, e, "another-orphan")

	// 构造一个被引用的镜像。
	config, _ := e.store.PutBytes(ctx, []byte(`{"os":"linux"}`))
	layer, _ := e.store.PutBytes(ctx, []byte("referenced-layer"))
	m := &manifest.Manifest{
		Config: manifest.Descriptor{MediaType: manifest.MediaTypeConfig, Digest: config, Size: sizeOf(ctx, t, e, config)},
		Layers: []manifest.Descriptor{{MediaType: manifest.MediaTypeLayer, Digest: layer, Size: sizeOf(ctx, t, e, layer)}},
	}
	data, err := manifest.EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	mdesc, err := manifest.PutManifest(ctx, e.store, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.reg.Tag(ctx, "app:v1", mdesc); err != nil {
		t.Fatal(err)
	}

	t.Logf("输入: 无引用层=%s,%s；被引用层=%s（经 app:v1）",
		orphan.Short(), orphan2.Short(), layer.Short())

	res, err := New(e.store, e.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("判定: visited=%d kept=%d deleted=%d", len(res.Visited), len(res.Kept), len(res.Deleted))
	for _, d := range res.Deleted {
		t.Logf("  删除 %s（依据: 不在任何引用闭包且未被 pin）", digest.Digest(d).Short())
	}
	for _, d := range res.Kept {
		t.Logf("  保留 %s（依据: 被引用或 pin）", digest.Digest(d).Short())
	}
	for _, d := range []digest.Digest{orphan, orphan2} {
		if _, err := e.store.GetBytes(ctx, d); err == nil {
			t.Fatalf("无引用层 %s 应被回收", d.Short())
		}
	}
	for _, d := range []digest.Digest{layer, config, mdesc.Digest} {
		if _, err := e.store.GetBytes(ctx, d); err != nil {
			t.Fatalf("仍被引用的对象 %s 不得删除: %v", d.Short(), err)
		}
	}

	// 再跑一轮：稳定状态下不应再删除任何东西（结论可复现）。
	res2, err := New(e.store, e.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Deleted) != 0 {
		t.Fatalf("第二轮回收不应再删除对象，实际删除 %d: %v", len(res2.Deleted), res2.Deleted)
	}
	t.Log("判定: 第二轮回收删除 0 个，稳定状态结论可复现")
}

func sizeOf(ctx context.Context, t *testing.T, e *env, d digest.Digest) int64 {
	t.Helper()
	n, err := e.store.Stat(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestGCCleansStaleTmp(t *testing.T) {
	e := newEnv(t)
	tmpDir := filepath.Join(e.dir, "tmp")
	// 模拟进程崩溃后残留、且没有任何进行中写入 pin 的临时文件。
	// 新语义不依赖 mtime：无 pin 即视为可回收。
	for _, name := range []string{"put-crashed-a", "put-crashed-b"} {
		if err := os.WriteFile(filepath.Join(tmpDir, name), []byte("half-written"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	res, err := New(e.store, e.reg).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: tmp 中 2 个无 pin 残留文件 -> 清理=%v", res.StaleTmp)
	if len(res.StaleTmp) != 2 {
		t.Fatalf("应清理 2 个残留临时文件，实际 %d: %v", len(res.StaleTmp), res.StaleTmp)
	}
}

func TestConcurrentGCAndWrites(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	// 先建立一个长期被引用的镜像，任何一轮 GC 都必须保留它。
	pinned := putBlob(t, e, "permanent-base-layer")
	cfg, _ := e.store.PutBytes(ctx, []byte(`{"os":"linux","arch":"amd64"}`))
	m := &manifest.Manifest{
		Config: manifest.Descriptor{MediaType: manifest.MediaTypeConfig, Digest: cfg, Size: sizeOf(ctx, t, e, cfg)},
		Layers: []manifest.Descriptor{{MediaType: manifest.MediaTypeLayer, Digest: pinned, Size: sizeOf(ctx, t, e, pinned)}},
	}
	data, _ := manifest.EncodeManifest(m)
	mdesc, _ := manifest.PutManifest(ctx, e.store, data)
	if err := e.reg.Tag(ctx, "base:v1", mdesc); err != nil {
		t.Fatal(err)
	}

	const writers, rounds = 6, 8
	var wg sync.WaitGroup
	errCh := make(chan error, writers*rounds+rounds)

	// 并发写：大量“临时”层，写完后不打引用 -> 可被回收。
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				content := fmt.Sprintf("ephemeral-w%d-i%d", w, i)
				d := digest.FromBytes([]byte(content))
				if err := e.store.Put(ctx, d, bytesReader([]byte(content))); err != nil {
					errCh <- fmt.Errorf("writer %d: %w", w, err)
					return
				}
			}
		}(w)
	}
	// 并发回收。
	for i := 0; i < rounds; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := New(e.store, e.reg).Run(ctx); err != nil {
				errCh <- fmt.Errorf("gc %d: %w", i, err)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	// 最终一轮回收：所有临时层应消失，被引用对象必须仍在且完整。
	res, err := New(e.store, e.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.GetBytes(ctx, pinned); err != nil {
		t.Fatalf("被引用层在并发回收后丢失/损坏: %v", err)
	}
	if _, err := e.store.GetBytes(ctx, mdesc.Digest); err != nil {
		t.Fatalf("引用清单在并发回收后丢失: %v", err)
	}
	all, _ := e.store.List(ctx)
	t.Logf("并发结束后剩余对象数=%d（visited=%d kept=%d deleted=%d）",
		len(all), len(res.Visited), len(res.Kept), len(res.Deleted))
	// 稳定后只剩 base 镜像的 3 个对象（清单+config+layer）。
	if len(all) != 3 {
		t.Fatalf("判定依据: 长期写入后占用应回落到可达集 3 个对象，实际 %d", len(all))
	}

	// 全量完整性：没有半写入/摘要错位对象。
	for _, d := range all {
		if _, err := e.store.GetBytes(ctx, d); err != nil {
			t.Fatalf("剩余对象 %s 不完整: %v", d.Short(), err)
		}
	}
	t.Log("判定: 并发写入+回收无半写入、无摘要错位，占用不随历史写入无界增长")
}
