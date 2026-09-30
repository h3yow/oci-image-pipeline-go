package registry_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/gc"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
)

type extEnv struct {
	dir   string
	store *cas.Store
	reg   *registry.Registry
}

func newExtEnv(t *testing.T) *extEnv {
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
	return &extEnv{dir: dir, store: store, reg: reg}
}

// writeImage 组装一个单平台镜像（config+层+清单）并写入 CAS。
func (e *extEnv) writeImage(t *testing.T, marker string) manifest.Descriptor {
	t.Helper()
	ctx := context.Background()
	cfg := []byte(fmt.Sprintf(`{"marker":%q}`, marker))
	cd, err := e.store.PutBytes(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ld, err := e.store.PutBytes(ctx, []byte("layer-"+marker))
	if err != nil {
		t.Fatal(err)
	}
	m := &manifest.Manifest{
		Config: manifest.Descriptor{MediaType: manifest.MediaTypeConfig, Digest: cd, Size: int64(len(cfg))},
		Layers: []manifest.Descriptor{{MediaType: manifest.MediaTypeLayer, Digest: ld, Size: int64(int64(len("layer-" + marker)))}},
	}
	data, err := manifest.EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	d, err := manifest.PutManifest(ctx, e.store, data)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// writeRawRef 直接落引用文件（可模拟无版本字段旧格式/未来版本）。
func (e *extEnv) writeRawRef(t *testing.T, ref string, doc []byte) {
	t.Helper()
	name, tag, ok := func() (string, string, bool) {
		for i := len(ref) - 1; i >= 0; i-- {
			if ref[i] == ':' {
				return ref[:i], ref[i+1:], true
			}
		}
		return "", "", false
	}()
	if !ok {
		t.Fatalf("bad ref %s", ref)
	}
	p := filepath.Join(e.dir, "refs", filepath.FromSlash(name), tag)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, doc, 0o644); err != nil {
		t.Fatal(err)
	}
}

// 提交闸门与回收互斥：BeginCommit 区间内 gc.Run 必须等待；区间内发布、
// 释放后再回收，新闭包对象受引用保护，一个都不少。
func TestCommitGateExcludesGC(t *testing.T) {
	e := newExtEnv(t)
	ctx := context.Background()
	m := e.writeImage(t, "gated")

	release := e.reg.BeginCommit() // 对象已写、引用未发布

	done := make(chan struct{})
	go func() {
		e.reg.BeginGC()
		e.reg.EndGC()
		close(done)
	}()

	select {
	case <-done:
		release()
		t.Fatal("判定依据: 提交区间内 gc 不得开始（BeginGC 应被阻塞）")
	default:
	}

	if err := e.reg.TagInCommit(ctx, "gated:v1", m); err != nil {
		release()
		t.Fatal(err)
	}
	release()
	<-done // gc 现在才能开始并完成

	if _, err := gc.New(e.store, e.reg).Run(ctx); err != nil {
		t.Fatal(err)
	}
	blobs, err := e.reg.ReferencedBlobs(ctx, "gated:v1")
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs) != 3 {
		t.Fatalf("提交并回收后闭包应仍为 3 个对象，实际 %d", len(blobs))
	}
	t.Logf("判定: 提交区间与 gc 互斥，发布后回收闭包对象=%d 全保留", len(blobs))
}

// 并发提交到各自独立引用：全部成功，引用各指其主，无半状态。
func TestConcurrentTagUnderGate(t *testing.T) {
	e := newExtEnv(t)
	ctx := context.Background()
	const n = 8
	descs := make([]manifest.Descriptor, n)
	for i := 0; i < n; i++ {
		descs[i] = e.writeImage(t, fmt.Sprintf("m%02d", i))
	}
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := e.reg.Tag(ctx, fmt.Sprintf("img%02d:v1", i), descs[i]); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("并发提交失败: %v", err)
	}
	for i := 0; i < n; i++ {
		got, err := e.reg.Resolve(ctx, fmt.Sprintf("img%02d:v1", i))
		if err != nil {
			t.Fatal(err)
		}
		if got.Digest != descs[i].Digest {
			t.Fatalf("img%02d 指向错位: got %s want %s", i, got.Digest.Short(), descs[i].Digest.Short())
		}
	}
	t.Logf("判定: %d 个并发提交全部成功且引用各指其主", n)
}

// 无 formatVersion 的旧引用在提交链路中照常工作；超出版本明确拒绝。
func TestLegacyRefWorksThroughCommit(t *testing.T) {
	e := newExtEnv(t)
	ctx := context.Background()
	amd := e.writeImage(t, "legacy-amd")

	// 无版本字段的旧引用，指向单清单。
	legacy, err := json.Marshal(map[string]any{"target": amd})
	if err != nil {
		t.Fatal(err)
	}
	e.writeRawRef(t, "old:latest", legacy)
	if _, err := e.reg.Resolve(ctx, "old:latest"); err != nil {
		t.Fatalf("旧引用应可解析: %v", err)
	}
	arm := e.writeImage(t, "legacy-arm")
	err = e.reg.AttachPlatform(ctx, "old:latest", arm, manifest.Platform{OS: "linux", Architecture: "arm64"})
	if !errors.Is(err, registry.ErrWrongKind) {
		t.Fatalf("对清单型旧引用追加平台应 ErrWrongKind（而非版本错误），got %v", err)
	}
	t.Logf("判定: 无版本字段旧引用照常解析，kind 判定正确 -> %v", err)

	// 旧索引引用：追加平台成功，闭包完整。
	ibytes, err := manifest.EncodeIndex(&manifest.Index{SchemaVersion: manifest.SchemaVersion})
	if err != nil {
		t.Fatal(err)
	}
	idesc, err := manifest.PutManifest(ctx, e.store, ibytes)
	if err != nil {
		t.Fatal(err)
	}
	legacyIdx, _ := json.Marshal(map[string]any{"target": idesc})
	e.writeRawRef(t, "oldidx:t", legacyIdx)
	if err := e.reg.AttachPlatform(ctx, "oldidx:t", amd,
		manifest.Platform{OS: "linux", Architecture: "amd64"}); err != nil {
		t.Fatalf("旧索引引用应可追加平台: %v", err)
	}
	blobs, err := e.reg.ReferencedBlobs(ctx, "oldidx:t")
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs) != 4 {
		t.Fatalf("旧索引追加后闭包应为 4（索引+清单+config+层），实际 %d", len(blobs))
	}
	t.Logf("判定: 旧索引引用经提交链路追加平台成功，闭包=%d", len(blobs))

	// 未来版本：明确拒绝，不与“缺失字段按 1 解释”混淆。
	future, _ := json.Marshal(map[string]any{"formatVersion": 2, "target": amd})
	e.writeRawRef(t, "old:latest", future)
	if _, err := e.reg.Resolve(ctx, "old:latest"); !errors.Is(err, registry.ErrUnsupportedReferenceVersion) {
		t.Fatalf("formatVersion=2 必须明确拒绝，got %v", err)
	}
	// 损坏引用不伪装成缺失。
	e.writeRawRef(t, "old:latest", []byte("{broken"))
	if _, err := e.reg.Resolve(ctx, "old:latest"); !errors.Is(err, registry.ErrCorruptRef) {
		t.Fatalf("损坏引用应 ErrCorruptRef，got %v", err)
	}
	t.Log("判定: 超版本明确拒绝、损坏引用不伪装成缺失")
}
