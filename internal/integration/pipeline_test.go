// Package integration 存放跨包端到端测试，验证从组装、分发、多架构选择到
// 回收的完整链路；每个用例打印关键输入（上下文内容/平台）与判定依据
// （摘要/错误归类），保证结论可复现、可解释。
package integration

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
	"github.com/h3yow/oci-image-pipeline-go/internal/gc"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
	"github.com/h3yow/oci-image-pipeline-go/internal/store"
)

type env struct {
	st  *store.Store
	reg *registry.Registry
}

func newEnv(t *testing.T) *env {
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
	return &env{st: st, reg: reg}
}

var clock = time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

func (e *env) contextDir(t *testing.T, files map[string]string) string {
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

func (e *env) build(t *testing.T, files map[string]string, osName, arch string) descriptor.Descriptor {
	t.Helper()
	dir := e.contextDir(t, files)
	a := assembler.New(e.st, func() time.Time { return clock })
	desc, err := e.reg.Stage(context.Background(), func() (descriptor.Descriptor, error) {
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

// TestRepeatablePipeline 同一上下文重复构建并经仓库解析，摘要完全一致。
func TestRepeatablePipeline(t *testing.T) {
	e := newEnv(t)
	files := map[string]string{"bin/app": "ELF-bytes", "etc/cfg": "k=v"}

	d1 := e.build(t, files, "linux", "amd64")
	d2 := e.build(t, files, "linux", "amd64")
	t.Logf("input files=%v", files)
	t.Logf("build#1 image=%s", d1.Digest)
	t.Logf("build#2 image=%s", d2.Digest)
	if d1.Digest != d2.Digest {
		t.Fatal("repeated pipeline produced different image digests")
	}
	ref, _ := registry.ParseRef("svc:1.0")
	if err := e.reg.PushManifest(context.Background(), ref, d1); err != nil {
		t.Fatal(err)
	}
	got, _, err := e.reg.PullManifest(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("resolved svc:1.0 version=%d layers=%d", got.Version, len(got.Layers))
}

// TestMultiArchSelection 索引内多平台选择：命中唯一/缺失/歧义均确定。
func TestMultiArchSelection(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	amd := e.build(t, map[string]string{"f": "amd-binary"}, "linux", "amd64")
	armV7 := e.build(t, map[string]string{"f": "armv7-binary"}, "linux", "arm")
	armV8 := e.build(t, map[string]string{"f": "armv8-binary"}, "linux", "arm")

	// 修正两个 arm 条目的变体字段（build 选项未暴露 variant，直接改索引输入）。
	pAmd := descriptor.Platform{OS: "linux", Architecture: "amd64"}
	pV7 := descriptor.Platform{OS: "linux", Architecture: "arm", Variant: "v7"}
	pV8 := descriptor.Platform{OS: "linux", Architecture: "arm", Variant: "v8"}
	idxDesc, err := e.reg.BuildAndPushIndex(ctx, refOf("svc", "multi"), []manifest.IndexEntry{
		{Descriptor: amd, Platform: &pAmd},
		{Descriptor: armV7, Platform: &pV7},
		{Descriptor: armV8, Platform: &pV8},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("index=%s with amd64/armv7/armv8", idxDesc.Digest)

	r := refOf("svc", "multi")
	if m, dgst, err := e.reg.PullForPlatform(ctx, r, descriptor.Platform{OS: "linux", Architecture: "amd64"}); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("select linux/amd64 -> %s (want %s)", dgst, amd.Digest)
		if dgst != amd.Digest {
			t.Fatal("amd64 selected wrong manifest")
		}
		_ = m
	}
	if _, dgst, err := e.reg.PullForPlatform(ctx, r, descriptor.Platform{OS: "linux", Architecture: "arm", Variant: "v8"}); err != nil {
		t.Fatalf("explicit v8 must resolve: %v", err)
	} else {
		t.Logf("select linux/arm/v8 -> %s (want %s)", dgst, armV8.Digest)
		if dgst != armV8.Digest {
			t.Fatal("arm/v8 selected wrong manifest")
		}
	}
	_, _, err = e.reg.PullForPlatform(ctx, r, descriptor.Platform{OS: "linux", Architecture: "arm"})
	t.Logf("select linux/arm (no variant) over v7+v8 -> %v", err)
	if !errors.Is(err, manifest.ErrAmbiguousPlatform) {
		t.Fatalf("want ErrAmbiguousPlatform, got %v", err)
	}
	_, _, err = e.reg.PullForPlatform(ctx, r, descriptor.Platform{OS: "linux", Architecture: "ppc64le"})
	if !errors.Is(err, manifest.ErrPlatformNotFound) {
		t.Fatalf("want ErrPlatformNotFound, got %v", err)
	}
}

// TestCorruptionIsClassified 端到端损坏：三类原因可区分，绝不静默返回坏内容。
func TestCorruptionIsClassified(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	d := e.build(t, map[string]string{"f": "payload"}, "linux", "amd64")
	r := refOf("svc", "1")
	if err := e.reg.PushManifest(ctx, r, d); err != nil {
		t.Fatal(err)
	}
	blob := filepath.Join(e.st.Root(), "blobs", "sha256", d.Digest.Hex())
	orig, _ := os.ReadFile(blob)

	// 1) 同长度篡改 → 摘要不符
	tampered := bytes.Clone(orig)
	tampered[len(tampered)-2] ^= 0x01
	_ = os.WriteFile(blob, tampered, 0o644)
	_, _, err := e.reg.PullManifest(ctx, r)
	t.Logf("[same-size tamper] digest=%s -> %v", d.Digest, err)
	if !errors.Is(err, store.ErrDigestMismatch) {
		t.Fatalf("want ErrDigestMismatch, got %v", err)
	}

	// 2) 截断 → 大小不符
	_ = os.WriteFile(blob, orig[:len(orig)/2], 0o644)
	_, _, err = e.reg.PullManifest(ctx, r)
	t.Logf("[truncate] declared=%d stored=%d -> %v", d.Size, len(orig)/2, err)
	if !errors.Is(err, store.ErrSizeMismatch) {
		t.Fatalf("want ErrSizeMismatch, got %v", err)
	}

	// 3) 删除 → 未命中
	_ = os.Remove(blob)
	_, _, err = e.reg.PullManifest(ctx, r)
	t.Logf("[missing] -> %v", err)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// TestConcurrentAssembleAndGC 并发组装+打标签与 GC 共存：已发布镜像始终完整。
func TestConcurrentAssembleAndGC(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	// 锚点镜像：全程必须存活且可读。
	anchor := e.build(t, map[string]string{"f": "anchor"}, "linux", "amd64")
	anchorRef := refOf("svc", "anchor")
	if err := e.reg.PushManifest(ctx, anchorRef, anchor); err != nil {
		t.Fatal(err)
	}

	const workers, iterations = 6, 15
	var wg sync.WaitGroup
	errCh := make(chan error, workers*iterations+iterations)
	a := assembler.New(e.st, func() time.Time { return clock })

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				dir := e.contextDir(t, map[string]string{"f": fmt.Sprintf("w%d-i%d", w, i)})
				r := refOf("wip", fmt.Sprintf("%d-%d", w, i))
				if err := e.reg.Publish(ctx, r, func() (descriptor.Descriptor, error) {
					return assemble(a, ctx, dir)
				}); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations*2; i++ {
			if _, err := gc.New(e.st, e.reg).Run(ctx); err != nil {
				errCh <- err
				return
			}
			// 每轮 GC 后锚点必须仍可拉取（层未被误删）。
			m, _, err := e.reg.PullManifest(ctx, anchorRef)
			if err != nil {
				errCh <- fmt.Errorf("anchor unreadable after gc: %w", err)
				return
			}
			rc, _, err := e.st.OpenExpected(ctx, m.Layers[0].Digest, m.Layers[0].Size)
			if err != nil {
				errCh <- fmt.Errorf("anchor layer unreadable after gc: %w", err)
				return
			}
			rc.Close()
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	t.Logf("anchor %s survived %d gc rounds under %d concurrent builders",
		anchor.Digest, iterations*2, workers)
}

func refOf(name, tag string) registry.Ref { return registry.Ref{Name: name, Tag: tag} }

// assemble 执行一次组装并返回清单描述符（发布临界区内使用）。
func assemble(a *assembler.Assembler, ctx context.Context, dir string) (descriptor.Descriptor, error) {
	res, err := a.Assemble(ctx, dir, assembler.Options{OS: "linux", Architecture: "amd64"})
	if err != nil {
		return descriptor.Descriptor{}, err
	}
	return res.ManifestDescriptor, nil
}
