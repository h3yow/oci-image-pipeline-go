package integration_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/assemble"
	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/gc"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
)

// 全链路：重复组装一致 -> 多架构索引唯一选择 -> 回收不误删 -> 损坏可区分。
func TestEndToEnd(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "repo")
	store, err := cas.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.New(store)
	if err != nil {
		t.Fatal(err)
	}
	asm := assemble.New(store)

	platforms := []struct {
		platform manifest.Platform
		files    []assemble.File
	}{
		{manifest.Platform{OS: "linux", Architecture: "amd64"}, []assemble.File{
			{Path: "bin/app", Data: []byte("APP-AMD64"), Mode: 0o755},
		}},
		{manifest.Platform{OS: "linux", Architecture: "arm64"}, []assemble.File{
			{Path: "bin/app", Data: []byte("APP-ARM64"), Mode: 0o755},
		}},
	}

	manifests := make([]manifest.Descriptor, len(platforms))
	for round := 0; round < 2; round++ { // 第二轮验证重复组装一致性
		for i, pl := range platforms {
			spec := assemble.ImageSpec{
				Config: map[string]any{"os": pl.platform.OS, "architecture": pl.platform.Architecture},
				Layers: []assemble.LayerSpec{{Name: "rootfs", Files: pl.files}},
			}
			desc, report, err := asm.AssembleImage(ctx, spec)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("round=%d 平台=%s/%s 镜像摘要=%s 层去重=%v",
				round, pl.platform.OS, pl.platform.Architecture, desc.Digest.Short(),
				report.LayerDecisions[0].Reused)
			if round == 1 && !report.LayerDecisions[0].Reused {
				t.Fatal("判定依据: 重复组装必须全部命中去重")
			}
			manifests[i] = desc
		}
	}

	// 建多架构索引。
	for i, pl := range platforms {
		if err := reg.AttachPlatform(ctx, "ns/app:1.0", manifests[i], pl.platform); err != nil {
			t.Fatal(err)
		}
	}

	// 平台选择必须唯一确定。
	for i, pl := range platforms {
		got, err := reg.SelectPlatform(ctx, "ns/app:1.0", pl.platform)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("选择 %s/%s -> %s", pl.platform.OS, pl.platform.Architecture, got.Digest.Short())
		if got.Digest != manifests[i].Digest {
			t.Fatalf("平台选择错位: %s/%s", pl.platform.OS, pl.platform.Architecture)
		}
	}
	if _, err := reg.SelectPlatform(ctx, "ns/app:1.0", manifest.Platform{OS: "linux", Architecture: "386"}); !errors.Is(err, registry.ErrNoMatch) {
		t.Fatalf("缺失平台应 ErrNoMatch，got %v", err)
	}

	// 先写一个孤儿层，GC 后应消失；被引用对象应保留。
	orphan, _ := store.PutBytes(ctx, []byte("soon-orphaned"))
	if _, err := gc.New(store, reg).Run(ctx); err != nil {
		t.Fatal(err)
	}
	if ok, _ := store.Exists(ctx, orphan); ok {
		t.Fatal("判定依据: 无引用孤儿层必须被回收")
	}
	blobs, err := reg.ReferencedBlobs(ctx, "ns/app:1.0")
	if err != nil {
		t.Fatal(err)
	}
	for d := range blobs {
		if ok, _ := store.Exists(ctx, d); !ok {
			t.Fatalf("判定依据: 仍被引用对象 %s 被 GC 误删", d.Short())
		}
	}
	t.Logf("GC 后可达对象=%d 个，全部完整保留", len(blobs))

	// 人为损坏一个层文件：读取必须报 ErrCorrupt 而非 ErrNotFound。
	var layer manifest.Descriptor
	_, mdata, err := manifest.GetManifest(ctx, store, manifests[0].Digest)
	if err != nil {
		t.Fatal(err)
	}
	m, _ := manifest.ParseManifest(mdata)
	layer = m.Layers[0]
	lp, _ := store.Path(layer.Digest)
	good, _ := os.ReadFile(lp)
	if err := os.WriteFile(lp, good[:len(good)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = store.GetBytes(ctx, layer.Digest)
	t.Logf("输入: 截断被引用层 %s -> err=%v", layer.Digest.Short(), err)
	if !errors.Is(err, cas.ErrCorrupt) {
		t.Fatalf("截断层必须 ErrCorrupt，got %v", err)
	}
	if !errors.Is(err, cas.ErrDigestMismatch) {
		t.Fatalf("截断层错误应保留 ErrDigestMismatch 因果，got %v", err)
	}

	// GC 面对损坏引用必须中止而非误删（ReferencedBlobs 返回错误）。
	_, err = gc.New(store, reg).Run(ctx)
	t.Logf("输入: 损坏对象仍被引用时执行 GC -> err=%v", err)
	if err == nil {
		t.Fatal("判定依据: 引用闭包无法完整解析时 GC 必须中止，避免误删")
	}
}
