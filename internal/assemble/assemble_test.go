package assemble

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
)

func sampleSpec() ImageSpec {
	return ImageSpec{
		Config: map[string]any{"os": "linux", "architecture": "amd64"},
		Layers: []LayerSpec{
			{Name: "base", Files: []File{
				{Path: "etc/hosts", Data: []byte("127.0.0.1 localhost\n")},
				{Path: "bin/app", Data: []byte("APP-BINARY-v1"), Mode: 0o755},
			}},
			{Name: "app", Files: []File{
				{Path: "var/log/run.txt", Data: []byte("startup")},
			}},
		},
	}
}

func TestRepeatedAssemblyIsIdentical(t *testing.T) {
	store, err := cas.NewStore(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	a := New(store)
	ctx := context.Background()

	d1, r1, err := a.AssembleImage(ctx, sampleSpec())
	if err != nil {
		t.Fatal(err)
	}
	d2, r2, err := a.AssembleImage(ctx, sampleSpec())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("第一次镜像摘要=%s config=%s", d1.Digest, r1.ConfigDigest)
	t.Logf("第二次镜像摘要=%s config=%s", d2.Digest, r2.ConfigDigest)
	if d1.Digest != d2.Digest || r1.ConfigDigest != r2.ConfigDigest {
		t.Fatal("判定依据: 重复组装得到不同镜像/config 摘要")
	}
	if d1.Size != d2.Size {
		t.Fatal("镜像大小不一致")
	}
	// 第二次所有层必须全部命中去重。
	for i, ld := range r2.LayerDecisions {
		t.Logf("层 %q: 摘要=%s reused=%v 输入指纹=%s", ld.Name, ld.Digest, ld.Reused, ld.InputSHA)
		if !ld.Reused {
			t.Fatalf("第 %d 层 %q 第二次组装未走去重", i, ld.Name)
		}
		if ld.Digest != r1.LayerDecisions[i].Digest {
			t.Fatal("层摘要在重复组装间不稳定")
		}
	}
}

func TestContentChangeChangesDigest(t *testing.T) {
	store, _ := cas.NewStore(filepath.Join(t.TempDir(), "store"))
	a := New(store)
	ctx := context.Background()

	d1, _, err := a.AssembleImage(ctx, sampleSpec())
	if err != nil {
		t.Fatal(err)
	}
	changed := sampleSpec()
	changed.Layers[1].Files[0].Data = []byte("startup-v2")
	d2, _, err := a.AssembleImage(ctx, changed)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("变更一层内容前镜像=%s 变更后=%s", d1.Digest, d2.Digest)
	if d1.Digest == d2.Digest {
		t.Fatal("判定依据: 内容变化后镜像摘要未变化")
	}
	// 未变化的第一层仍应去重：组装第三份与 d1 完全相同的输入。
	d3, r3, err := a.AssembleImage(ctx, sampleSpec())
	if err != nil {
		t.Fatal(err)
	}
	if d3.Digest != d1.Digest {
		t.Fatal("相同输入在其他镜像写入后仍应得到原摘要")
	}
	if !r3.LayerDecisions[0].Reused {
		t.Fatal("共享的相同层必须被去重，不得重复写入")
	}
	t.Log("判定: 相同层跨镜像共享去重，不同层稳定区分")
}

func TestConfigDefaultSemantics(t *testing.T) {
	store, _ := cas.NewStore(filepath.Join(t.TempDir(), "store"))
	a := New(store)
	ctx := context.Background()

	s1 := sampleSpec()
	s2 := sampleSpec()
	s2.Config["created"] = "2026-09-23T00:00:00Z" // 易变字段必须被忽略
	d1, _, _ := a.AssembleImage(ctx, s1)
	d2, _, _ := a.AssembleImage(ctx, s2)
	t.Logf("含 created 输入 vs 不含: %s / %s", d1.Digest, d2.Digest)
	if d1.Digest != d2.Digest {
		t.Fatal("created 必须固定忽略，否则结论不可复现")
	}
}

func TestManifestProducedIsSupportedShape(t *testing.T) {
	store, _ := cas.NewStore(filepath.Join(t.TempDir(), "store"))
	a := New(store)
	desc, _, err := a.AssembleImage(context.Background(), sampleSpec())
	if err != nil {
		t.Fatal(err)
	}
	if desc.MediaType != manifest.MediaTypeManifest {
		t.Fatalf("输出描述符 mediaType=%s", desc.MediaType)
	}
	if _, err := store.GetBytes(context.Background(), desc.Digest); err != nil &&
		!errors.Is(err, nil) {
		t.Fatalf("清单对象不可读: %v", err)
	}
}
