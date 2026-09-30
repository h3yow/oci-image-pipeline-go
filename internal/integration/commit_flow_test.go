package integration_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/assemble"
	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/commit"
	"github.com/h3yow/oci-image-pipeline-go/internal/gc"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
	"github.com/h3yow/oci-image-pipeline-go/internal/verify"
)

// 端到端：原子提交多平台 -> 提交失败可重试且引用不动 -> verify 逐对象
// 三分类 -> gc 长期占用有界。
func TestCommitAtomicityAndRetryFlow(t *testing.T) {
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
	committer := commit.New(store, reg)

	platform := func(osName, arch, bin string) commit.Request {
		return commit.Request{
			Ref:      "ns/app:1.0",
			Platform: manifest.Platform{OS: osName, Architecture: arch},
			AsIndex:  true,
			Image: assembleSpec(osName, arch, []assemble.File{
				{Path: "bin/app", Data: []byte(bin), Mode: 0o755},
			}),
		}
	}

	// 两次成功的多平台提交。
	r1, err := committer.Commit(ctx, platform("linux", "amd64", "APP-AMD64"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := committer.Commit(ctx, platform("linux", "arm64", "APP-ARM64")); err != nil {
		t.Fatal(err)
	}
	t.Logf("提交后引用目标（索引）=%s", r1.Target.MediaType)

	// 引用闭包随时可解析、对象全完整。
	blobs, err := reg.ReferencedBlobs(ctx, "ns/app:1.0")
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs) != 7 { // 1 索引 + 2 清单 + 2 config + 2 层
		t.Fatalf("两平台闭包应为 7 个对象，实际 %d", len(blobs))
	}

	// 健康状态下 verify：全部 ok。
	rep, err := verify.New(store, reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for st, n := range rep.Counts() {
		if st != verify.StatusOK && n != 0 {
			t.Fatalf("健康仓库不应有 %s 结论，实际 %d 个", st, n)
		}
	}

	// 人为截断 amd64 层：对该平台再次“同内容提交”应被拒（组装写层时
	// 发现同名损坏），引用保持当前索引；修复后提交（重复平台）得到
	// ErrDuplicatePlatform，引用仍是原索引——可重试、无半状态。
	amdDesc, err := reg.SelectPlatform(ctx, "ns/app:1.0", manifest.Platform{OS: "linux", Architecture: "amd64"})
	if err != nil {
		t.Fatal(err)
	}
	_, mdata, err := manifest.GetManifest(ctx, store, amdDesc.Digest)
	if err != nil {
		t.Fatal(err)
	}
	m, _ := manifest.ParseManifest(mdata)
	lp, _ := store.Path(m.Layers[0].Digest)
	raw, err := os.ReadFile(lp)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lp, raw[:len(raw)-3], 0o644); err != nil {
		t.Fatal(err)
	}
	before, _ := reg.Resolve(ctx, "ns/app:1.0")

	_, err = committer.Commit(ctx, platform("linux", "amd64", "APP-AMD64"))
	if !errors.Is(err, commit.ErrCommitFailed) {
		t.Fatalf("损坏状态提交必须失败，got %v", err)
	}
	after, _ := reg.Resolve(ctx, "ns/app:1.0")
	if after.Digest != before.Digest {
		t.Fatal("提交失败后引用必须保持原索引")
	}
	t.Logf("判定: 损坏提交被整体拒绝，引用不动 %s", after.Digest.Short())

	// 截断在 verify 中必须归类为 size-mismatch。
	rep2, err := verify.New(store, reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range rep2.Findings {
		if f.Digest == m.Layers[0].Digest {
			found = true
			if f.Status != verify.StatusSizeMismatch {
				t.Fatalf("截断层应 size-mismatch，got %s (%s)", f.Status, f.Error)
			}
		}
	}
	if !found {
		t.Fatal("verify 结论中必须包含被截断层")
	}
	t.Logf("判定: verify 把截断层归类为 size-mismatch")

	// gc 面对损坏的被引用闭包必须中止（不误删）。
	if _, err := gc.New(store, reg).Run(ctx); err == nil {
		t.Fatal("引用闭包损坏时 gc 必须中止整轮")
	}

	// 修复后：重复平台提交得到 ErrDuplicatePlatform（语义性失败，引用不变），
	// 随后 gc 与 verify 全部恢复正常。
	if err := store.Delete(ctx, m.Layers[0].Digest); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, m.Layers[0].Digest, bytesReader(raw)); err != nil {
		t.Fatal(err)
	}
	_, err = committer.Commit(ctx, platform("linux", "amd64", "APP-AMD64"))
	if !errors.Is(err, registry.ErrDuplicatePlatform) || !errors.Is(err, commit.ErrCommitFailed) {
		t.Fatalf("修复后重复平台应 ErrDuplicatePlatform+ErrCommitFailed，got %v", err)
	}
	if _, err := gc.New(store, reg).Run(ctx); err != nil {
		t.Fatalf("修复后 gc 应正常完成: %v", err)
	}
	rep3, err := verify.New(store, reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if bad := rep3.Counts()[verify.StatusSizeMismatch] + rep3.Counts()[verify.StatusDigestMismatch] + rep3.Counts()[verify.StatusMissing]; bad != 0 {
		t.Fatalf("修复后不应有任何坏对象结论，实际 %d", bad)
	}
	all, _ := store.List(ctx)
	if len(all) != 7 {
		t.Fatalf("判定依据: 反复失败/重试/回收后占用应回落到可达集 7，实际 %d", len(all))
	}
	t.Logf("判定: 修复后链路恢复，占用回落到可达集 %d", len(all))
}
