package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/assemble"
	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
	"github.com/h3yow/oci-image-pipeline-go/internal/verify"
)

// writeRefFile 绕过提交闸门直接写引用文件（模拟磁盘上被手工改坏的引用）。
func writeRefFile(t *testing.T, root, ref string, b []byte) {
	t.Helper()
	idx := 0
	for i := len(ref) - 1; i >= 0; i-- {
		if ref[i] == ':' {
			idx = i
			break
		}
	}
	p := filepath.Join(root, "refs", filepath.FromSlash(ref[:idx]), ref[idx+1:])
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// 端到端：健康索引引用 + 读不出来的引用 + 超版本引用并存时，verify 必须
// 逐条给出可区分结论、报告不健康（命令据此失败退出），且健康引用不被连坐。
func TestVerifyFlagsCorruptAndFutureReferences(t *testing.T) {
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

	files := []assemble.File{{Path: "bin/app", Data: []byte("APP-AMD64"), Mode: 0o755}}
	asm := assemble.New(store)
	md, _, err := asm.AssembleImage(ctx, assembleSpec("linux", "amd64", files))
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.AttachPlatform(ctx, "ns/app:1.0", md,
		manifest.Platform{OS: "linux", Architecture: "amd64"}); err != nil {
		t.Fatal(err)
	}

	// 坏引用：引用文件被改成读不出来的垃圾。
	writeRefFile(t, root, "ns/broken:1", []byte("{not-json"))

	// 超版本引用：在健康目标上把 formatVersion 改成 99。
	writeRefFile(t, root, "ns/future:1", []byte(`{
		"formatVersion": 99,
		"target": {"mediaType": "`+md.MediaType+`", "digest": "`+string(md.Digest)+`", "size": `+strconv.FormatInt(md.Size, 10)+`}
	}`))

	rep, err := verify.New(store, reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Healthy() {
		t.Fatal("判定依据: 存在坏引用/超版本引用时巡检绝不允许全绿")
	}
	counts := map[verify.RefStatus]int{}
	for _, f := range rep.RefFindings {
		counts[f.Status]++
		if f.Ref == "ns/app:1.0" && f.Status != verify.RefStatusOK {
			t.Fatalf("健康索引引用不应连坐，got %s @ %s", f.Status, f.Where)
		}
	}
	if counts[verify.RefStatusCorruptRef] != 1 {
		t.Fatalf("应有 1 条 corrupt-ref，got %d (%+v)", counts[verify.RefStatusCorruptRef], rep.RefFindings)
	}
	if counts[verify.RefStatusUnsupportedVersion] != 1 {
		t.Fatalf("应有 1 条 unsupported-reference-version，got %d", counts[verify.RefStatusUnsupportedVersion])
	}
	if counts[verify.RefStatusOK] < 1 {
		t.Fatal("健康索引引用应给出 OK 结论")
	}
	for _, f := range rep.RefFindings {
		if f.Status != verify.RefStatusOK {
			t.Logf("坏引用结论: %s -> %s (%s)", f.Ref, f.Status, f.Error)
		}
	}
}
