package integration_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/assemble"
	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/commit"
	"github.com/h3yow/oci-image-pipeline-go/internal/gc"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
	"github.com/h3yow/oci-image-pipeline-go/internal/verify"
)

// 端到端巡检闭环：健康 -> 引用被改坏（损坏/超版本）-> verify 明确失败、
// 命令口径 Failed()=true、gc 中止 -> 重新提交修复 -> 巡检恢复全绿。
func TestVerifyDetectsAndRepairsBadReferences(t *testing.T) {
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

	spec := assembleSpec("linux", "amd64", []assemble.File{
		{Path: "bin/app", Data: []byte("APP"), Mode: 0o755},
	})
	if _, err := committer.Commit(ctx, commit.Request{Ref: "ns/app:1.0", Image: spec}); err != nil {
		t.Fatal(err)
	}

	// 健康基线：巡检不失败。
	if rep, err := verify.New(store, reg).Run(ctx); err != nil || rep.Failed() {
		t.Fatalf("健康仓库巡检应通过，err=%v failed=%v", err, rep.Failed())
	}

	refFile := filepath.Join(root, "refs", "ns", "app", "1.0")
	cases := []struct {
		name string
		raw  []byte
		want verify.RefStatus
	}{
		{"corrupt", []byte("{not readable json"), verify.StatusCorruptRef},
		{"unsupported-version", []byte(`{"formatVersion":42,"target":{}}`),
			verify.StatusUnsupportedRefVersion},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(refFile, tc.raw, 0o644); err != nil {
				t.Fatal(err)
			}
			rep, err := verify.New(store, reg).Run(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !rep.Failed() {
				t.Fatalf("%s 引用必须让巡检失败", tc.name)
			}
			var rf verify.RefFinding
			found := false
			for _, f := range rep.RefFindings {
				if f.Ref == "ns/app:1.0" {
					rf, found = f, true
				}
			}
			if !found || rf.Status != tc.want {
				t.Fatalf("%s 引用应报 %s，got %+v found=%v", tc.name, tc.want, rf, found)
			}
			// 判定口径与 registry 一致：gc 同样必须中止，绝不误删。
			_, gerr := gc.New(store, reg).Run(ctx)
			if gerr == nil {
				t.Fatalf("%s 引用下 gc 必须中止", tc.name)
			}
			if tc.want == verify.StatusUnsupportedRefVersion &&
				!errors.Is(gerr, registry.ErrUnsupportedReferenceVersion) {
				t.Fatalf("超版本引用的 gc 错误应可 errors.Is 到对应类别，got %v", gerr)
			}
			if tc.want == verify.StatusCorruptRef &&
				!errors.Is(gerr, registry.ErrCorruptRef) {
				t.Fatalf("损坏引用的 gc 错误应可 errors.Is 到 ErrCorruptRef，got %v", gerr)
			}
			t.Logf("判定: %s -> verify %s / gc 中止: %v", tc.name, rf.Status, gerr)
		})
	}

	// 修复：原样重新提交（同内容恒得同摘要），引用恢复健康。
	if _, err := committer.Commit(ctx, commit.Request{Ref: "ns/app:1.0", Image: spec}); err != nil {
		t.Fatalf("修复重试必须成功: %v", err)
	}
	rep, err := verify.New(store, reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Failed() {
		t.Fatalf("修复后巡检应恢复全绿，objects=%v refs=%+v", rep.Counts(), rep.RefFindings)
	}
	if _, err := gc.New(store, reg).Run(ctx); err != nil {
		t.Fatalf("修复后 gc 应正常完成: %v", err)
	}
	t.Log("判定: 坏引用修复后 verify 全绿、gc 正常")
}

// 静态保证：坏引用结论字符串能对上具体引用名，便于转出去排查。
func TestVerifyRefFindingCarriesName(t *testing.T) {
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
	if _, err := committer.Commit(ctx, commit.Request{
		Ref:   "ns/named:tag",
		Image: assembleSpec("linux", "amd64", []assemble.File{{Path: "a", Data: []byte("A")}}),
	}); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(root, "refs", "ns", "named", "tag")
	if err := os.WriteFile(p, []byte("###"), 0o644); err != nil {
		t.Fatal(err)
	}
	rep, err := verify.New(store, reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.RefFindings) != 1 {
		t.Fatalf("应恰好一条引用结论，got %+v", rep.RefFindings)
	}
	rf := rep.RefFindings[0]
	if rf.Ref != "ns/named:tag" || !strings.Contains(rf.Error, "ns/named:tag") {
		t.Fatalf("结论必须带上具体引用名，got %+v", rf)
	}
}
