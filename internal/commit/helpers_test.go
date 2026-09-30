package commit_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
	"github.com/h3yow/oci-image-pipeline-go/internal/verify"
)

// errVerifySaw 校验者在无任何人为破坏的并发场景里看到了非正常结论。
func errVerifySaw(st verify.Status, n int) error {
	return fmt.Errorf("verify 在并发期间发现 %d 个 %s 结论（无破坏场景下应为 0）", n, st)
}

// rewriteLegacy 把引用文件改写成最早的“无 formatVersion 字段”旧格式，
// 验证旧格式引用在提交/回收/校验链路中照常可用。
func rewriteLegacy(t *testing.T, store *cas.Store, ref string, target manifest.Descriptor) {
	t.Helper()
	name, tag, ok := strings.Cut(ref, ":")
	if !ok {
		t.Fatalf("bad ref %s", ref)
	}
	p := filepath.Join(store.Root(), "refs", filepath.FromSlash(name), tag)
	doc, err := json.Marshal(map[string]any{"target": target})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, doc, 0o644); err != nil {
		t.Fatal(err)
	}
	// 立刻经正式读取路径确认旧格式能解析。
	reg, err := registry.New(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Resolve(context.Background(), ref); err != nil {
		t.Fatalf("旧格式引用写回后应可解析: %v", err)
	}
}
