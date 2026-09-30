package verify_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
	"github.com/h3yow/oci-image-pipeline-go/internal/verify"
)

func setup(t *testing.T) (*cas.Store, *registry.Registry, context.Context) {
	t.Helper()
	store, err := cas.NewStore(filepath.Join(t.TempDir(), "repo"))
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.New(store)
	if err != nil {
		t.Fatal(err)
	}
	return store, reg, context.Background()
}

// writeImage 写一个最小单平台镜像（config+1层+清单），返回清单描述符。
func writeImage(t *testing.T, ctx context.Context, store *cas.Store, marker string) manifest.Descriptor {
	t.Helper()
	cfg := []byte(`{"marker":"` + marker + `"}`)
	cd, err := store.PutBytes(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	layer := []byte("layer-" + marker)
	ld, err := store.PutBytes(ctx, layer)
	if err != nil {
		t.Fatal(err)
	}
	m := &manifest.Manifest{
		Config: manifest.Descriptor{MediaType: manifest.MediaTypeConfig, Digest: cd, Size: int64(len(cfg))},
		Layers: []manifest.Descriptor{{MediaType: manifest.MediaTypeLayer, Digest: ld, Size: int64(len(layer))}},
	}
	data, err := manifest.EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	desc, err := manifest.PutManifest(ctx, store, data)
	if err != nil {
		t.Fatal(err)
	}
	return desc
}

func TestVerifyClassifiesPerObject(t *testing.T) {
	store, reg, ctx := setup(t)

	// 引用 app:good 指向完整镜像；app:bad 引用闭包里注入三种问题。
	good := writeImage(t, ctx, store, "good")
	if err := reg.Tag(ctx, "app:good", good); err != nil {
		t.Fatal(err)
	}

	bad := writeImage(t, ctx, store, "bad")
	if err := reg.Tag(ctx, "app:bad", bad); err != nil {
		t.Fatal(err)
	}

	// 解析 bad 清单，拿到 config 与层路径。
	_, mdata, err := manifest.GetManifest(ctx, store, bad.Digest)
	if err != nil {
		t.Fatal(err)
	}
	m, err := manifest.ParseManifest(mdata)
	if err != nil {
		t.Fatal(err)
	}

	// 1) 层：截断（实际长度 < 声明 size）。
	lp, _ := store.Path(m.Layers[0].Digest)
	orig, _ := os.ReadFile(lp)
	if err := os.WriteFile(lp, orig[:len(orig)-2], 0o644); err != nil {
		t.Fatal(err)
	}
	// 2) config：等长篡改。
	cp, _ := store.Path(m.Config.Digest)
	corig, _ := os.ReadFile(cp)
	tampered := append([]byte(nil), corig...)
	tampered[len(tampered)-2] = tampered[len(tampered)-2] + 1
	if err := os.WriteFile(cp, tampered, 0o644); err != nil {
		t.Fatal(err)
	}
	// 3) 再造一个被引用但缺失的对象：新清单引用一个“幽灵层”（不存在），
	//    config 必须保持完整，否则提交会先因 config 损坏被拒。
	ghost := "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	gcfg := []byte(`{"marker":"ghost"}`)
	gcd, err := store.PutBytes(ctx, gcfg)
	if err != nil {
		t.Fatal(err)
	}
	gm := &manifest.Manifest{
		Config: manifest.Descriptor{MediaType: manifest.MediaTypeConfig, Digest: gcd, Size: int64(len(gcfg))},
		Layers: []manifest.Descriptor{{
			MediaType: manifest.MediaTypeLayer,
			Digest:    digest.Digest(ghost), Size: 5,
		}},
	}
	gdata, err := manifest.EncodeManifest(gm)
	if err != nil {
		t.Fatal(err)
	}
	gdesc, err := manifest.PutManifest(ctx, store, gdata)
	if err != nil {
		t.Fatal(err)
	}
	// 直接落引用文件，模拟“绕过正常提交形成的存量坏引用”（如历史遗留/
	// 外部损坏）：引用指向的幽灵层根本不存在。
	writeRawRef(t, store.Root(), "app:ghost", gdesc)

	rep, err := verify.New(store, reg).Run(ctx)
	if err != nil {
		t.Fatalf("巡检本身不应因坏对象中止: %v", err)
	}
	if !rep.Failed() {
		t.Fatal("存在坏对象时 Report.Failed 必须为 true")
	}

	find := func(ref, digestS string, kind verify.Kind) bool {
		for _, f := range rep.Objects {
			if f.Ref == ref && f.Digest == digestS && f.Kind == kind {
				return true
			}
		}
		return false
	}

	// good 引用闭包全部 ok。
	if !find("app:good", string(good.Digest), verify.KindOK) {
		t.Fatal("app:good 根清单必须为 ok")
	}

	// bad：层截断、config 篡改，结论必须分开且都被逐对象列出。
	if !find("app:bad", string(m.Layers[0].Digest), verify.KindTruncated) {
		t.Fatalf("被截断层必须逐条标记 truncated，明细: %+v", rep.Objects)
	}
	if !find("app:bad", string(m.Config.Digest), verify.KindTampered) {
		t.Fatalf("等长篡改 config 必须逐条标记 tampered，明细: %+v", rep.Objects)
	}

	// ghost：不存在的层必须逐条标记 missing，而非被跳过。
	if !find("app:ghost", ghost, verify.KindMissing) {
		t.Fatalf("幽灵层必须逐条标记 missing（不得当作没找到跳过），明细: %+v", rep.Objects)
	}

	// 裸对象巡检同样必须能看到坏层/坏 config（按摘要口径为 tampered，
	// 因为没有描述符声明大小可对比）。
	if rep.Count(verify.KindTruncated)+rep.Count(verify.KindTampered)+rep.Count(verify.KindMissing) == 0 {
		t.Fatal("至少应出现一类非 ok 结论")
	}

	t.Logf("巡检结论计数: ok=%d missing=%d truncated=%d tampered=%d",
		rep.Count(verify.KindOK), rep.Count(verify.KindMissing),
		rep.Count(verify.KindTruncated), rep.Count(verify.KindTampered))

}

// writeRawRef 直接按磁盘布局写引用文件（绕过提交保护），模拟存量坏引用。
func writeRawRef(t *testing.T, root, ref string, desc manifest.Descriptor) {
	t.Helper()
	name, tag, ok := splitRef(ref)
	if !ok {
		t.Fatalf("bad ref %q", ref)
	}
	p := filepath.Join(root, "refs", filepath.FromSlash(name), tag)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	doc, err := json.Marshal(struct {
		FormatVersion int                 `json:"formatVersion,omitempty"`
		Target        manifest.Descriptor `json:"target"`
	}{FormatVersion: 1, Target: desc})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, append(doc, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func splitRef(ref string) (string, string, bool) {
	for i := len(ref) - 1; i >= 0; i-- {
		if ref[i] == ':' {
			return ref[:i], ref[i+1:], i > 0 && i < len(ref)-1
		}
	}
	return "", "", false
}
