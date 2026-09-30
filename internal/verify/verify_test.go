package verify_test

import (
	"bytes"
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

type env struct {
	dir   string
	store *cas.Store
	reg   *registry.Registry
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
	return &env{dir: dir, store: store, reg: reg}
}

// publishImage 构造并提交一个单平台镜像，返回清单/config/层描述符。
func (e *env) publishImage(t *testing.T, ref, marker string) (manifest.Descriptor, manifest.Descriptor, manifest.Descriptor) {
	t.Helper()
	ctx := context.Background()
	cfg := []byte(`{"marker":"` + marker + `"}`)
	cd, err := e.store.PutBytes(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	layer := []byte("layer-" + marker)
	ld, err := e.store.PutBytes(ctx, layer)
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
	mdesc, err := manifest.PutManifest(ctx, e.store, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.reg.Tag(ctx, ref, mdesc); err != nil {
		t.Fatal(err)
	}
	return mdesc,
		manifest.Descriptor{MediaType: manifest.MediaTypeConfig, Digest: cd, Size: int64(len(cfg))},
		manifest.Descriptor{MediaType: manifest.MediaTypeLayer, Digest: ld, Size: int64(len(layer))}
}

func statusOf(rep *verify.Report, d digest.Digest) (verify.Finding, bool) {
	for _, f := range rep.Findings {
		if f.Digest == d {
			return f, true
		}
	}
	return verify.Finding{}, false
}

// writeRawRef 绕过提交闸门直接落引用文件，模拟磁盘上既有的“指向缺失对象”引用。
func writeRawRef(t *testing.T, e *env, ref string, target manifest.Descriptor) {
	t.Helper()
	idx := -1
	for i := len(ref) - 1; i >= 0; i-- {
		if ref[i] == ':' {
			idx = i
			break
		}
	}
	if idx < 0 {
		t.Fatalf("bad ref %s", ref)
	}
	rel := filepath.Join(filepath.FromSlash(ref[:idx]), ref[idx+1:])
	p := filepath.Join(e.dir, "refs", rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	doc, err := json.Marshal(map[string]any{"target": target})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, doc, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyAllOK(t *testing.T) {
	e := newEnv(t)
	mdesc, cfg, layer := e.publishImage(t, "app:v1", "ok")
	rep, err := verify.New(e.store, e.reg).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []digest.Digest{mdesc.Digest, cfg.Digest, layer.Digest} {
		f, ok := statusOf(rep, d)
		if !ok || f.Status != verify.StatusOK {
			t.Fatalf("完整对象 %s 应为 ok，got %+v found=%v", d.Short(), f, ok)
		}
	}
	if rep.Counts()[verify.StatusOK] != 3 {
		t.Fatalf("判定依据: 3 个对象都应 ok，got %v", rep.Counts())
	}
	t.Logf("输入: 完整镜像 3 对象 -> 全部 ok")
}

// 同一份报告里逐对象区分：截断 / 等长篡改 / 被引用但缺失。
func TestVerifyClassifiesEachKind(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	_, cfg, layer := e.publishImage(t, "app:v1", "x")

	// 截断被引用层：长度与声明不符。
	lp, _ := e.store.Path(layer.Digest)
	good, err := os.ReadFile(lp)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lp, good[:len(good)-2], 0o644); err != nil {
		t.Fatal(err)
	}

	// 等长篡改 config：长度一致但摘要不符。
	cp, _ := e.store.Path(cfg.Digest)
	cgood, err := os.ReadFile(cp)
	if err != nil {
		t.Fatal(err)
	}
	tampered := append([]byte(nil), cgood...)
	tampered[len(tampered)-2] ^= 0xFF
	if err := os.WriteFile(cp, tampered, 0o644); err != nil {
		t.Fatal(err)
	}

	// 被清单声明但根本不存在的 config（绕过闸门直接落引用，模拟磁盘现状）。
	ld, err := e.store.PutBytes(ctx, []byte("ghost-layer"))
	if err != nil {
		t.Fatal(err)
	}
	missingCfg := manifest.Descriptor{
		MediaType: manifest.MediaTypeConfig,
		Digest:    digest.FromBytes([]byte("ghost-config")),
		Size:      100,
	}
	gm := &manifest.Manifest{
		Config: missingCfg,
		Layers: []manifest.Descriptor{{MediaType: manifest.MediaTypeLayer, Digest: ld, Size: int64(len("ghost-layer"))}},
	}
	gdata, err := manifest.EncodeManifest(gm)
	if err != nil {
		t.Fatal(err)
	}
	gmd, err := manifest.PutManifest(ctx, e.store, gdata)
	if err != nil {
		t.Fatal(err)
	}
	writeRawRef(t, e, "ghost:v1", gmd)

	rep, err := verify.New(e.store, e.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}

	f, _ := statusOf(rep, layer.Digest)
	if f.Status != verify.StatusSizeMismatch {
		t.Fatalf("截断层应 size-mismatch，got %s (%s)", f.Status, f.Error)
	}
	f, _ = statusOf(rep, cfg.Digest)
	if f.Status != verify.StatusDigestMismatch {
		t.Fatalf("篡改 config 应 digest-mismatch，got %s (%s)", f.Status, f.Error)
	}
	f, ok := statusOf(rep, missingCfg.Digest)
	if !ok || f.Status != verify.StatusMissing {
		t.Fatalf("缺失 config 应出现在结论中且为 missing，got %+v found=%v", f, ok)
	}
	if f.DeclaredSize != 100 || f.ActualSize != -1 {
		t.Fatalf("missing 结论应带 declared=100 actual=-1，got %+v", f)
	}
	t.Logf("判定: 截断=%s 篡改=%s 缺失=%s，三类逐对象可导出",
		verify.StatusSizeMismatch, verify.StatusDigestMismatch, verify.StatusMissing)
	for _, ff := range rep.Findings {
		t.Logf("  %-16s %s", ff.Status, ff.Digest.Short())
	}
}

// 孤立对象（无引用、无清单声明其大小）损坏：无大小声明，按摘要归类。
func TestVerifyOrphanTamper(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	data := []byte("orphan-blob-content-0123456789")
	d := digest.FromBytes(data)
	if err := e.store.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	tampered := []byte("orphan-blob-content-1123456789") // 等长、内容不同
	p, _ := e.store.Path(d)
	if err := os.WriteFile(p, tampered, 0o644); err != nil {
		t.Fatal(err)
	}
	rep, err := verify.New(e.store, e.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := statusOf(rep, d)
	if !ok {
		t.Fatal("孤立对象必须出现在结论中")
	}
	if f.Status != verify.StatusDigestMismatch || f.DeclaredSize != -1 {
		t.Fatalf("孤立损坏对象应 digest-mismatch 且无声明大小，got %+v", f)
	}
	t.Logf("判定: 孤立对象 %s 无声明大小，损坏归 %s", d.Short(), verify.StatusDigestMismatch)
}
