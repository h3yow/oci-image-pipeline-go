package verify_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/assemble"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/verify"
)

// refPath 把 name:tag 映射到引用文件路径（与 registry 内部布局一致）。
func refPath(t *testing.T, e *env, ref string) string {
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
	return filepath.Join(e.dir, "refs", rel)
}

// writeRefDoc 绕过提交闸门直接写引用文件，用于模拟磁盘上既有的坏引用。
func writeRefDoc(t *testing.T, e *env, ref string, doc any, raw bool) {
	t.Helper()
	p := refPath(t, e, ref)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	var b []byte
	if raw {
		b = []byte(doc.(string))
	} else {
		var err error
		b, err = json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func refStatus(rep *verify.Report, ref string, st verify.RefStatus) (verify.RefFinding, int) {
	n := 0
	var last verify.RefFinding
	for _, f := range rep.RefFindings {
		if f.Ref == ref && f.Status == st {
			n++
			last = f
		}
	}
	return last, n
}

// 引用文件被改成读不出来的垃圾：必须明确报 corrupt-ref，仓库不健康，
// 且结论里能对上具体引用名；绝不能逐条 OK、全绿。
func TestVerifyCorruptReferenceFails(t *testing.T) {
	e := newEnv(t)
	e.publishImage(t, "good:1", "g")
	writeRefDoc(t, e, "broken:1", "{this is not json", true)

	rep, err := verify.New(e.store, e.reg).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f, n := refStatus(rep, "broken:1", verify.RefStatusCorruptRef)
	if n != 1 {
		t.Fatalf("坏引用必须恰好 1 条 corrupt-ref 结论，got %d（全部: %+v）", n, rep.RefFindings)
	}
	if f.Where != "reference" || f.Error == "" {
		t.Fatalf("corrupt-ref 应带位置与原始错误，got %+v", f)
	}
	if _, okN := refStatus(rep, "good:1", verify.RefStatusOK); okN == 0 {
		t.Fatal("健康引用仍应报 OK，不能被坏引用连坐")
	}
	if rep.Healthy() {
		t.Fatal("判定依据: 存在读不出来的引用时巡检必须不健康（失败退出）")
	}
	t.Logf("判定: 坏引用 broken:1 -> %s（%s），报告不健康", f.Status, f.Error)
}

// 合法 JSON 但结构不合法（目标描述符缺要素）：同样是 corrupt-ref，
// 不伪装成“引用不存在”。
func TestVerifyStructurallyInvalidReferenceFails(t *testing.T) {
	e := newEnv(t)
	e.publishImage(t, "good:1", "g")
	// target 缺 size / mediaType，Descriptor.Validate 必拒。
	writeRefDoc(t, e, "bad:1", map[string]any{
		"formatVersion": 1,
		"target":        map[string]any{"digest": "sha256:" + string(make([]byte, 0))},
	}, false)

	rep, err := verify.New(e.store, e.reg).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, n := refStatus(rep, "bad:1", verify.RefStatusCorruptRef); n != 1 {
		t.Fatalf("结构不合法引用应 1 条 corrupt-ref，got %+v", rep.RefFindings)
	}
	if rep.Healthy() {
		t.Fatal("结构不合法引用必须让巡检失败")
	}
}

// 版本标记超出支持范围：明确报 unsupported-reference-version；
// 无版本字段的旧格式引用照常 OK，两种格式不混着解释。
func TestVerifyReferenceVersionBoundary(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	mdesc, _, _ := e.publishImage(t, "old:1", "o")
	e.publishImage(t, "future:1", "f")

	// old:1 改写成无 formatVersion 的旧格式。
	writeRawRef(t, e, "old:1", mdesc)
	// future:1 改写成 formatVersion=99。
	futurePath := refPath(t, e, "future:1")
	doc, err := os.ReadFile(futurePath)
	if err != nil {
		t.Fatal(err)
	}
	var rec map[string]any
	if err := json.Unmarshal(doc, &rec); err != nil {
		t.Fatal(err)
	}
	rec["formatVersion"] = 99
	writeRefDoc(t, e, "future:1", rec, false)

	rep, err := verify.New(e.store, e.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, okN := refStatus(rep, "old:1", verify.RefStatusOK); okN == 0 {
		t.Fatal("无 formatVersion 的旧引用必须照常可用（OK），got 报告不含 OK")
	}
	f, n := refStatus(rep, "future:1", verify.RefStatusUnsupportedVersion)
	if n != 1 {
		t.Fatalf("超版本引用必须 1 条 unsupported-reference-version，got %+v", rep.RefFindings)
	}
	if f.Error == "" {
		t.Fatal("超版本结论应带原始错误信息")
	}
	// 超版本引用绝不被当成 corrupt（两种格式不能混着解释）。
	if _, n := refStatus(rep, "future:1", verify.RefStatusCorruptRef); n != 0 {
		t.Fatal("超版本引用不得同时归类 corrupt-ref")
	}
	if rep.Healthy() {
		t.Fatal("超版本引用必须让巡检失败")
	}
	t.Logf("判定: 旧引用 OK；超版本 future:1 -> %s", f.Status)
}

// 引用闭包三类完整性结论逐类可导出：缺失 / 长度不符 / 内容被改，
// 且结论里带引用名、闭包位置与对象摘要。
func TestVerifyReferenceClosureFindings(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	// 缺失：引用指向的清单声明了一个不存在的层。
	missLayer := manifest.Descriptor{MediaType: manifest.MediaTypeLayer, Size: 11}
	missLayer.Digest = digestOf(t, []byte("definitely-absent-layer"))
	missM := &manifest.Manifest{
		Config: manifest.Descriptor{MediaType: manifest.MediaTypeConfig,
			Digest: digestOf(t, []byte("absent-cfg")), Size: 9},
		Layers: []manifest.Descriptor{missLayer},
	}
	missDesc := putManifest(t, e, missM)
	writeRawRef(t, e, "miss:1", missDesc)

	// 长度不符：层对象完整但引用清单声明了错误大小。
	truncCfg, truncLayer, truncM := buildParts(t, e, "trunc")
	wrong := truncLayer
	wrong.Size += 9 // 与实际长度不符
	truncM.Layers = []manifest.Descriptor{wrong}
	truncDesc := putManifest(t, e, truncM)
	writeRawRef(t, e, "trunc:1", truncDesc)
	_ = truncCfg

	// 内容被改：层长度不变但翻转一个字节。
	_, tamperLayer, tamperM := buildParts(t, e, "tamper")
	tp, _ := e.store.Path(tamperLayer.Digest)
	raw, err := os.ReadFile(tp)
	if err != nil {
		t.Fatal(err)
	}
	tampered := append([]byte(nil), raw...)
	tampered[0] ^= 0xFF
	if err := os.WriteFile(tp, tampered, 0o644); err != nil {
		t.Fatal(err)
	}
	tamperDesc := putManifest(t, e, tamperM)
	writeRawRef(t, e, "tamper:1", tamperDesc)

	rep, err := verify.New(e.store, e.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		ref   string
		st    verify.RefStatus
		where string
	}{
		{"miss:1", verify.RefStatusMissing, "target config"},
		{"trunc:1", verify.RefStatusSizeMismatch, "target layers[0]"},
		{"tamper:1", verify.RefStatusDigestMismatch, "target layers[0]"},
	}
	for _, c := range cases {
		var got []verify.RefFinding
		for _, f := range rep.RefFindings {
			if f.Ref == c.ref && f.Status == c.st {
				got = append(got, f)
			}
		}
		if len(got) == 0 {
			t.Fatalf("%s 必须出现 %s 结论，got %+v", c.ref, c.st, rep.RefFindings)
		}
		match := false
		for _, f := range got {
			if f.Where == c.where && f.Digest != "" {
				match = true
			}
			if f.Digest == "" {
				t.Fatalf("%s 结论必须带上出问题的对象摘要: %+v", c.ref, f)
			}
			t.Logf("判定: %-10s -> %-16s @ %s %s", c.ref, f.Status, f.Where, f.Digest.Short())
		}
		if !match {
			t.Fatalf("%s 的 %s 结论应包含位置 %s，got %+v", c.ref, c.st, c.where, got)
		}
	}
	// 三类 blob 级结论也要能逐条导出。
	if rep.Counts()[verify.StatusMissing] == 0 ||
		rep.Counts()[verify.StatusSizeMismatch] == 0 ||
		rep.Counts()[verify.StatusDigestMismatch] == 0 {
		t.Fatalf("blob 级三类结论都应可导出，got %v", rep.Counts())
	}
	if rep.Healthy() {
		t.Fatal("存在三类闭包问题时巡检必须失败")
	}
}

// digestOf 计算一段字节的摘要（测试辅助）。
func digestOf(t *testing.T, b []byte) digest.Digest {
	t.Helper()
	return digest.FromBytes(b)
}

// putManifest 编码清单写入 CAS 并返回描述符（测试辅助）。
func putManifest(t *testing.T, e *env, m *manifest.Manifest) manifest.Descriptor {
	t.Helper()
	data, err := manifest.EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	desc, err := manifest.PutManifest(context.Background(), e.store, data)
	if err != nil {
		t.Fatal(err)
	}
	return desc
}

// buildParts 构造 config+层 已写入、返回层描述符与内存清单的最小镜像，
// 供调用方进一步篡改（声明大小/文件内容）。
func buildParts(t *testing.T, e *env, marker string) (manifest.Descriptor, manifest.Descriptor, *manifest.Manifest) {
	t.Helper()
	ctx := context.Background()
	cfg := []byte(`{"marker":"` + marker + `"}`)
	cd, err := e.store.PutBytes(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	layer := []byte("layer-content-" + marker)
	ld, err := e.store.PutBytes(ctx, layer)
	if err != nil {
		t.Fatal(err)
	}
	cdesc := manifest.Descriptor{MediaType: manifest.MediaTypeConfig, Digest: cd, Size: int64(len(cfg))}
	ldesc := manifest.Descriptor{MediaType: manifest.MediaTypeLayer, Digest: ld, Size: int64(len(layer))}
	m := &manifest.Manifest{Config: cdesc, Layers: []manifest.Descriptor{ldesc}}
	return cdesc, ldesc, m
}

func imageSpec(marker string) assemble.ImageSpec {
	return assemble.ImageSpec{
		Config: map[string]any{"os": "linux", "architecture": "amd64", "marker": marker},
		Layers: []assemble.LayerSpec{{
			Name:  "rootfs",
			Files: []assemble.File{{Path: "app", Data: []byte("APP-" + marker), Mode: 0o755}},
		}},
	}
}

// 引用目标描述符声称是清单，对象内容却是 config 类 JSON（ParseKind 无法
// 识别为清单/索引）：闭包结构不合法，报 malformed，而不是笼统“坏了”，
// 更不能被当成“没找到”跳过。
func TestVerifyMalformedClosureIsClassified(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	// 写一段“非清单/索引”的 JSON 对象（这里是 config 形态），却用
	// manifest 媒体类型挂到引用上。
	cfgBytes := []byte(`{"architecture":"amd64","os":"linux"}`)
	d, err := e.store.PutBytes(ctx, cfgBytes)
	if err != nil {
		t.Fatal(err)
	}
	fakeManifest := manifest.Descriptor{
		MediaType: manifest.MediaTypeManifest,
		Digest:    d,
		Size:      int64(len(cfgBytes)),
	}
	writeRawRef(t, e, "wrong:1", fakeManifest)

	rep, err := verify.New(e.store, e.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f, n := refStatus(rep, "wrong:1", verify.RefStatusMalformed)
	if n == 0 {
		t.Fatalf("结构不合法闭包必须报 malformed，got %+v", rep.RefFindings)
	}
	if f.Digest != d {
		t.Fatalf("malformed 结论应带上对象摘要，got %s", f.Digest)
	}
	if rep.Healthy() {
		t.Fatal("结构不合法闭包必须让巡检失败")
	}
	t.Logf("判定: 类型对不上的引用 -> malformed @ %s (%s)", f.Where, f.Error)
}
