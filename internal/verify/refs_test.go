package verify_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/assemble"
	"github.com/h3yow/oci-image-pipeline-go/internal/commit"
	"github.com/h3yow/oci-image-pipeline-go/internal/gc"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
	"github.com/h3yow/oci-image-pipeline-go/internal/verify"
)

// refPath 返回引用文件的磁盘路径（仅供测试制造磁盘现状）。
func refPath(e *env, ref string) string {
	name, tag, ok := strings.Cut(ref, ":")
	if !ok {
		panic("bad ref " + ref)
	}
	return filepath.Join(e.dir, "refs", filepath.FromSlash(name), tag)
}

// writeRefDoc 以自定义结构（可指定 formatVersion / target）直接落引用文件。
func writeRefDoc(t *testing.T, e *env, ref string, doc map[string]any) {
	t.Helper()
	p := refPath(e, ref)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// writeRefBytes 直接写入引用文件原始字节（用于制造读不出来的损坏引用）。
func writeRefBytes(t *testing.T, e *env, ref string, raw []byte) {
	t.Helper()
	p := refPath(e, ref)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func refFinding(rep *verify.Report, ref string) (verify.RefFinding, bool) {
	for _, rf := range rep.RefFindings {
		if rf.Ref == ref {
			return rf, true
		}
	}
	return verify.RefFinding{}, false
}

// commitSpec 构造一个单平台镜像 spec（内容随 marker）。
func commitSpec(marker string) assemble.ImageSpec {
	return assemble.ImageSpec{
		Config: map[string]any{"os": "linux", "architecture": "amd64", "marker": marker},
		Layers: []assemble.LayerSpec{{
			Name:  "rootfs",
			Files: []assemble.File{{Path: "app", Data: []byte("APP-" + marker), Mode: 0o755}},
		}},
	}
}

// 引用文件读不出来（根本不是 JSON）：必须明确报 corrupt-ref、巡检失败，
// 而不是逐条 OK、全绿退出。
func TestVerifyCorruptRefReportedAndFails(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publishImage(t, "ns/app:1.0", "ok")

	writeRefBytes(t, e, "ns/app:1.0", []byte("this is {{{ not json at all"))
	rep, err := verify.New(e.store, e.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rf, ok := refFinding(rep, "ns/app:1.0")
	if !ok {
		t.Fatalf("损坏引用必须出现在 RefFindings 中，got %+v", rep.RefFindings)
	}
	if rf.Status != verify.StatusCorruptRef {
		t.Fatalf("读不出来的引用应 %s，got %s", verify.StatusCorruptRef, rf.Status)
	}
	if !rep.Failed() {
		t.Fatal("存在损坏引用时巡检必须判定失败，不能给绿色结论")
	}
	t.Logf("判定: 损坏引用 ns/app:1.0 -> %s: %s", rf.Status, rf.Error)
}

// 引用结构自相矛盾（target 描述符摘要非法）：同样归 corrupt-ref，
// 不能伪装成“引用不存在”而被跳过。
func TestVerifyRefWithIllegalTargetIsCorrupt(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publishImage(t, "ns/app:1.0", "ok")

	writeRefDoc(t, e, "ns/app:1.0", map[string]any{
		"formatVersion": 1,
		"target": map[string]any{
			"mediaType": manifest.MediaTypeManifest,
			"digest":    "not-a-digest",
			"size":      3,
		},
	})
	rep, err := verify.New(e.store, e.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rf, ok := refFinding(rep, "ns/app:1.0")
	if !ok || rf.Status != verify.StatusCorruptRef {
		t.Fatalf("非法目标引用应 %s，got %+v ok=%v", verify.StatusCorruptRef, rf, ok)
	}
	if !rep.Failed() {
		t.Fatal("非法目标引用必须使巡检失败")
	}
	t.Logf("判定: 非法目标引用 -> %s", rf.Status)
}

// formatVersion 超出支持范围：必须单独报 unsupported-ref-version，与
// 损坏引用区分，且绝不与旧格式（无字段）混着解释。
func TestVerifyUnsupportedRefVersionRejected(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	mdesc, _, _ := e.publishImage(t, "ns/app:1.0", "ok")

	writeRefDoc(t, e, "ns/app:1.0", map[string]any{
		"formatVersion": 99, // 来自更新的格式，本系统不支持
		"target":        mdesc,
	})
	rep, err := verify.New(e.store, e.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rf, ok := refFinding(rep, "ns/app:1.0")
	if !ok {
		t.Fatal("超版本引用必须出现在 RefFindings 中")
	}
	if rf.Status != verify.StatusUnsupportedRefVersion {
		t.Fatalf("超版本引用应 %s，got %s", verify.StatusUnsupportedRefVersion, rf.Status)
	}
	if !rep.Failed() {
		t.Fatal("超版本引用必须使巡检失败退出")
	}
	if !strings.Contains(rf.Error, "99") {
		t.Fatalf("结论应能对上具体版本号，got %s", rf.Error)
	}
	t.Logf("判定: 超版本引用 -> %s: %s", rf.Status, rf.Error)
}

// 没有 formatVersion 的旧格式引用：继续照常可用，巡检全 OK。
func TestVerifyLegacyRefStillVerifiesOK(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	mdesc, _, _ := e.publishImage(t, "ns/app:1.0", "ok")

	// 去掉 formatVersion：最早格式的引用文件。
	writeRefDoc(t, e, "ns/app:1.0", map[string]any{"target": mdesc})

	rep, err := verify.New(e.store, e.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.RefFindings) != 0 {
		t.Fatalf("旧格式引用不应产生引用结论，got %+v", rep.RefFindings)
	}
	if rep.Failed() {
		t.Fatalf("旧格式引用必须照常可用，got %v", rep.Counts())
	}
	for _, f := range rep.Findings {
		if f.Status != verify.StatusOK {
			t.Fatalf("旧格式引用闭包应全部 OK，%s got %s", f.Digest.Short(), f.Status)
		}
	}
	t.Log("判定: 无 formatVersion 旧引用照常可用，闭包全部 ok")
}

// 引用指向的目标对象根本不存在：missing 结论必须带上是哪条引用、
// 哪个位置，巡检失败。
func TestVerifyRefTargetMissingClassified(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	missing := manifest.Descriptor{
		MediaType: manifest.MediaTypeManifest,
		Digest:    "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		Size:      10,
	}
	writeRawRef(t, e, "ghost:v1", missing)

	rep, err := verify.New(e.store, e.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := statusOf(rep, missing.Digest)
	if !ok || f.Status != verify.StatusMissing {
		t.Fatalf("缺失目标应 missing，got %+v found=%v", f, ok)
	}
	if len(f.ReferencedBy) != 1 || f.ReferencedBy[0] != "ghost:v1" {
		t.Fatalf("missing 结论应标明来自 ghost:v1，got %v", f.ReferencedBy)
	}
	if f.Location == "" {
		t.Fatal("missing 结论应带位置信息")
	}
	if !rep.Failed() {
		t.Fatal("引用目标缺失时巡检必须失败")
	}
	t.Logf("判定: 缺失目标 %s via %v at %s", f.Digest.Short(), f.ReferencedBy, f.Location)
}

// 经“正常提交的引用”闭包：截断层归 size-mismatch、等长篡改 config 归
// digest-mismatch，且结论都能对上具体引用，与读取/回收/提交门同口径。
func TestVerifyPublishedRefClosureClassifications(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	_, cfg, layer := e.publishImage(t, "ns/app:1.0", "x")

	lp, _ := e.store.Path(layer.Digest)
	good, err := os.ReadFile(lp)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lp, good[:len(good)-2], 0o644); err != nil {
		t.Fatal(err)
	}
	cp, _ := e.store.Path(cfg.Digest)
	cb, err := os.ReadFile(cp)
	if err != nil {
		t.Fatal(err)
	}
	tampered := append([]byte(nil), cb...)
	tampered[len(tampered)-2] ^= 0xFF
	if err := os.WriteFile(cp, tampered, 0o644); err != nil {
		t.Fatal(err)
	}

	rep, err := verify.New(e.store, e.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	lf, _ := statusOf(rep, layer.Digest)
	if lf.Status != verify.StatusSizeMismatch {
		t.Fatalf("截断层应 size-mismatch，got %s", lf.Status)
	}
	if len(lf.ReferencedBy) != 1 || lf.ReferencedBy[0] != "ns/app:1.0" {
		t.Fatalf("截断层结论应标明引用 ns/app:1.0，got %v", lf.ReferencedBy)
	}
	cf, _ := statusOf(rep, cfg.Digest)
	if cf.Status != verify.StatusDigestMismatch {
		t.Fatalf("篡改 config 应 digest-mismatch，got %s", cf.Status)
	}
	if len(cf.ReferencedBy) != 1 || cf.ReferencedBy[0] != "ns/app:1.0" {
		t.Fatalf("篡改结论应标明引用 ns/app:1.0，got %v", cf.ReferencedBy)
	}
	if len(rep.RefFindings) != 0 {
		t.Fatalf("引用本身没坏，不应有引用结论，got %+v", rep.RefFindings)
	}
	if !rep.Failed() {
		t.Fatal("闭包中存在损坏时巡检必须失败")
	}
	t.Log("判定: 截断/篡改均经引用闭包归类且可对上引用")
}

// 并发不变量：当某条引用处于损坏/超版本状态时，无论提交、回收、巡检
// 如何并发，巡检绝不能给出绿色结论；引用健康窗口里也不能误报对象问题。
func TestVerifyConcurrentRefCorruptionNeverGreen(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	committer := commit.New(e.store, e.reg)

	if _, err := committer.Commit(ctx, commit.Request{Ref: "work:latest", Image: commitSpec("work")}); err != nil {
		t.Fatal(err)
	}
	if _, err := committer.Commit(ctx, commit.Request{Ref: "other:latest", Image: commitSpec("other")}); err != nil {
		t.Fatal(err)
	}

	const rounds = 40
	var wg sync.WaitGroup
	problems := make(chan error, rounds*4)

	// 引用损坏方：反复把 work:latest 在“损坏/超版本/健康”之间切换。
	wg.Add(1)
	go func() {
		defer wg.Done()
		p := refPath(e, "work:latest")
		good, err := os.ReadFile(p)
		if err != nil {
			problems <- err
			return
		}
		for i := 0; i < rounds; i++ {
			if i%2 == 0 {
				if err := os.WriteFile(p, []byte("{broken json"), 0o644); err != nil {
					problems <- err
					return
				}
			} else {
				doc := []byte(`{"formatVersion":7,"target":{}}`)
				if err := os.WriteFile(p, doc, 0o644); err != nil {
					problems <- err
					return
				}
			}
			// 让损坏态短暂停留后恢复为健康引用。
			for j := 0; j < 100; j++ {
			}
			if err := os.WriteFile(p, good, 0o644); err != nil {
				problems <- err
				return
			}
		}
	}()

	// 巡检方。
	for i := 0; i < rounds; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rep, err := verify.New(e.store, e.reg).Run(ctx)
			if err != nil {
				problems <- err
				return
			}
			rf, sawBadRef := refFinding(rep, "work:latest")
			switch {
			case sawBadRef:
				if rf.Status != verify.StatusCorruptRef && rf.Status != verify.StatusUnsupportedRefVersion {
					problems <- &unexpectedRefStatus{st: string(rf.Status)}
					return
				}
				if !rep.Failed() {
					problems <- errGreenWithBadRef{}
				}
			default:
				// 引用此刻健康：对象不能误报（尤其 work 闭包），引用无残留结论。
				for _, f := range rep.Findings {
					if f.Status != verify.StatusOK {
						for _, r := range f.ReferencedBy {
							if r == "work:latest" {
								problems <- errFalseBadObject{st: f.Status, ref: r}
								return
							}
						}
					}
				}
			}
		}()
	}

	// 回收方：损坏引用期间必须中止且只能因“引用无法解析”中止，不得误删。
	for i := 0; i < rounds/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := gc.New(e.store, e.reg).Run(ctx); err != nil {
				if !(errors.Is(err, registry.ErrCorruptRef) ||
					errors.Is(err, registry.ErrUnsupportedReferenceVersion) ||
					strings.Contains(err.Error(), "resolve")) {
					problems <- errUnexpectedGC{err: err}
				}
			}
		}()
	}

	// 提交方：持续向独立引用提交同内容镜像，不应受影响。
	for i := 0; i < rounds/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := committer.Commit(ctx,
				commit.Request{Ref: "other:latest", Image: commitSpec("other")}); err != nil {
				problems <- errUnexpectedCommit{err: err}
			}
		}()
	}

	wg.Wait()
	close(problems)
	for err := range problems {
		t.Error(err)
	}

	// 最终恢复健康引用：巡检必须全绿。
	if _, err := committer.Commit(ctx, commit.Request{Ref: "work:latest", Image: commitSpec("work")}); err != nil {
		t.Fatal(err)
	}
	rep, err := verify.New(e.store, e.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Failed() {
		t.Fatalf("修复后巡检必须全绿，objects=%v refs=%+v", rep.Counts(), rep.RefFindings)
	}
	t.Log("判定: 并发提交/回收/巡检下，坏引用从不给绿；修复后恢复全绿")
}

type errGreenWithBadRef struct{}

func (errGreenWithBadRef) Error() string {
	return "巡检列出了坏引用却判定健康（绿色）"
}

type errFalseBadObject struct {
	st  verify.Status
	ref string
}

func (e errFalseBadObject) Error() string {
	return "引用健康窗口里巡检误报对象 " + string(e.st) + " via " + e.ref
}

type unexpectedRefStatus struct{ st string }

func (e *unexpectedRefStatus) Error() string { return "坏引用结论类别异常: " + e.st }

type errUnexpectedGC struct{ err error }

func (e errUnexpectedGC) Error() string {
	return "回收因非引用解析原因失败: " + e.err.Error()
}

type errUnexpectedCommit struct{ err error }

func (e errUnexpectedCommit) Error() string { return "并发提交意外失败: " + e.err.Error() }

// 引用目标对象字节完整（摘要、长度都对），但内容根本不是合法清单：
// 归 malformed，巡检失败——结构问题不能被当成 OK。
func TestVerifyMalformedReferencedDocument(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	d, err := e.store.PutBytes(ctx, []byte(`{"config":}`)) // 字节合法、JSON 非法
	if err != nil {
		t.Fatal(err)
	}
	raw, err := e.store.GetBytes(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	target := manifest.Descriptor{MediaType: manifest.MediaTypeManifest, Digest: d, Size: int64(len(raw))}
	writeRawRef(t, e, "bad:v1", target)

	rep, err := verify.New(e.store, e.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := statusOf(rep, d)
	if !ok || f.Status != verify.StatusMalformed {
		t.Fatalf("结构非法文档应 malformed，got %+v found=%v", f, ok)
	}
	if len(f.ReferencedBy) != 1 || f.ReferencedBy[0] != "bad:v1" {
		t.Fatalf("malformed 结论应标明引用 bad:v1，got %v", f.ReferencedBy)
	}
	if !rep.Failed() {
		t.Fatal("结构非法文档必须使巡检失败")
	}
	t.Logf("判定: 结构非法文档 %s -> malformed", d.Short())
}

// 引用目标对象字节完整，但用的是本系统不支持的清单格式：
// 归 unsupported-format，明确拒绝而非猜测解释。
func TestVerifyUnsupportedReferencedFormat(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	// schemaVersion=1 / Docker 风格 mediaType：ParseKind 判超范围。
	doc := []byte(`{"schemaVersion":1,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{},"layers":[]}`)
	d, err := e.store.PutBytes(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	target := manifest.Descriptor{MediaType: manifest.MediaTypeManifest, Digest: d, Size: int64(len(doc))}
	writeRawRef(t, e, "docker:v1", target)

	rep, err := verify.New(e.store, e.reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := statusOf(rep, d)
	if !ok || f.Status != verify.StatusUnsupportedFormat {
		t.Fatalf("超范围格式应 unsupported-format，got %+v found=%v", f, ok)
	}
	if len(f.ReferencedBy) != 1 || f.ReferencedBy[0] != "docker:v1" {
		t.Fatalf("unsupported-format 结论应标明引用 docker:v1，got %v", f.ReferencedBy)
	}
	if !rep.Failed() {
		t.Fatal("超范围格式必须使巡检失败")
	}
	t.Logf("判定: 超范围格式文档 %s -> unsupported-format", d.Short())
}
