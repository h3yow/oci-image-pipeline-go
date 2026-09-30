package commit_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/assemble"
	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/commit"
	"github.com/h3yow/oci-image-pipeline-go/internal/gc"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
	"github.com/h3yow/oci-image-pipeline-go/internal/verify"
)

func newEnv(t *testing.T) (*cas.Store, *registry.Registry, *commit.Committer) {
	t.Helper()
	store, err := cas.NewStore(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.New(store)
	if err != nil {
		t.Fatal(err)
	}
	return store, reg, commit.New(store, reg)
}

func specFor(marker, osName, arch string) assemble.ImageSpec {
	return assemble.ImageSpec{
		Config: map[string]any{"os": osName, "architecture": arch, "marker": marker},
		Layers: []assemble.LayerSpec{{
			Name:  "rootfs",
			Files: []assemble.File{{Path: "app", Data: []byte("APP-" + marker), Mode: 0o755}},
		}},
	}
}

// 成功提交后：引用可达闭包必须完整可用（resolve/读 config/读层都成功）。
func TestCommitSuccessPublishesUsableImage(t *testing.T) {
	store, reg, c := newEnv(t)
	ctx := context.Background()
	res, err := c.Commit(ctx, commit.Request{
		Ref:   "ns/app:1.0",
		Image: specFor("amd64", "linux", "amd64"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("提交成功 manifest=%s target=%s", res.Manifest.Digest.Short(), res.Target.Digest.Short())

	desc, err := reg.Resolve(ctx, "ns/app:1.0")
	if err != nil {
		t.Fatal(err)
	}
	if desc.Digest != res.Manifest.Digest {
		t.Fatalf("引用应指向提交的清单 %s，实际 %s", res.Manifest.Digest.Short(), desc.Digest.Short())
	}
	blobs, err := reg.ReferencedBlobs(ctx, "ns/app:1.0")
	if err != nil {
		t.Fatal(err)
	}
	for d := range blobs {
		if _, err := store.Stat(ctx, d); err != nil {
			t.Fatalf("提交后引用闭包中的对象 %s 必须完整可读: %v", d.Short(), err)
		}
	}
}

// 提交失败（闭包不完整）：引用保持原状，且错误能区分具体原因；重试可成功。
//
// 这里分别覆盖两条失败路径：
//  1. 组装阶段发现同名对象损坏 -> ErrCommitFailed + ErrDigestMismatch；
//  2. 发布前闸门发现描述符声明大小与实际不符（截断）->
//     ErrCommitFailed + ErrSizeMismatch（对象字节其实完整，是引用它的
//     清单声明了错误大小，模拟“按错误清单提交”）。
func TestCommitFailureIsAtomicAndRetriable(t *testing.T) {
	store, reg, c := newEnv(t)
	ctx := context.Background()

	// 先有一个健康的旧版本引用。
	old, err := c.Commit(ctx, commit.Request{Ref: "app:v", Image: specFor("v0", "linux", "amd64")})
	if err != nil {
		t.Fatal(err)
	}

	// 路径 1：组装新版本，破坏其层文件，再 Commit：组装写层时即被拒，
	// 原因是损坏（按摘要口径 digest-mismatch），引用不变。
	newSpec := specFor("v1", "linux", "amd64")
	mdesc, _, err := assemble.New(store).AssembleImage(ctx, newSpec)
	if err != nil {
		t.Fatal(err)
	}
	_, mdata, err := manifest.GetManifest(ctx, store, mdesc.Digest)
	if err != nil {
		t.Fatal(err)
	}
	m, err := manifest.ParseManifest(mdata)
	if err != nil {
		t.Fatal(err)
	}
	lp, _ := store.Path(m.Layers[0].Digest)
	good, err := os.ReadFile(lp)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lp, good[:len(good)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 新镜像层被截断 %s，尝试提交 app:v", m.Layers[0].Digest.Short())

	_, err = c.Commit(ctx, commit.Request{Ref: "app:v", Image: newSpec})
	if err == nil {
		t.Fatal("判定依据: 闭包不完整时提交必须整体失败")
	}
	if !errors.Is(err, commit.ErrCommitFailed) || !errors.Is(err, cas.ErrCorrupt) {
		t.Fatalf("组装阶段损坏应 ErrCommitFailed+ErrCorrupt，got %v", err)
	}
	t.Logf("判定: 组装阶段失败原因可区分 -> %v", err)
	cur, err := reg.Resolve(ctx, "app:v")
	if err != nil {
		t.Fatal(err)
	}
	if cur.Digest != old.Manifest.Digest {
		t.Fatalf("失败后引用必须保持旧版本，got %s want %s", cur.Digest.Short(), old.Manifest.Digest.Short())
	}

	// 修复层（先删除损坏对象，再以正确字节重写）后重试：必须成功且幂等。
	if err := store.Delete(ctx, m.Layers[0].Digest); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, m.Layers[0].Digest, bytes.NewReader(good)); err != nil {
		t.Fatal(err)
	}
	retried, err := c.Commit(ctx, commit.Request{Ref: "app:v", Image: newSpec})
	if err != nil {
		t.Fatalf("修复后重试必须成功，got %v", err)
	}
	if retried.Manifest.Digest != mdesc.Digest {
		t.Fatalf("重试应得到相同镜像摘要（幂等组装），got %s want %s",
			retried.Manifest.Digest.Short(), mdesc.Digest.Short())
	}

	// 路径 2：构造一个“层声明大小错误”的清单（层对象本身完整），
	// 经 Publish 提交：闸门按声明大小发现 size-mismatch，拒绝且引用不变。
	badSize := *m
	badSize.Layers = []manifest.Descriptor{{
		MediaType: m.Layers[0].MediaType,
		Digest:    m.Layers[0].Digest,
		Size:      m.Layers[0].Size + 7, // 与实际长度不符
	}}
	badBytes, err := manifest.EncodeManifest(&badSize)
	if err != nil {
		t.Fatal(err)
	}
	badDesc, err := manifest.PutManifest(ctx, store, badBytes)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Publish(ctx, "app:v", badDesc, manifest.Platform{}, false)
	if !errors.Is(err, commit.ErrCommitFailed) || !errors.Is(err, cas.ErrSizeMismatch) {
		t.Fatalf("声明大小不符应 ErrCommitFailed+ErrSizeMismatch，got %v", err)
	}
	cur2, _ := reg.Resolve(ctx, "app:v")
	if cur2.Digest != retried.Manifest.Digest {
		t.Fatal("闸门拒绝后引用必须保持上一个成功版本")
	}
	t.Logf("判定: 发布前闸门截断分类=%v，引用仍为 %s", err, cur2.Digest.Short())
}

// 缺失对象导致的提交失败应归类 ErrNotFound，而不是笼统失败。
func TestCommitMissingBlobClassified(t *testing.T) {
	store, reg, c := newEnv(t)
	ctx := context.Background()
	spec := specFor("miss", "linux", "amd64")
	mdesc, _, err := assemble.New(store).AssembleImage(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	_, mdata, _ := manifest.GetManifest(ctx, store, mdesc.Digest)
	m, _ := manifest.ParseManifest(mdata)
	// 删除层，制造“清单声明但对象不存在”。
	if err := store.Delete(ctx, m.Layers[0].Digest); err != nil {
		t.Fatal(err)
	}
	_, err = c.Publish(ctx, "app:z", mdesc, manifest.Platform{}, false)
	if !errors.Is(err, commit.ErrCommitFailed) || !errors.Is(err, cas.ErrNotFound) {
		t.Fatalf("缺失闭包应 ErrCommitFailed+ErrNotFound，got %v", err)
	}
	if _, rerr := reg.Resolve(ctx, "app:z"); !errors.Is(rerr, registry.ErrNoSuchRef) {
		t.Fatalf("失败后引用应像没动过（不存在），got %v", rerr)
	}
	t.Logf("判定: 缺失对象提交失败原因=%v，引用未创建", err)

	// 等长篡改的发布失败应归类 digest-mismatch（层在、长度对、内容错）。
	m2, _, err := assemble.New(store).AssembleImage(ctx, specFor("tamper", "linux", "amd64"))
	if err != nil {
		t.Fatal(err)
	}
	_, d2, _ := manifest.GetManifest(ctx, store, m2.Digest)
	mm2, _ := manifest.ParseManifest(d2)
	lp, _ := store.Path(mm2.Layers[0].Digest)
	raw, err := os.ReadFile(lp)
	if err != nil {
		t.Fatal(err)
	}
	tampered := append([]byte(nil), raw...)
	tampered[0] ^= 0xFF
	if err := os.WriteFile(lp, tampered, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = c.Publish(ctx, "app:t", m2, manifest.Platform{}, false)
	if !errors.Is(err, commit.ErrCommitFailed) || !errors.Is(err, cas.ErrDigestMismatch) {
		t.Fatalf("等长篡改应 ErrCommitFailed+ErrDigestMismatch，got %v", err)
	}
	t.Logf("判定: 等长篡改提交失败原因=%v", err)
}

// AsIndex 提交：多平台成功；重复平台失败且引用不变；旧版本无版本字段
// 的引用文件在链路中照常可解析。
func TestCommitIndexDuplicateAndLegacyRef(t *testing.T) {
	store, reg, c := newEnv(t)
	ctx := context.Background()
	p1 := commit.Request{Ref: "img:t", Image: specFor("a", "linux", "amd64"),
		Platform: manifest.Platform{OS: "linux", Architecture: "amd64"}, AsIndex: true}
	p2 := commit.Request{Ref: "img:t", Image: specFor("b", "linux", "arm64"),
		Platform: manifest.Platform{OS: "linux", Architecture: "arm64"}, AsIndex: true}
	if _, err := c.Commit(ctx, p1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Commit(ctx, p2); err != nil {
		t.Fatal(err)
	}
	got, err := reg.SelectPlatform(ctx, "img:t", manifest.Platform{OS: "linux", Architecture: "arm64"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("判定: 索引提交后 arm64 -> %s", got.Digest.Short())

	// 重复平台提交：ErrCommitFailed + ErrDuplicatePlatform，引用不动。
	before, _ := reg.Resolve(ctx, "img:t")
	_, err = c.Commit(ctx, p1)
	if !errors.Is(err, commit.ErrCommitFailed) || !errors.Is(err, registry.ErrDuplicatePlatform) {
		t.Fatalf("重复平台应 ErrCommitFailed+ErrDuplicatePlatform，got %v", err)
	}
	after, _ := reg.Resolve(ctx, "img:t")
	if after.Digest != before.Digest {
		t.Fatal("重复平台失败后引用不得变化")
	}
	t.Logf("判定: 重复平台被拒，引用保持 %s", before.Digest.Short())

	// 把引用文件改写成无 formatVersion 的旧格式：解析照常，闭包校验照常。
	rewriteLegacy(t, store, "img:t", after)
	blobs, err := reg.ReferencedBlobs(ctx, "img:t")
	if err != nil {
		t.Fatalf("旧格式引用必须在链路中照常可用: %v", err)
	}
	if len(blobs) != 1+2*3 {
		t.Fatalf("旧格式引用闭包应为 %d，实际 %d", 1+2*3, len(blobs))
	}
	t.Logf("判定: 无 formatVersion 的旧引用照常解析，闭包对象=%d", len(blobs))
}

// 并发压测：提交、回收、全库校验同时跑。
// 不变量：任何一次成功提交后，引用闭包始终完整；回收永不误删可达对象；
// 长期占用只随当前可达集变化；verify 看到的可达对象结论始终一致。
func TestConcurrentCommitGCVerify(t *testing.T) {
	store, reg, c := newEnv(t)
	ctx := context.Background()
	const rounds = 12
	var wg sync.WaitGroup
	errCh := make(chan error, rounds*4)

	// 一个长期存在、内容固定的基线引用，任何时刻都必须完整。
	base, err := c.Commit(ctx, commit.Request{Ref: "base:1", Image: specFor("base", "linux", "amd64")})
	if err != nil {
		t.Fatal(err)
	}

	// 提交者：反复把“同一内容”的镜像提交到滚动 tag，外加多平台索引追加。
	for i := 0; i < rounds; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := commit.Request{
				Ref:   "roll:latest",
				Image: specFor("roll", "linux", "amd64"), // 内容恒定 -> 摘要恒定
			}
			if _, err := c.Commit(ctx, req); err != nil {
				errCh <- err
			}
		}(i)
	}
	// 回收者。
	for i := 0; i < rounds; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := gc.New(store, reg).Run(ctx); err != nil {
				errCh <- err
			}
		}()
	}
	// 校验者：任何时刻，引用当前可达对象都不能是坏/缺失结论。
	for i := 0; i < rounds; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rep, err := verify.New(store, reg).Run(ctx)
			if err != nil {
				errCh <- err
				return
			}
			bad := map[verify.Status]int{}
			for _, f := range rep.Findings {
				if f.Status != verify.StatusOK {
					bad[f.Status]++
				}
			}
			// 允许存在“截断/篡改”吗？本场景没有任何人为破坏，因此不允许。
			// missing 也不允许：被引用对象不会被回收误删。
			for st, n := range bad {
				if n > 0 {
					errCh <- errVerifySaw(st, n)
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	// 并发结束后：基线与滚动引用都必须完整。
	for _, ref := range []string{"base:1", "roll:latest"} {
		blobs, err := reg.ReferencedBlobs(ctx, ref)
		if err != nil {
			t.Fatalf("%s 闭包必须可解析: %v", ref, err)
		}
		for d := range blobs {
			if _, err := store.Stat(ctx, d); err != nil {
				t.Fatalf("%s 的可达对象 %s 丢失/损坏: %v", ref, d.Short(), err)
			}
		}
	}
	if base.Manifest.Digest == "" {
		t.Fatal("base 摘要为空")
	}

	// 最终回收后占用回落：base 与 roll 内容不同，各自 3 对象，共 6。
	if _, err := gc.New(store, reg).Run(ctx); err != nil {
		t.Fatal(err)
	}
	all, _ := store.List(ctx)
	if len(all) != 6 {
		t.Fatalf("判定依据: 长期反复提交后占用应只与当前可达集有关（6 个对象），实际 %d", len(all))
	}
	t.Logf("判定: 并发提交+回收+校验无悬挂引用、无误删、无坏对象，最终占用=%d", len(all))
}

// 提交失败不累积垃圾：多次失败 + 成功 + GC 后，对象数等于可达集。
func TestFailedCommitsDoNotAccumulate(t *testing.T) {
	store, reg, c := newEnv(t)
	ctx := context.Background()

	// 5 个“组装成功但故意不发布”的镜像：对象在存储中但无引用。
	for i := 0; i < 5; i++ {
		if _, _, err := assemble.New(store).AssembleImage(ctx, specFor("unused", "linux", "amd64")); err != nil {
			t.Fatal(err)
		}
	}
	// 一个真正发布的引用。
	if _, err := c.Commit(ctx, commit.Request{Ref: "keep:1", Image: specFor("keep", "linux", "amd64")}); err != nil {
		t.Fatal(err)
	}
	before, _ := store.List(ctx)
	res, err := gc.New(store, reg).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := store.List(ctx)
	if len(after) != 3 {
		t.Fatalf("判定依据: 未发布对象应被回收，只剩 3 个可达对象，实际 %d（before=%d deleted=%d）",
			len(after), len(before), len(res.Deleted))
	}
	// 再跑一轮 GC：删除数必须为 0，不随时间增长。
	res2, _ := gc.New(store, reg).Run(ctx)
	if len(res2.Deleted) != 0 {
		t.Fatalf("稳定状态第二轮 GC 不应删除，实际 %v", res2.Deleted)
	}
	t.Logf("判定: 失败/未发布对象一轮回收后清空（%d -> %d），复测删除 0", len(before), len(after))
}
