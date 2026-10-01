package verify_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/commit"
	"github.com/h3yow/oci-image-pipeline-go/internal/gc"
	"github.com/h3yow/oci-image-pipeline-go/internal/verify"
)

// 提交、回收、巡检并发：任何一次巡检都不能对坏引用给出绿色结论，也不能
// 对健康闭包报假阳性。
//
// 场景：
//   - base:1 健康基线引用，全程必须 OK；
//   - roll:latest 被并发反复提交（内容恒定 -> 摘要恒定），提交成功的任一
//     时刻其闭包都必须完整（绝无 missing/size/digest 结论）；
//   - broken:1 是一个被改成读不出来的引用，全程必须报 corrupt-ref，
//     绝不能因为并发提交/回收而漏判成 OK（用户报告的“假绿”场景）。
func TestVerifyConcurrentCommitGCAndPersistentCorruptRef(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	c := commit.New(e.store, e.reg)
	const rounds = 25
	var wg sync.WaitGroup
	errCh := make(chan error, rounds*4)
	failf := func(format string, a ...any) {
		select {
		case errCh <- fmt.Errorf(format, a...):
		default:
		}
	}

	// 健康基线。
	if _, err := c.Commit(ctx, commit.Request{Ref: "base:1", Image: imageSpec("base")}); err != nil {
		t.Fatal(err)
	}
	// 滚动引用先建一个健康版本。
	if _, err := c.Commit(ctx, commit.Request{Ref: "roll:latest", Image: imageSpec("roll")}); err != nil {
		t.Fatal(err)
	}
	// 持久坏引用：整个测试期间都保持读不出来。
	writeRefDoc(t, e, "broken:1", "{not json", true)

	vf := verify.New(e.store, e.reg)

	// 提交者：反复把同一内容提交到 roll:latest。
	for i := 0; i < rounds; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Commit(ctx, commit.Request{Ref: "roll:latest", Image: imageSpec("roll")}); err != nil {
				errCh <- err
			}
		}()
	}
	// 回收者：坏引用让 GC 整轮中止是预期行为；只关心它不 panic/不误删。
	for i := 0; i < rounds; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = gc.New(e.store, e.reg).Run(ctx)
		}()
	}
	// 巡检者：任何一轮都必须稳定抓到 broken:1，其余引用不得假阳性。
	for i := 0; i < rounds; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rep, err := vf.Run(ctx)
			if err != nil {
				errCh <- err
				return
			}
			if rep.Healthy() {
				failf("巡检对持久坏引用给出了全绿结论（假绿）")
				return
			}
			if _, n := refStatus(rep, "broken:1", verify.RefStatusCorruptRef); n == 0 {
				failf("坏引用 broken:1 在并发巡检中漏报（没有 corrupt-ref 结论）")
			}
			for _, f := range rep.RefFindings {
				if f.Ref == "base:1" && f.Status != verify.RefStatusOK {
					failf("健康基线 base:1 出现假阳性 %s: %s", f.Status, f.Error)
				}
				if f.Ref == "roll:latest" {
					switch f.Status {
					case verify.RefStatusMissing, verify.RefStatusSizeMismatch,
						verify.RefStatusDigestMismatch, verify.RefStatusCorruptRef,
						verify.RefStatusMalformed:
						failf("滚动引用 roll:latest 出现不该有的闭包结论 %s @ %s: %s",
							f.Status, f.Where, f.Error)
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	// 最终状态：base/roll 闭包完整可读。
	for _, ref := range []string{"base:1", "roll:latest"} {
		blobs, err := e.reg.ReferencedBlobs(ctx, ref)
		if err != nil {
			t.Fatalf("%s 闭包必须可解析: %v", ref, err)
		}
		for d := range blobs {
			if _, err := e.store.Stat(ctx, d); err != nil {
				t.Fatalf("%s 对象 %s 丢失/损坏: %v", ref, d.Short(), err)
			}
		}
	}
	t.Logf("判定: 并发提交+回收+巡检下坏引用必被抓到、健康闭包无假阳性")
}
