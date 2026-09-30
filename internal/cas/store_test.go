package cas

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
)

func TestPutDedupAndGetRoundTrip(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	data := []byte("layer-content-A")
	d := digest.FromBytes(data)
	t.Logf("输入内容=%q 计算摘要=%s", data, d)

	if err := store.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatalf("首次写入失败: %v", err)
	}
	p, _ := store.Path(d)
	info1, _ := os.Stat(p)

	// 再次写入相同摘要：必须去重，文件不被重写（mtime/ino 不变即“未重复写入”）。
	if err := store.Put(ctx, d, bytes.NewReader(append([]byte(nil), data...))); err != nil {
		t.Fatalf("重复写入应去重成功: %v", err)
	}
	info2, _ := os.Stat(p)
	if !info1.ModTime().Equal(info2.ModTime()) {
		t.Fatalf("判定依据: 已存在层被重复写入，mtime %v -> %v", info1.ModTime(), info2.ModTime())
	}
	t.Logf("判定: 相同层第二次写入被去重（mtime 保持 %v）", info2.ModTime())

	got, err := store.GetBytes(ctx, d)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("读回不一致: data=%q err=%v", got, err)
	}
}

func TestPutRejectsDigestMismatch(t *testing.T) {
	store := newTestStore(t)
	declared := digest.FromBytes([]byte("expected-content"))
	actual := []byte("tampered-content")
	t.Logf("声明摘要=%s 对应内容=%q；实际写入内容=%q", declared, "expected-content", actual)

	err := store.Put(context.Background(), declared, bytes.NewReader(actual))
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("摘要不匹配应 ErrDigestMismatch，got %v (判定依据: errors.Is)", err)
	}
	// 拒绝后对象不得落地，且不得伪装成普通未命中以外的东西。
	if _, err := store.GetBytes(context.Background(), declared); !errors.Is(err, ErrNotFound) {
		t.Fatalf("被拒绝内容不应落地，期望 ErrNotFound，got %v", err)
	}
	t.Log("判定: 摘要不符被拒绝，且无半写入对象落地")
}

func TestMissingVsCorruptAreDistinct(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	missing := digest.FromBytes([]byte("never-written"))
	if _, err := store.GetBytes(ctx, missing); !errors.Is(err, ErrNotFound) {
		t.Fatalf("缺失对象应 ErrNotFound，got %v", err)
	}
	t.Logf("输入: 未写入摘要 %s -> 判定 ErrNotFound", missing.Short())

	data := []byte("object-to-corrupt")
	d := digest.FromBytes(data)
	if err := store.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	p, _ := store.Path(d)

	// 场景1：截断（尾部丢字节）：按摘要直读只能判定 ErrCorrupt（没有声明
	// 大小可对比，绝不伪装成缺失）；细分截断/篡改须走 Check(d, declaredSize)。
	if err := os.WriteFile(p, data[:len(data)-3], 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := store.GetBytes(ctx, d)
	t.Logf("输入: 截断后的对象（%d/%d 字节）直读 -> err=%v", len(data)-3, len(data), err)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("截断必须归类 ErrCorrupt，got %v", err)
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatalf("截断不得伪装成 ErrNotFound，got %v", err)
	}
	if _, cerr := store.Check(ctx, d, int64(len(data))); !errors.Is(cerr, ErrSizeMismatch) ||
		errors.Is(cerr, ErrDigestMismatch) {
		t.Fatalf("带声明大小校验必须把截断归类 ErrSizeMismatch（非摘要不符），got %v", cerr)
	}
	if ok, err := store.Exists(ctx, d); ok || err == nil {
		t.Fatalf("损坏对象 Exists 不得返回 true,nil（禁止静默视为完整），got ok=%v err=%v", ok, err)
	}

	// 场景2：内容被篡改但长度相同（字节替换）：直读报 ErrCorrupt；
	// 带声明大小的 Check 必须细分到 ErrDigestMismatch。
	tampered := append([]byte(nil), data...)
	tampered[0] = tampered[0] + 1
	if err := os.WriteFile(p, tampered, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = store.GetBytes(ctx, d)
	t.Logf("输入: 等长篡改内容直读 -> err=%v", err)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("篡改必须归类 ErrCorrupt（而非 ErrNotFound），got %v", err)
	}
	if _, terr := store.Check(ctx, d, int64(len(data))); !errors.Is(terr, ErrDigestMismatch) ||
		errors.Is(terr, ErrSizeMismatch) {
		t.Fatalf("带声明大小校验必须把等长篡改归类 ErrDigestMismatch（非大小不符），got %v", terr)
	}

	// 损坏对象可用正确内容原子修复。
	if err := store.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatalf("以正确内容修复损坏对象失败: %v", err)
	}
	if _, err := store.GetBytes(ctx, d); err != nil {
		t.Fatalf("修复后读取失败: %v", err)
	}
	t.Log("判定: 损坏对象被正确内容原子替换后恢复完整")
}

func TestConcurrentPutSameDigest(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	data := []byte("concurrent-layer-content")
	d := digest.FromBytes(data)
	t.Logf("并发输入: 20 个 goroutine 写入同一摘要 %s", d.Short())

	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.Put(ctx, d, bytes.NewReader(append([]byte(nil), data...)))
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("并发写入出现错误: %v", err)
		}
	}
	got, err := store.GetBytes(ctx, d)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("并发后读取必须得到完整内容: err=%v", err)
	}
	all, _ := store.List(ctx)
	if len(all) != 1 {
		t.Fatalf("判定依据: 并发同摘要写入后应仅 1 个对象，实际 %d", len(all))
	}
	t.Log("判定: 20 并发同摘要写入无半写入、无重复对象，唯一对象完整")
}

func TestConcurrentPutDistinctDigests(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	const n = 30
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b := []byte(fmt.Sprintf("distinct-content-%03d", i))
			d := digest.FromBytes(b)
			if err := store.Put(ctx, d, bytes.NewReader(b)); err != nil {
				t.Errorf("写入 %d 失败: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	all, _ := store.List(ctx)
	if len(all) != n {
		t.Fatalf("不同内容并发写入后应有 %d 个对象，实际 %d", n, len(all))
	}
	t.Logf("判定: %d 个不同摘要并发写入，全部完整可见", n)
}

// TestCheckThreeWayClassification 直接压统一口径 Check：缺失 / 截断 /
// 等长篡改三类必须各自能 errors.Is 到对应哨兵，且互不串味。
func TestCheckThreeWayClassification(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	data := []byte("declared-size-content-0123456789")
	d := digest.FromBytes(data)

	// 1) 缺失。
	if _, err := store.Check(ctx, d, int64(len(data))); !errors.Is(err, ErrNotFound) {
		t.Fatalf("缺失必须 ErrNotFound，got %v", err)
	}

	if err := store.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	// 2) 完整：返回实际大小。
	n, err := store.Check(ctx, d, int64(len(data)))
	if err != nil || n != int64(len(data)) {
		t.Fatalf("完整对象应通过，n=%d err=%v", n, err)
	}

	// 3) 截断：声明大小仍是原长，实际更短。
	p, _ := store.Path(d)
	if err := os.WriteFile(p, data[:len(data)-5], 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = store.Check(ctx, d, int64(len(data)))
	if !errors.Is(err, ErrSizeMismatch) || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("截断必须 ErrSizeMismatch+ErrCorrupt，got %v", err)
	}
	if errors.Is(err, ErrDigestMismatch) || errors.Is(err, ErrNotFound) {
		t.Fatalf("截断不得报成摘要不符或缺失，got %v", err)
	}

	// 4) 等长篡改：大小一致、摘要不符。
	tampered := append([]byte(nil), data...)
	tampered[len(tampered)-1] = tampered[len(tampered)-1] + 1
	if err := os.WriteFile(p, tampered, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = store.Check(ctx, d, int64(len(data)))
	if !errors.Is(err, ErrDigestMismatch) || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("篡改必须 ErrDigestMismatch+ErrCorrupt，got %v", err)
	}
	if errors.Is(err, ErrSizeMismatch) || errors.Is(err, ErrNotFound) {
		t.Fatalf("篡改不得报成大小不符或缺失，got %v", err)
	}
	t.Logf("判定: 三类完整性结论（ErrNotFound/ErrSizeMismatch/ErrDigestMismatch）可明确区分")
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := NewStore(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	return s
}
