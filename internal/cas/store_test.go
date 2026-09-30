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

	// 场景1：截断（尾部丢字节）。
	if err := os.WriteFile(p, data[:len(data)-3], 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := store.GetBytes(ctx, d)
	t.Logf("输入: 截断后的对象（%d/%d 字节）-> err=%v", len(data)-3, len(data), err)
	if !errors.Is(err, ErrCorrupt) || !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("截断必须归类 ErrCorrupt 且可 errors.Is 到 ErrDigestMismatch，got %v", err)
	}
	if ok, err := store.Exists(ctx, d); ok || err == nil {
		t.Fatalf("损坏对象 Exists 不得返回 true,nil（禁止静默视为完整），got ok=%v err=%v", ok, err)
	}

	// 场景2：内容被篡改但长度相同（字节替换）。
	tampered := append([]byte(nil), data...)
	tampered[0] = tampered[0] + 1
	if err := os.WriteFile(p, tampered, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = store.GetBytes(ctx, d)
	t.Logf("输入: 等长篡改内容 -> err=%v", err)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("篡改必须归类 ErrCorrupt（而非 ErrNotFound），got %v", err)
	}

	// 损坏对象不会被 Put 静默覆盖（防止掩盖被引用对象的完整性问题）。
	if err := store.Put(ctx, d, bytes.NewReader(data)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("损坏同名对象 Put 必须报 ErrCorrupt 而非静默覆盖，got %v", err)
	}
	// 修复路径：先显式删除损坏对象，再以正确内容写入。
	if err := store.Delete(ctx, d); err != nil {
		t.Fatalf("删除损坏对象失败: %v", err)
	}
	if err := store.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatalf("以正确内容重新写入失败: %v", err)
	}
	if _, err := store.GetBytes(ctx, d); err != nil {
		t.Fatalf("修复后读取失败: %v", err)
	}
	t.Log("判定: 损坏对象不被静默覆盖；先删除后重写即恢复完整")
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

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := NewStore(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	return s
}
