package cas

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
)

// 三类完整性结论必须能被明确区分：
// 不存在 / 长度不符（截断）/ 长度一致但内容被改过（篡改）。
func TestInspectThreeClassClassification(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// 第 1 类：对象根本不存在。
	missing := digest.FromBytes([]byte("never-existed"))
	_, err := store.Inspect(ctx, missing, 123)
	t.Logf("输入: 未写入摘要=%s declaredSize=123 -> err=%v", missing.Short(), err)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("缺失必须是 ErrNotFound，got %v", err)
	}
	if errors.Is(err, ErrCorrupt) {
		t.Fatalf("缺失不得归类为损坏，got %v", err)
	}

	data := []byte("the-quick-brown-fox-content")
	d := digest.FromBytes(data)
	if err := store.Put(ctx, d, byteReader(append([]byte(nil), data...))); err != nil {
		t.Fatal(err)
	}
	wantSize := int64(len(data))

	// 完整对象。
	size, err := store.Inspect(ctx, d, wantSize)
	if err != nil || size != wantSize {
		t.Fatalf("完整对象应通过，size=%d err=%v", size, err)
	}
	t.Logf("输入: 完整对象 size=%d -> OK", wantSize)

	// 第 2 类：对象在，但长度对不上（截断）。
	p, _ := store.Path(d)
	if err := os.WriteFile(p, data[:len(data)-5], 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = store.Inspect(ctx, d, wantSize)
	t.Logf("输入: 截断对象 %d/%d 字节 -> err=%v", len(data)-5, wantSize, err)
	if !errors.Is(err, ErrCorrupt) || !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("截断必须是 ErrCorrupt+ErrSizeMismatch，got %v", err)
	}
	if errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("长度不符应先判为 size-mismatch，不应再报 digest-mismatch，got %v", err)
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatalf("损坏对象不得伪装成缺失，got %v", err)
	}

	// 第 3 类：长度一致，但内容被改过（等长篡改）。
	tampered := append([]byte(nil), data...)
	tampered[0] = tampered[0] ^ 0xFF
	if err := os.WriteFile(p, tampered, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = store.Inspect(ctx, d, wantSize)
	t.Logf("输入: 等长篡改 size=%d -> err=%v", wantSize, err)
	if !errors.Is(err, ErrCorrupt) || !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("篡改必须是 ErrCorrupt+ErrDigestMismatch，got %v", err)
	}
	if errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("等长篡改不得报 size-mismatch，got %v", err)
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatalf("损坏对象不得伪装成缺失，got %v", err)
	}

	// 无大小声明（孤立对象校验路径）：截断也只能按摘要判为 digest-mismatch。
	if err := os.WriteFile(p, data[:len(data)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = store.Inspect(ctx, d, -1)
	t.Logf("输入: 截断对象且无大小声明 -> err=%v", err)
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("无大小声明时损坏应按摘要判 ErrDigestMismatch，got %v", err)
	}

	// 声明大小大于实际也要 size-mismatch（声明被改/对象被追加判定口径一致）。
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = store.Inspect(ctx, d, wantSize+10)
	if !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("声明大小大于实际应 ErrSizeMismatch，got %v", err)
	}
	t.Log("判定: 不存在/截断/篡改三类结论互斥且可用 errors.Is 精确区分")
}

// 非法摘要必须给 ErrInvalidDigest，不落入任何完整性结论。
func TestInspectInvalidDigest(t *testing.T) {
	store := newTestStore(t)
	if _, err := store.Inspect(context.Background(), "sha256:bad", 0); !errors.Is(err, ErrInvalidDigest) {
		t.Fatalf("非法摘要应 ErrInvalidDigest，got %v", err)
	}
}
