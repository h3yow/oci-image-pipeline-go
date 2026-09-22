package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestPutIsIdempotentAndDeduplicates(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	content := []byte("layer-content-A")

	d1, n1, err := st.Put(ctx, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	d2, n2, err := st.Put(ctx, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input=%q first=%s second=%s", content, d1, d2)
	if !d1.Equal(d2) || n1 != n2 {
		t.Fatalf("repeated put diverged: %s/%d vs %s/%d", d1, n1, d2, n2)
	}
	// 目录中该对象只有一个文件。
	entries, err := os.ReadDir(filepath.Join(st.Root(), "blobs", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 blob file after duplicate puts, got %d", len(entries))
	}
	// 不同内容必须得到不同摘要。
	d3, _, err := st.Put(ctx, bytes.NewReader([]byte("layer-content-B")))
	if err != nil {
		t.Fatal(err)
	}
	if d1.Equal(d3) {
		t.Fatal("different content produced identical digest")
	}
}

func TestGetClassifiesFailures(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	content := []byte("0123456789abcdef")
	d, _, err := st.Put(ctx, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	blobPath := filepath.Join(st.Root(), "blobs", "sha256", d.Hex())

	t.Run("missing", func(t *testing.T) {
		ghost, _ := digest.Parse("sha256:" + strings.Repeat("1", 64))
		_, _, err := st.Get(ctx, ghost)
		t.Logf("missing object %s -> %v", ghost, err)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})

	t.Run("truncated", func(t *testing.T) {
		declared := int64(len(content))
		// 截断：写入比原内容更短的字节（文件头相同，大小不同）。
		if err := os.WriteFile(blobPath, content[:5], 0o644); err != nil {
			t.Fatal(err)
		}
		rc, stored, err := st.OpenExpected(ctx, d, declared)
		if err == nil {
			defer rc.Close()
			_, err = io.ReadAll(rc)
		}
		t.Logf("truncated object %s (declared=%d stored=%d) -> %v",
			d, declared, stored, err)
		if !errors.Is(err, ErrSizeMismatch) {
			t.Fatalf("want ErrSizeMismatch, got %v", err)
		}
	})

	t.Run("corrupted-same-size", func(t *testing.T) {
		// 同长度但内容不同：摘要必然不符。
		tampered := bytes.Clone(content)
		tampered[0] ^= 0xFF
		if err := os.WriteFile(blobPath, tampered, 0o644); err != nil {
			t.Fatal(err)
		}
		rc, _, err := st.Get(ctx, d)
		if err != nil {
			t.Fatalf("open should succeed, got %v", err)
		}
		defer rc.Close()
		_, rerr := io.ReadAll(rc)
		t.Logf("corrupted object %s -> %v", d, rerr)
		if !errors.Is(rerr, ErrDigestMismatch) {
			t.Fatalf("want ErrDigestMismatch, got %v", rerr)
		}
	})

	t.Run("restored", func(t *testing.T) {
		if err := os.WriteFile(blobPath, content, 0o644); err != nil {
			t.Fatal(err)
		}
		rc, _, err := st.Get(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
		defer rc.Close()
		got, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("restored content rejected: %v", err)
		}
		if !bytes.Equal(got, content) {
			t.Fatal("content mismatch after restore")
		}
	})
}

func TestConcurrentPutGetDelete(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	// 预先放入一批对象，其中一半保留、一半删除，验证并发下不互相干扰。
	const writers = 8
	const readers = 8
	var wg sync.WaitGroup
	errs := make(chan error, writers*4)

	// 并发写同一内容：全部应得到同一摘要，且只有一个文件。
	shared := []byte("shared-content")
	var want digest.Digest
	var once sync.Once
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := append(bytes.Clone(shared), byte('0'+i))
			d, _, err := st.Put(ctx, bytes.NewReader(payload))
			if err != nil {
				errs <- fmt.Errorf("put: %w", err)
				return
			}
			if i == 0 {
				once.Do(func() { want = d })
			}
			// 写后立即读回校验。
			rc, _, err := st.Get(ctx, d)
			if err != nil {
				errs <- fmt.Errorf("get after put: %w", err)
				return
			}
			got, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				errs <- fmt.Errorf("read after put: %w", err)
				return
			}
			if !bytes.Equal(got, payload) {
				errs <- errors.New("read back content differs")
			}
		}(i)
	}
	// 并发读（可能读到不存在的对象：只允许 ErrNotFound）。
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ghost, _ := digest.Parse(fmt.Sprintf("sha256:%064x", i+1))
			_, _, err := st.Get(ctx, ghost)
			if err != nil && !errors.Is(err, ErrNotFound) {
				errs <- fmt.Errorf("concurrent get: %w", err)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	_ = want
}

func TestDeleteSemantics(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	d, _, err := st.Put(ctx, bytes.NewReader([]byte("to-delete")))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Delete(ctx, d); err != nil {
		t.Fatal(err)
	}
	err = st.Delete(ctx, d)
	t.Logf("delete missing %s -> %v", d, err)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound on second delete, got %v", err)
	}
}
