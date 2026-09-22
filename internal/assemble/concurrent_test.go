package assemble

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
)

// 并发重复组装相同镜像：必须全部得到同一摘要、无错误、无重复对象。
func TestConcurrentRepeatedAssembly(t *testing.T) {
	store, err := cas.NewStore(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	a := New(store)
	const n = 16
	var wg sync.WaitGroup
	results := make(chan string, n)
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			desc, _, err := a.AssembleImage(context.Background(), sampleSpec())
			if err != nil {
				errCh <- err
				return
			}
			results <- string(desc.Digest)
		}()
	}
	wg.Wait()
	close(results)
	close(errCh)
	for err := range errCh {
		t.Fatalf("并发组装错误: %v", err)
	}
	var first string
	count := 0
	for d := range results {
		if first == "" {
			first = d
		} else if d != first {
			t.Fatalf("并发组装得到不同摘要: %s vs %s", first, d)
		}
		count++
	}
	all, _ := store.List(context.Background())
	// 1 config + 2 layers + 1 manifest = 4
	if len(all) != 4 {
		t.Fatalf("判定依据: 并发重复组装后应仅 4 个对象，实际 %d", len(all))
	}
	t.Logf("判定: %d 个并发组装全部收敛到 %s，存储中 %d 个对象", n, first[:19], len(all))
}

// 并发组装大量不同镜像：全部成功且对象数确定。
func TestConcurrentDistinctAssembly(t *testing.T) {
	store, err := cas.NewStore(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	a := New(store)
	const n = 20
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			spec := ImageSpec{
				Config: map[string]any{"i": fmt.Sprintf("img-%02d", i)},
				Layers: []LayerSpec{{Name: "l", Files: []File{
					{Path: "f", Data: []byte(fmt.Sprintf("content-%02d", i))},
				}}},
			}
			if _, _, err := a.AssembleImage(context.Background(), spec); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	all, _ := store.List(context.Background())
	if len(all) != 3*n { // 每镜像 config+layer+manifest
		t.Fatalf("判定依据: %d 个不同镜像应产生 %d 个对象，实际 %d", n, 3*n, len(all))
	}
	t.Logf("判定: %d 个并发不同镜像全部成功，对象数=%d", n, len(all))
}
