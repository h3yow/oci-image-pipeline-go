package registry

import (
	"context"
	"sync"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
)

// 并发追加不同平台到同一索引：全部成功，最终索引完整无重复。
func TestConcurrentAttachDistinctPlatforms(t *testing.T) {
	store, reg := newEnv(t)
	ctx := context.Background()
	const n = 10
	// 清单在主 goroutine 预生成，避免在子 goroutine 中调用 t.Fatal。
	manifests := make([]manifest.Descriptor, n)
	for i := 0; i < n; i++ {
		manifests[i] = writeManifest(t, store, platformMarker(i))
	}
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := manifest.Platform{OS: "linux", Architecture: archName(i)}
			if err := reg.AttachPlatform(ctx, "img:t", manifests[i], p); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("并发追加平台错误: %v", err)
	}
	for i := 0; i < n; i++ {
		got, err := reg.SelectPlatform(ctx, "img:t", manifest.Platform{OS: "linux", Architecture: archName(i)})
		if err != nil {
			t.Fatalf("平台 %d 应可唯一选择: %v", i, err)
		}
		_ = got
	}
	blobs, err := reg.ReferencedBlobs(ctx, "img:t")
	if err != nil {
		t.Fatal(err)
	}
	// 索引 + n 清单 + n config + n 层
	if len(blobs) != 1+3*n {
		t.Fatalf("判定依据: 索引闭包应为 %d 个对象，实际 %d", 1+3*n, len(blobs))
	}
	t.Logf("判定: %d 个并发不同平台追加成功，闭包对象数=%d", n, len(blobs))
}

func platformMarker(i int) string { return archName(i) + "-image" }

func archName(i int) string {
	names := []string{"amd64", "arm64", "386", "ppc64le", "s390x", "mips64", "riscv64", "arm", "loong64", "wasm"}
	return names[i%len(names)]
}
