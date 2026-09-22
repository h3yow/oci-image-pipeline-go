package registry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
)

func newEnv(t *testing.T) (*cas.Store, *Registry) {
	t.Helper()
	store, err := cas.NewStore(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	reg, err := New(store)
	if err != nil {
		t.Fatal(err)
	}
	return store, reg
}

// writeManifest 生成一个带指定标记的清单并写入 CAS，返回其描述符。
func writeManifest(t *testing.T, store *cas.Store, marker string) manifest.Descriptor {
	t.Helper()
	config := []byte(fmt.Sprintf(`{"marker":%q}`, marker))
	cd, err := store.PutBytes(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	layer := []byte("layer-" + marker)
	ld, err := store.PutBytes(context.Background(), layer)
	if err != nil {
		t.Fatal(err)
	}
	m := &manifest.Manifest{
		Config: manifest.Descriptor{MediaType: manifest.MediaTypeConfig, Digest: cd, Size: int64(len(config))},
		Layers: []manifest.Descriptor{{MediaType: manifest.MediaTypeLayer, Digest: ld, Size: int64(len(layer))}},
	}
	data, err := manifest.EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	desc, err := manifest.PutManifest(context.Background(), store, data)
	if err != nil {
		t.Fatal(err)
	}
	return desc
}

func TestMultiArchSelectUnique(t *testing.T) {
	store, reg := newEnv(t)
	ctx := context.Background()
	amd := writeManifest(t, store, "amd64-image")
	arm := writeManifest(t, store, "arm64-image")

	if err := reg.AttachPlatform(ctx, "app:latest", amd, manifest.Platform{OS: "linux", Architecture: "amd64"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.AttachPlatform(ctx, "app:latest", arm, manifest.Platform{OS: "linux", Architecture: "arm64"}); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 索引含 linux/amd64=%s linux/arm64=%s", amd.Digest.Short(), arm.Digest.Short())

	got, err := reg.SelectPlatform(ctx, "app:latest", manifest.Platform{OS: "linux", Architecture: "arm64"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Digest != arm.Digest {
		t.Fatalf("判定依据: 平台选择错位，期望 %s got %s", arm.Digest, got.Digest)
	}
	t.Logf("判定: linux/arm64 唯一选中 %s", got.Digest.Short())

	// 缺失平台：必须明确 ErrNoMatch，不得任选。
	_, err = reg.SelectPlatform(ctx, "app:latest", manifest.Platform{OS: "windows", Architecture: "amd64"})
	t.Logf("输入: 请求 windows/amd64 -> err=%v", err)
	if !errors.Is(err, ErrNoMatch) {
		t.Fatalf("缺失平台必须 ErrNoMatch，got %v", err)
	}

	// 重复平台：必须明确拒绝。
	err = reg.AttachPlatform(ctx, "app:latest", writeManifest(t, store, "amd64-again"),
		manifest.Platform{OS: "linux", Architecture: "amd64"})
	t.Logf("输入: 重复追加 linux/amd64 -> err=%v", err)
	if !errors.Is(err, ErrDuplicatePlatform) {
		t.Fatalf("重复平台必须 ErrDuplicatePlatform，got %v", err)
	}
}

func TestSelectOnNonIndexRejected(t *testing.T) {
	store, reg := newEnv(t)
	ctx := context.Background()
	m := writeManifest(t, store, "single")
	if err := reg.Tag(ctx, "solo:v1", m); err != nil {
		t.Fatal(err)
	}
	_, err := reg.SelectPlatform(ctx, "solo:v1", manifest.Platform{OS: "linux", Architecture: "amd64"})
	t.Logf("输入: 对清单引用做平台选择 -> err=%v", err)
	if !errors.Is(err, ErrWrongKind) {
		t.Fatalf("对清单引用做平台选择必须 ErrWrongKind，got %v", err)
	}
}

func TestRefVersionCompatibility(t *testing.T) {
	store, reg := newEnv(t)
	ctx := context.Background()
	m := writeManifest(t, store, "v1")

	// 旧格式引用：无 formatVersion 字段 -> 按 version 1 解释。
	p, err := reg.refPath("old:latest")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	legacy := fmt.Sprintf(`{"target":{"mediaType":%q,"digest":%q,"size":%d}}`,
		m.MediaType, m.Digest, m.Size)
	if err := os.WriteFile(p, []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}
	desc, err := reg.Resolve(ctx, "old:latest")
	t.Logf("输入: 无 formatVersion 的旧引用 -> desc=%s err=%v", desc.Digest.Short(), err)
	if err != nil || desc.Digest != m.Digest {
		t.Fatalf("旧格式引用必须兼容，got desc=%v err=%v", desc, err)
	}

	// 未来版本引用：明确拒绝，不猜测。
	future := fmt.Sprintf(`{"formatVersion":99,"target":{"mediaType":%q,"digest":%q,"size":%d}}`,
		m.MediaType, m.Digest, m.Size)
	if err := os.WriteFile(p, []byte(future), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = reg.Resolve(ctx, "old:latest")
	t.Logf("输入: formatVersion=99 的引用 -> err=%v", err)
	if !errors.Is(err, ErrUnsupportedReferenceVersion) {
		t.Fatalf("超版本引用必须 ErrUnsupportedReferenceVersion，got %v", err)
	}

	// 损坏引用：不得伪装成不存在。
	if err := os.WriteFile(p, []byte("{broken"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = reg.Resolve(ctx, "old:latest")
	t.Logf("输入: 损坏的引用文件 -> err=%v", err)
	if !errors.Is(err, ErrCorruptRef) {
		t.Fatalf("损坏引用必须 ErrCorruptRef，got %v", err)
	}
	if errors.Is(err, ErrNoSuchRef) {
		t.Fatal("损坏引用不得伪装成不存在")
	}
}

func TestReferencedBlobsClosure(t *testing.T) {
	store, reg := newEnv(t)
	ctx := context.Background()
	amd := writeManifest(t, store, "amd64-image")
	arm := writeManifest(t, store, "arm64-image")
	if err := reg.AttachPlatform(ctx, "app:latest", amd, manifest.Platform{OS: "linux", Architecture: "amd64"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.AttachPlatform(ctx, "app:latest", arm, manifest.Platform{OS: "linux", Architecture: "arm64"}); err != nil {
		t.Fatal(err)
	}
	blobs, err := reg.ReferencedBlobs(ctx, "app:latest")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 双平台索引 -> 可达对象数=%d", len(blobs))
	// 索引 + 2 清单 + 2 config + 2 层 = 7
	if len(blobs) != 7 {
		t.Fatalf("判定依据: 可达闭包应为 7 个对象，实际 %d", len(blobs))
	}
	for _, d := range []digest.Digest{amd.Digest, arm.Digest} {
		if _, ok := blobs[d]; !ok {
			t.Fatalf("清单 %s 必须可达", d.Short())
		}
	}
}
