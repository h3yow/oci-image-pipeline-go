package registry

import (
	"context"
	"errors"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
)

func TestVariantAmbiguityIsRejected(t *testing.T) {
	store, reg := newEnv(t)
	ctx := context.Background()

	v7 := writeManifest(t, store, "arm-v7")
	v8 := writeManifest(t, store, "arm-v8")
	if err := reg.AttachPlatform(ctx, "multi:latest", v7, manifest.Platform{OS: "linux", Architecture: "arm", Variant: "v7"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.AttachPlatform(ctx, "multi:latest", v8, manifest.Platform{OS: "linux", Architecture: "arm", Variant: "v8"}); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 索引含 linux/arm/v7=%s 与 linux/arm/v8=%s", v7.Digest.Short(), v8.Digest.Short())

	// 未指定 variant 且存在两个变体 -> ErrAmbiguous，禁止任选。
	_, err := reg.SelectPlatform(ctx, "multi:latest", manifest.Platform{OS: "linux", Architecture: "arm"})
	t.Logf("输入: 请求 linux/arm（不带 variant）-> err=%v", err)
	if !errors.Is(err, ErrAmbiguous) {
		t.Fatalf("多变体未指定 variant 必须 ErrAmbiguous，got %v", err)
	}

	// 指定精确 variant -> 唯一命中。
	got, err := reg.SelectPlatform(ctx, "multi:latest", manifest.Platform{OS: "linux", Architecture: "arm", Variant: "v7"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Digest != v7.Digest {
		t.Fatalf("精确 variant 请求选错: 期望 %s got %s", v7.Digest, got.Digest)
	}
	t.Logf("判定: 精确请求 linux/arm/v7 唯一选中 %s", got.Digest.Short())

	// 指定不存在的 variant -> ErrNoMatch（不得回退任选）。
	_, err = reg.SelectPlatform(ctx, "multi:latest", manifest.Platform{OS: "linux", Architecture: "arm", Variant: "v6"})
	if !errors.Is(err, ErrNoMatch) {
		t.Fatalf("不存在的 variant 必须 ErrNoMatch，got %v", err)
	}
	t.Log("判定: linux/arm/v6 -> ErrNoMatch，不回退")
}

func TestAttachDuplicateVariantRejected(t *testing.T) {
	store, reg := newEnv(t)
	ctx := context.Background()
	m1 := writeManifest(t, store, "x1")
	m2 := writeManifest(t, store, "x2")
	p := manifest.Platform{OS: "linux", Architecture: "arm", Variant: "v7"}
	if err := reg.AttachPlatform(ctx, "img:t", m1, p); err != nil {
		t.Fatal(err)
	}
	err := reg.AttachPlatform(ctx, "img:t", m2, p)
	t.Logf("输入: 重复追加完全相同平台 linux/arm/v7 -> err=%v", err)
	if !errors.Is(err, ErrDuplicatePlatform) {
		t.Fatalf("重复平台必须 ErrDuplicatePlatform，got %v", err)
	}
}
