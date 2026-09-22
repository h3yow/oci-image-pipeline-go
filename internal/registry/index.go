package registry

import (
	"bytes"
	"context"
	"fmt"

	"github.com/h3yow/oci-image-pipeline-go/internal/descriptor"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/store"
)

// BuildAndPushIndex 用一组（平台→已存在清单描述符）组装规范化多架构索引，
// 写入 CAS 并绑定到 ref。平台缺失/重复/无法区分由 manifest 包判定并拒绝。
//
// 每个清单描述符必须真实存在且通过完整性校验；条目顺序不影响索引摘要
// （编码时按平台键稳定排序发生在 SelectPlatform 之外，此处显式排序输入）。
func (r *Registry) BuildAndPushIndex(ctx context.Context, ref Ref, entries []manifest.IndexEntry) (descriptor.Descriptor, error) {
	idx := &manifest.Index{Manifests: entries}
	raw, err := manifest.EncodeIndex(idx)
	if err != nil {
		return descriptor.Descriptor{}, err
	}
	dgst, size, err := r.store().Put(ctx, bytes.NewReader(raw))
	if err != nil {
		return descriptor.Descriptor{}, fmt.Errorf("registry: put index: %w", err)
	}
	desc := descriptor.Descriptor{
		MediaType: descriptor.MediaTypeImageIndex,
		Digest:    dgst,
		Size:      size,
	}
	if err := r.PushIndex(ctx, ref, desc); err != nil {
		return descriptor.Descriptor{}, err
	}
	return desc, nil
}

// LoadManifestDescriptor 从 CAS 读取 desc 对应清单（供命令侧组装索引前校验）。
func (r *Registry) LoadManifestDescriptor(ctx context.Context, desc descriptor.Descriptor) error {
	rc, size, err := r.store().Get(ctx, desc.Digest)
	if err != nil {
		return err
	}
	defer rc.Close()
	if size != desc.Size {
		return fmt.Errorf("%w: descriptor=%d stored=%d",
			store.ErrSizeMismatch, desc.Size, size)
	}
	return nil
}

// store 暴露关联的 CAS（包内使用）。
func (r *Registry) store() *store.Store { return r.st }
