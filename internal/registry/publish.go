package registry

import (
	"context"
	"fmt"

	"github.com/h3yow/oci-image-pipeline-go/internal/descriptor"
)

// Publish 在一个与 GC 互斥的发布临界区内执行 materialize：
// materialize 负责把新镜像的层/配置/清单写入 CAS，并返回要绑定的清单
// 描述符；返回后由 Publish 原子完成标签记录落盘。
//
// 语义保证：GC 无法观察到"对象已写、标签未见"的中间态，因此不会把
// 发布进行中的层误判为垃圾。materialize 内部不得再触发 GC。
func (r *Registry) Publish(ctx context.Context, ref Ref, materialize func() (descriptor.Descriptor, error)) error {
	r.st.PublishLock()
	defer r.st.PublishUnlock()

	desc, err := materialize()
	if err != nil {
		return err
	}
	if desc.MediaType == descriptor.MediaTypeLegacyManifest {
		return fmt.Errorf("%w: digest=%s", ErrLegacyRejected, desc.Digest)
	}
	return r.PushManifestLocked(ctx, ref, desc)
}

// PushIndexPublish 是索引版的原子发布：materialize 产出索引描述符后
// 原子绑定标签。
func (r *Registry) PushIndexPublish(ctx context.Context, ref Ref, materialize func() (descriptor.Descriptor, error)) error {
	r.st.PublishLock()
	defer r.st.PublishUnlock()

	desc, err := materialize()
	if err != nil {
		return err
	}
	return r.PushIndexLocked(ctx, ref, desc)
}

// Stage 在与 GC 互斥的临界区内执行 materialize 写入对象，但不绑定标签。
// 适用于"先构造镜像、稍后再通过 Publish/PushManifest 发布"的场景：
// 临界区结束后对象可能是孤儿（后续 GC 会回收），但绝不会出现"已有标签
// 指向、对象却已被本轮 GC 删除"的情况——因为打标签本身也在发布锁内，
// 而 GC 会在自己的临界区里看到该标签并保留其完整闭包。
func (r *Registry) Stage(ctx context.Context, materialize func() (descriptor.Descriptor, error)) (descriptor.Descriptor, error) {
	r.st.PublishLock()
	defer r.st.PublishUnlock()
	return materialize()
}
