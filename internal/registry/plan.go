package registry

import (
	"bytes"
	"context"

	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
)

// LayerInput 一个层的确定性 tar 字节（摘要由字节决定，写入前即可算出）。
type LayerInput struct {
	Tar []byte
}

// ImagePlan 是一次“组装并提交”的完整输入：原始字节齐备，可在写入任何
// 对象之前先算出整个闭包（层/config/清单）的摘要并全部 pin。
type ImagePlan struct {
	ConfigDoc []byte             // config 的规范 JSON 字节
	Layers    []LayerInput       // 按顺序的层 tar 字节
	Platform  *manifest.Platform // 非 nil 表示作为平台条目并入索引；nil 表示单镜像
}

// plannedBlob 闭包中的一个待写对象：摘要在写入前即可确定。
type plannedBlob struct {
	digest digest.Digest
	size   int64
	data   []byte
}

// materializePlan 计算镜像闭包全部对象的摘要（不写盘），返回层、config
// 与清单对象及清单描述符。相同计划恒得相同摘要。
func materializePlan(plan ImagePlan) (layers []plannedBlob, config plannedBlob, manifestBlob plannedBlob, desc manifest.Descriptor, err error) {
	layerDescs := make([]manifest.Descriptor, 0, len(plan.Layers))
	for _, l := range plan.Layers {
		d := digest.FromBytes(l.Tar)
		layers = append(layers, plannedBlob{digest: d, size: int64(len(l.Tar)), data: l.Tar})
		layerDescs = append(layerDescs, manifest.Descriptor{
			MediaType: manifest.MediaTypeLayer, Digest: d, Size: int64(len(l.Tar)),
		})
	}
	cd := digest.FromBytes(plan.ConfigDoc)
	config = plannedBlob{digest: cd, size: int64(len(plan.ConfigDoc)), data: plan.ConfigDoc}

	m := &manifest.Manifest{
		SchemaVersion: manifest.SchemaVersion,
		Config: manifest.Descriptor{
			MediaType: manifest.MediaTypeConfig, Digest: cd, Size: int64(len(plan.ConfigDoc)),
		},
		Layers: layerDescs,
	}
	canonical, err := manifest.EncodeManifest(m)
	if err != nil {
		return nil, plannedBlob{}, plannedBlob{}, manifest.Descriptor{}, err
	}
	md := digest.FromBytes(canonical)
	manifestBlob = plannedBlob{digest: md, size: int64(len(canonical)), data: canonical}
	desc = manifest.Descriptor{MediaType: manifest.MediaTypeManifest, Digest: md, Size: int64(len(canonical))}
	return layers, config, manifestBlob, desc, nil
}

// pinBlobsBeforeWrite 对计划中的全部对象按摘要先登记读 pin，再逐个写入。
// 因为摘要是内容决定的，对象自 rename 落地那一刻起就已在 pin 表中，
// 并发回收在任何时刻都看不到“已落地但未受保护”的对象。
func (r *Registry) pinBlobsBeforeWrite(ctx context.Context, ps *pinSet, blobs ...plannedBlob) error {
	for _, b := range blobs {
		ps.add(r.store.Lock().Pin(b.digest))
	}
	// pin 全部就位后再写：Put 内部对已存在且完整的对象去重，对损坏对象
	// 原子修复；写失败由调用方回滚 pin（defer ps.release）。
	for _, b := range blobs {
		if err := r.store.Put(ctx, b.digest, bytes.NewReader(b.data)); err != nil {
			return err
		}
	}
	return nil
}
