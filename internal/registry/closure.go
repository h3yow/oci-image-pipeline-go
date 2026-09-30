package registry

import (
	"context"
	"fmt"

	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
)

// ClosureObject 描述引用闭包中的一个已校验对象。
type ClosureObject struct {
	Digest    digest.Digest
	Size      int64
	MediaType string
	Role      string // index | manifest | config | layer
}

// VerifyClosure 从根对象（引用指向的索引/清单）出发，按描述符声明的
// digest+size 逐个校验整个可达闭包，统一走 cas 的三类完整性口径：
//
//   - 对象根本不存在 -> cas.ErrNotFound；
//   - 长度与声明 size 对不上（截断/追加）-> cas.ErrSizeMismatch(+ErrCorrupt)；
//   - 长度对但摘要不符（内容被改过）-> cas.ErrDigestMismatch(+ErrCorrupt)。
//
// 读取、回收、提交前检查共用这一份判定，绝不各说各话。位置类型也强校验：
// config 位必须是 config、layer 位必须是层、索引条目必须是清单/索引。
//
// 不持有读 pin；需要跨“校验→发布”窗口防回收的调用方（提交路径）应使用
// VerifyClosurePinned。
func (r *Registry) VerifyClosure(ctx context.Context, root digest.Digest, rootSize int64) ([]ClosureObject, error) {
	objs, _, err := r.verifyClosure(ctx, root, rootSize, nil)
	return objs, err
}

// VerifyClosurePinned 与 VerifyClosure 同口径，但每个对象在“校验之前”
// 先登记读 pin 并保持到返回的 release 被调用。提交路径据此把 pin 持有到
// 引用原子 rename 之后，杜绝“校验通过的对象在发布前被并发回收删掉”的窗口。
//
// 校验失败时内部已释放全部 pin，调用方只需（也应当）在成功后 defer release。
func (r *Registry) VerifyClosurePinned(ctx context.Context, root digest.Digest, rootSize int64) ([]ClosureObject, func(), error) {
	pins := map[digest.Digest]struct{}{}
	objs, _, err := r.verifyClosure(ctx, root, rootSize, pins)
	if err != nil {
		releaseAll(r.store, pins)
		return nil, func() {}, err
	}
	return objs, func() { releaseAll(r.store, pins) }, nil
}

func releaseAll(store *cas.Store, pins map[digest.Digest]struct{}) {
	for d := range pins {
		store.Lock().EndRead(d)
	}
}

// verifyClosure 递归校验。pins != nil 时对每个对象先 pin 再校验。
func (r *Registry) verifyClosure(ctx context.Context, d digest.Digest, declaredSize int64, pins map[digest.Digest]struct{}) ([]ClosureObject, map[digest.Digest]struct{}, error) {
	if pins != nil {
		if _, ok := pins[d]; !ok {
			r.store.Lock().BeginRead(d)
			pins[d] = struct{}{}
		}
	}

	// 根/清单/索引本身：若有声明大小（描述符带 size）则按三类口径细分；
	// 根从引用直接出发且没有声明大小时退化为摘要校验。
	if _, err := r.store.Check(ctx, d, declaredSize); err != nil {
		return nil, nil, err
	}

	mt, data, err := manifest.GetManifest(ctx, r.store, d)
	if err != nil {
		return nil, nil, err
	}

	role := manifestRole(mt)
	out := []ClosureObject{{Digest: d, MediaType: mt, Role: role}}
	seen := map[digest.Digest]struct{}{d: {}}

	switch mt {
	case manifest.MediaTypeIndex:
		idx, perr := manifest.ParseIndex(data)
		if perr != nil {
			return nil, nil, perr
		}
		for i, md := range idx.Manifests {
			if md.MediaType != manifest.MediaTypeManifest && md.MediaType != manifest.MediaTypeIndex {
				return nil, nil, fmt.Errorf("%w: index manifests[%d] mediaType %q",
					manifest.ErrMalformed, i, md.MediaType)
			}
			sub, subSeen, verr := r.verifyClosure(ctx, md.Digest, md.Size, pins)
			if verr != nil {
				return nil, nil, fmt.Errorf("index %s manifests[%d] %s: %w",
					d.Short(), i, md.Digest.Short(), verr)
			}
			for _, o := range sub {
				if _, dup := seen[o.Digest]; dup {
					continue
				}
				seen[o.Digest] = struct{}{}
				out = append(out, o)
			}
			for sd := range subSeen {
				seen[sd] = struct{}{}
			}
		}
	case manifest.MediaTypeManifest:
		m, perr := manifest.ParseManifest(data)
		if perr != nil {
			return nil, nil, perr
		}
		if m.Config.MediaType != manifest.MediaTypeConfig {
			return nil, nil, fmt.Errorf("%w: config slot mediaType %q",
				manifest.ErrMalformed, m.Config.MediaType)
		}
		leafs := []struct {
			where string
			desc  manifest.Descriptor
		}{{"config", m.Config}}
		for i, ld := range m.Layers {
			if ld.MediaType != manifest.MediaTypeLayer {
				return nil, nil, fmt.Errorf("%w: layers[%d] slot mediaType %q",
					manifest.ErrMalformed, i, ld.MediaType)
			}
			leafs = append(leafs, struct {
				where string
				desc  manifest.Descriptor
			}{fmt.Sprintf("layers[%d]", i), ld})
		}
		for _, leaf := range leafs {
			if _, ok := seen[leaf.desc.Digest]; ok {
				continue
			}
			if pins != nil {
				r.store.Lock().BeginRead(leaf.desc.Digest)
				pins[leaf.desc.Digest] = struct{}{}
			}
			n, verr := r.store.Check(ctx, leaf.desc.Digest, leaf.desc.Size)
			if verr != nil {
				return nil, nil, fmt.Errorf("manifest %s %s %s: %w",
					d.Short(), leaf.where, leaf.desc.Digest.Short(), verr)
			}
			seen[leaf.desc.Digest] = struct{}{}
			out = append(out, ClosureObject{
				Digest: leaf.desc.Digest, Size: n,
				MediaType: leaf.desc.MediaType,
				Role:      leafRole(leaf.where),
			})
		}
	}
	return out, seen, nil
}

func manifestRole(mediaType string) string {
	if mediaType == manifest.MediaTypeIndex {
		return "index"
	}
	return "manifest"
}

func leafRole(where string) string {
	if where == "config" {
		return "config"
	}
	return "layer"
}
