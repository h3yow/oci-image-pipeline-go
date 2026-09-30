// Package verify 对整个本地仓库做逐对象完整性巡检：枚举全部对象与全部
// 引用闭包，对每个对象按统一三类口径给出结论（缺失 / 大小不符即截断 /
// 大小一致但内容被改过），不因首个问题中止，便于转出去排查。
//
// 口径与读取、回收、提交前检查完全一致（同一份 cas.Check 判定），坏对象
// 绝不被当成没找到而跳过。
package verify

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
)

// Kind 是完整性三类结论（外加正常结论 ok）。
type Kind string

const (
	// KindOK 对象存在、大小与摘要均正确。
	KindOK Kind = "ok"
	// KindMissing 对象被引用/期望存在，但根本不存在。
	KindMissing Kind = "missing"
	// KindTruncated 对象存在，但实际长度与声明大小不符（截断/追加）。
	KindTruncated Kind = "truncated"
	// KindTampered 对象存在、长度一致，但内容摘要与声明不符（被改过）。
	KindTampered Kind = "tampered"
)

// Finding 单条巡检结论。
//
// 存储枚举路径下 Ref 为空；引用闭包路径下 Ref 为对应 name:tag，Role 为
// 该对象在闭包中的位置（root/manifest/config/layers[i]/manifests[i]）。
// 同一对象可能出现在“裸对象巡检”与若干引用下，每条都独立给结论，方便
// 定位“是哪个引用指向了坏对象”。
type Finding struct {
	Digest string `json:"digest"`
	Kind   Kind   `json:"kind"`
	Role   string `json:"role,omitempty"`
	Ref    string `json:"ref,omitempty"`
	Detail string `json:"detail,omitempty"`
}

// Report 一轮全库巡检的逐对象结论。
type Report struct {
	Objects []Finding `json:"objects"`
}

// Failed 报告是否存在任何缺失/截断/篡改结论。
func (r *Report) Failed() bool {
	for _, f := range r.Objects {
		if f.Kind != KindOK {
			return true
		}
	}
	return false
}

// Count 按结论分类计数。
func (r *Report) Count(kind Kind) int {
	n := 0
	for _, f := range r.Objects {
		if f.Kind == kind {
			n++
		}
	}
	return n
}

// Store 是巡检所需的存储能力（*cas.Store 天然满足）。
type Store interface {
	List(ctx context.Context) ([]digest.Digest, error)
	Check(ctx context.Context, d digest.Digest, declaredSize int64) (int64, error)
	GetBytes(ctx context.Context, d digest.Digest) ([]byte, error)
}

// Registry 是巡检所需的引用能力。
type Registry interface {
	AllRefs(ctx context.Context) ([]string, error)
	Resolve(ctx context.Context, ref string) (manifest.Descriptor, error)
}

// Verifier 整库完整性巡检器。
type Verifier struct {
	store    Store
	registry Registry
}

// New 创建巡检器。
func New(store Store, reg Registry) *Verifier {
	return &Verifier{store: store, registry: reg}
}

// classify 把 cas 错误映射到三类结论；未识别错误返回 ok=false，调用方
// 不得吞掉这类“无法归类”的异常。
func classify(err error) (Kind, bool) {
	switch {
	case err == nil:
		return KindOK, true
	case errors.Is(err, cas.ErrNotFound):
		return KindMissing, true
	case errors.Is(err, cas.ErrSizeMismatch):
		return KindTruncated, true
	case errors.Is(err, cas.ErrDigestMismatch):
		return KindTampered, true
	case errors.Is(err, cas.ErrCorrupt):
		// 兜底：确认损坏但细分原因丢失时，保守归为篡改（摘要口径失败）。
		return KindTampered, true
	default:
		return "", false
	}
}

type pending struct {
	desc manifest.Descriptor
	role string
}

// Run 枚举存储全部对象并逐个校验（裸对象没有声明大小，做摘要校验），
// 再对每个引用的整个闭包按描述符声明大小逐对象细分三类。
//
// 任一对象缺失/截断/篡改都只记录结论并继续巡检其余对象，绝不因首个问题
// 中止，也绝不把坏对象当成“没找到”跳过；返回的 error 仅在“巡检本身
// 无法进行”（如无法枚举目录）时非 nil。
func (v *Verifier) Run(ctx context.Context) (*Report, error) {
	all, err := v.store.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("verify: list objects: %w", err)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	rep := &Report{}

	// 1) 裸对象巡检：按摘要直读（无声明大小）。枚举得到的对象必然存在，
	//    这里主要暴露“文件在但内容坏”的对象。
	for _, d := range all {
		_, verr := v.store.Check(ctx, d, -1)
		kind, ok := classify(verr)
		if !ok {
			return nil, fmt.Errorf("verify: check %s: %w", d.Short(), verr)
		}
		f := Finding{Digest: string(d), Kind: kind}
		if verr != nil {
			f.Detail = verr.Error()
		}
		rep.Objects = append(rep.Objects, f)
	}

	// 2) 引用闭包巡检：逐对象、带声明大小校验，截断/篡改/缺失都能分开。
	refs, err := v.registry.AllRefs(ctx)
	if err != nil {
		return nil, fmt.Errorf("verify: list refs: %w", err)
	}
	sort.Strings(refs)
	for _, ref := range refs {
		root, rerr := v.registry.Resolve(ctx, ref)
		if rerr != nil {
			// 引用文件本身损坏/超版本：记录该引用根的问题，继续下一个引用，
			// 不伪装成“引用不存在”。
			rep.Objects = append(rep.Objects, Finding{
				Ref: ref, Kind: refKind(rerr), Role: "reference", Detail: rerr.Error(),
			})
			continue
		}
		v.walkClosure(ctx, ref, root, rep)
	}
	return rep, nil
}

// walkClosure 对单个引用闭包逐对象出结论。队列驱动：某个对象缺失或损坏
// 时无法继续解析其内部引用（字节不可信），记录该对象后跳过其下游分支，
// 但不影响闭包中其他独立分支与其他引用的巡检。
func (v *Verifier) walkClosure(ctx context.Context, ref string, root manifest.Descriptor, rep *Report) {
	queue := []pending{{desc: root, role: "root"}}
	visited := map[string]bool{}

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		d := item.desc.Digest
		key := string(d)
		if visited[key] {
			continue
		}
		visited[key] = true

		// 先按描述符声明大小校验（缺失/截断/篡改三类细分）。
		_, verr := v.store.Check(ctx, d, item.desc.Size)
		kind, ok := classify(verr)
		if !ok {
			kind = KindTampered // 无法归类的读取异常：内容不可信
		}
		f := Finding{Digest: key, Kind: kind, Role: item.role, Ref: ref}
		if verr != nil {
			f.Detail = verr.Error()
			rep.Objects = append(rep.Objects, f)
			continue // 对象不可用：不解析其下游，避免基于坏字节做任何推断
		}
		rep.Objects = append(rep.Objects, f)

		if item.desc.MediaType != manifest.MediaTypeIndex && item.desc.MediaType != manifest.MediaTypeManifest {
			continue // config/层等叶子对象，闭包到此为止
		}
		data, gerr := v.store.GetBytes(ctx, d)
		if gerr != nil {
			rep.Objects = append(rep.Objects, Finding{
				Digest: key, Kind: KindTampered, Role: item.role,
				Ref: ref, Detail: gerr.Error(),
			})
			continue
		}
		switch item.desc.MediaType {
		case manifest.MediaTypeIndex:
			idx, perr := manifest.ParseIndex(data)
			if perr != nil {
				rep.Objects = append(rep.Objects, Finding{
					Digest: key, Kind: KindTampered, Role: item.role,
					Ref: ref, Detail: perr.Error(),
				})
				continue
			}
			for i, md := range idx.Manifests {
				if md.MediaType != manifest.MediaTypeManifest && md.MediaType != manifest.MediaTypeIndex {
					rep.Objects = append(rep.Objects, Finding{
						Digest: string(md.Digest), Kind: KindTampered,
						Role: fmt.Sprintf("manifests[%d]", i), Ref: ref,
						Detail: fmt.Sprintf("index slot has mediaType %q", md.MediaType),
					})
					continue
				}
				queue = append(queue, pending{desc: md, role: "manifest"})
			}
		case manifest.MediaTypeManifest:
			m, perr := manifest.ParseManifest(data)
			if perr != nil {
				rep.Objects = append(rep.Objects, Finding{
					Digest: key, Kind: KindTampered, Role: item.role,
					Ref: ref, Detail: perr.Error(),
				})
				continue
			}
			queue = append(queue, pending{desc: m.Config, role: "config"})
			for i, ld := range m.Layers {
				queue = append(queue, pending{desc: ld, role: fmt.Sprintf("layers[%d]", i)})
			}
		}
	}
}

// refKind 把引用文件自身的读取错误映射到三类口径。
func refKind(err error) Kind {
	if errors.Is(err, cas.ErrNotFound) {
		return KindMissing
	}
	// 损坏引用/超版本引用：文件在但不可用，按内容不可信记录。
	return KindTampered
}
