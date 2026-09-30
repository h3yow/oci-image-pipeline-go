// Package verify 对本地内容寻址存储做全库逐对象的完整性校验。
//
// 与 cas.Inspect / registry.VerifyClosure 共用同一套完整性三分类口径：
//   - StatusOK：对象存在、长度与声明一致、摘要通过；
//   - StatusMissing：被引用对象根本不存在（不存在于 blobs 目录）；
//   - StatusSizeMismatch：对象存在但长度与声明不符（疑似截断/追加）；
//   - StatusDigestMismatch：对象存在、长度一致，但内容摘要不符（疑似篡改）。
//
// 校验不会在第一个坏对象处中止：每个对象都给出独立结论，便于整库导出
// 排查。无任何清单/索引引用的孤立对象没有声明大小，按摘要校验，
// 此时不存在“长度声明”，损坏只能归类为 digest-mismatch。
package verify

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
)

// Status 是单个对象的完整性结论。
type Status string

const (
	// StatusOK 对象完整可用。
	StatusOK Status = "ok"
	// StatusMissing 被声明/引用，但对象不存在。
	StatusMissing Status = "missing"
	// StatusSizeMismatch 对象存在但字节长度与声明不符（疑似截断）。
	StatusSizeMismatch Status = "size-mismatch"
	// StatusDigestMismatch 对象存在、长度一致但摘要不符（疑似篡改）；
	// 无大小声明时的损坏也归此类。
	StatusDigestMismatch Status = "digest-mismatch"
)

// Finding 记录一个对象的校验结论，供逐对象导出排查。
type Finding struct {
	Digest digest.Digest `json:"digest"`
	Status Status        `json:"status"`
	// DeclaredSize 已知的声明大小；-1 表示无声明（无引用的孤立对象）。
	DeclaredSize int64 `json:"declaredSize"`
	// ActualSize 实测长度；对象缺失时为 -1。
	ActualSize int64 `json:"actualSize"`
	// Error 非 OK 结论附带的原始错误（OK 时为空）。
	Error string `json:"error,omitempty"`
}

// Report 全库校验明细。
type Report struct {
	Findings []Finding `json:"findings"`
}

// Counts 返回各结论数量。
func (r *Report) Counts() map[Status]int {
	out := map[Status]int{}
	for _, f := range r.Findings {
		out[f.Status]++
	}
	return out
}

// Verifier 全库校验器。
type Verifier struct {
	store    *cas.Store
	registry *registry.Registry
}

// New 创建校验器。
func New(store *cas.Store, reg *registry.Registry) *Verifier {
	return &Verifier{store: store, registry: reg}
}

// Run 枚举存储中全部对象并逐个给出完整性结论；同时把所有引用中声明、
// 但 blobs 中不存在的对象记为 missing。返回的 Report 永远包含全部
// 结论，只有“无法枚举存储”这类环境错误才返回非空 error。
func (v *Verifier) Run(ctx context.Context) (*Report, error) {
	claims, err := v.collectClaims(ctx)
	if err != nil {
		return nil, err
	}

	present, err := v.store.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("verify: list objects: %w", err)
	}
	presentSet := make(map[digest.Digest]struct{}, len(present))

	rep := &Report{}
	for _, d := range present {
		presentSet[d] = struct{}{}
		declared := int64(-1)
		if c, ok := claims[d]; ok {
			declared = c.Size
		}
		size, ierr := v.store.Inspect(ctx, d, declared)
		rep.Findings = append(rep.Findings, Finding{
			Digest:       d,
			Status:       classify(ierr),
			DeclaredSize: declared,
			ActualSize:   actualSize(ierr, size),
			Error:        errText(ierr),
		})
	}

	// 被某清单/索引声明但根本不在存储中的对象：missing。
	var missing []digest.Digest
	for d := range claims {
		if _, ok := presentSet[d]; ok {
			continue
		}
		missing = append(missing, d)
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	for _, d := range missing {
		rep.Findings = append(rep.Findings, Finding{
			Digest:       d,
			Status:       StatusMissing,
			DeclaredSize: claims[d].Size,
			ActualSize:   -1,
			Error:        fmt.Sprintf("%v", fmt.Errorf("%w: %s", cas.ErrNotFound, d.Short())),
		})
	}

	sort.Slice(rep.Findings, func(i, j int) bool {
		return rep.Findings[i].Digest < rep.Findings[j].Digest
	})
	return rep, nil
}

// collectClaims 汇总所有引用闭包中出现过的描述符声明（digest+size），
// 以及存储中“内容确实是合法清单/索引”的对象内部声明。引用本身解析
// 失败（损坏引用等）不阻断全库校验：引用问题在 GC/提交链路上暴露，
// 这里只汇总能读到的大小声明。
func (v *Verifier) collectClaims(ctx context.Context) (map[digest.Digest]manifest.Descriptor, error) {
	claims := map[digest.Digest]manifest.Descriptor{}

	// 1) 存储中每个 hash 完整的对象：若它是清单/索引，收录其内部描述符。
	all, err := v.store.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("verify: list objects: %w", err)
	}
	for _, d := range all {
		data, gerr := v.store.GetBytes(ctx, d)
		if gerr != nil {
			// 对象自身损坏：稍后由 Inspect 归类，无法从它提取声明。
			continue
		}
		mt, perr := manifest.ParseKind(data)
		if perr != nil {
			continue // 非清单/索引或不受支持格式：不产生大小声明
		}
		switch mt {
		case manifest.MediaTypeManifest:
			m, perr := manifest.ParseManifest(data)
			if perr != nil {
				continue
			}
			mergeClaim(claims, manifest.Descriptor{
				MediaType: manifest.MediaTypeConfig, Digest: m.Config.Digest, Size: m.Config.Size,
			})
			for _, l := range m.Layers {
				mergeClaim(claims, l)
			}
		case manifest.MediaTypeIndex:
			idx, perr := manifest.ParseIndex(data)
			if perr != nil {
				continue
			}
			for _, child := range idx.Manifests {
				mergeClaim(claims, child)
			}
		}
	}

	// 2) 所有现有引用的目标也登记声明（覆盖引用直接指向的根对象）。
	if v.registry != nil {
		refs, rerr := v.registry.AllRefs(ctx)
		if rerr == nil {
			for _, ref := range refs {
				if desc, rerr := v.registry.Resolve(ctx, ref); rerr == nil {
					mergeClaim(claims, desc)
				}
			}
		}
	}
	return claims, nil
}

// mergeClaim 收录一条描述符声明；同一 digest 的 size 声明不一致时保留
// 首个——矛盾声明会在 Inspect 阶段以 size-mismatch 暴露其中至少一条，
// 这里不猜测哪条正确。
func mergeClaim(into map[digest.Digest]manifest.Descriptor, d manifest.Descriptor) {
	if _, err := digest.Parse(string(d.Digest)); err != nil {
		return
	}
	if _, ok := into[d.Digest]; !ok {
		into[d.Digest] = d
	}
}

// classify 把 cas.Inspect 的错误映射到三类非 OK 结论；nil 即 OK。
func classify(err error) Status {
	switch {
	case err == nil:
		return StatusOK
	case errors.Is(err, cas.ErrSizeMismatch):
		return StatusSizeMismatch
	case errors.Is(err, cas.ErrDigestMismatch):
		return StatusDigestMismatch
	case errors.Is(err, cas.ErrNotFound):
		return StatusMissing
	default:
		// 其他 I/O 错误不伪装成已知结论：按摘要口径失败处理，保留原始信息。
		return StatusDigestMismatch
	}
}

func actualSize(err error, inspected int64) int64 {
	if err != nil {
		if errors.Is(err, cas.ErrNotFound) {
			return -1
		}
	}
	return inspected
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
