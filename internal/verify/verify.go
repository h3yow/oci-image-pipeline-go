// Package verify 对本地内容寻址存储做全库逐对象的完整性校验。
//
// 与 cas.Inspect / registry.VerifyClosure / gc 共用同一套判定口径：
//   - StatusOK：对象存在、长度与声明一致、摘要通过；
//   - StatusMissing：被声明/引用，但对象根本不存在；
//   - StatusSizeMismatch：对象存在但长度与声明不符（疑似截断/追加）；
//   - StatusDigestMismatch：对象存在、长度一致但摘要不符（疑似篡改），
//     无大小声明的孤立对象损坏也归此类；
//   - StatusMalformed：对象字节完整（摘要通过），但作为清单/索引其
//     结构无法解析或自相矛盾（含描述符非法、标识与实际种类不符）；
//   - StatusUnsupportedFormat：对象字节完整，但格式/媒体类型超出
//     本系统支持范围，明确拒绝而非猜测解释。
//
// 引用文件自身的问题单列在 RefFindings 中，绝不伪装成“引用不存在”、
// 也不会被静默跳过：
//   - StatusCorruptRef：引用读不出来或结构不合法（含目标描述符非法）；
//   - StatusUnsupportedRefVersion：引用 formatVersion 超出支持范围。
//
// 校验不会在第一个坏对象处中止：每个对象、每条引用都给出独立结论，
// 便于整库导出排查。整轮校验在 registry 快照区间内完成：期间 gc 无法
// 删除被引用对象、提交发布的引用闭包也必然完整，因此并发下不会出现
// “引用已坏、巡检却给绿色结论”的窗口。
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
	// StatusMalformed 对象字节完整，但作为清单/索引结构非法
	// （无法解析、自相矛盾、描述符非法或种类标识与内容不符）。
	StatusMalformed Status = "malformed"
	// StatusUnsupportedFormat 对象字节完整，但格式/媒体类型超出支持范围。
	StatusUnsupportedFormat Status = "unsupported-format"
)

// RefStatus 是引用文件自身的结论类别。
type RefStatus string

const (
	// StatusCorruptRef 引用文件存在但读不出来或结构不合法。
	StatusCorruptRef RefStatus = "corrupt-ref"
	// StatusUnsupportedRefVersion 引用 formatVersion 超出支持范围。
	StatusUnsupportedRefVersion RefStatus = "unsupported-ref-version"
)

// Finding 记录一个对象的校验结论，供逐对象导出排查。
type Finding struct {
	Digest digest.Digest `json:"digest"`
	Status Status        `json:"status"`
	// DeclaredSize 已知的声明大小；-1 表示无声明（无引用的孤立对象）。
	DeclaredSize int64 `json:"declaredSize"`
	// ActualSize 实测长度；对象缺失时为 -1。
	ActualSize int64 `json:"actualSize"`
	// ReferencedBy 经哪些引用的闭包可达该对象（排序去重）；
	// 孤立对象（仅磁盘存在、无引用闭包可达）为空。
	ReferencedBy []string `json:"referencedBy,omitempty"`
	// Location 该对象第一次在引用结构中被看到的位置（便于对上具体引用）。
	Location string `json:"location,omitempty"`
	// Error 非 OK 结论附带的原始错误（OK 时为空）。
	Error string `json:"error,omitempty"`
}

// RefFinding 记录一条引用文件自身的结论。
type RefFinding struct {
	Ref    string    `json:"ref"`
	Status RefStatus `json:"status"`
	Error  string    `json:"error"`
}

// Report 全库校验明细。
type Report struct {
	// Findings 每个对象一条结论（按摘要排序）。
	Findings []Finding `json:"findings"`
	// RefFindings 每条无法正常解析的引用一条结论（按引用名排序）。
	RefFindings []RefFinding `json:"refFindings,omitempty"`
}

// Counts 返回各对象结论数量。
func (r *Report) Counts() map[Status]int {
	out := map[Status]int{}
	for _, f := range r.Findings {
		out[f.Status]++
	}
	return out
}

// RefCounts 返回各引用结论数量。
func (r *Report) RefCounts() map[RefStatus]int {
	out := map[RefStatus]int{}
	for _, f := range r.RefFindings {
		out[f.Status]++
	}
	return out
}

// Failed 报告中是否存在任何问题（对象非 OK 或引用无法解析）。
func (r *Report) Failed() bool {
	for _, f := range r.Findings {
		if f.Status != StatusOK {
			return true
		}
	}
	return len(r.RefFindings) > 0
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

// acc 汇总遍历过程中的逐对象结论。
type acc struct {
	// findings 以摘要原文为键，同一对象的多次观测合并成一条。
	findings map[digest.Digest]*Finding
	// claimed 记录经任一描述符（引用闭包或磁盘上的清单/索引内部声明）
	// 到达过的摘要：这些对象已有“带声明大小”的结论，基线摘要口径
	// 不得把截断等结论覆盖成 digest-mismatch。
	claimed map[digest.Digest]struct{}
	// expanded 记录已展开过内部描述符的文档，避免重复劳动与文档自指环。
	expanded map[digest.Digest]struct{}
}

// severity 数值越大优先级越高；同一对象多个矛盾观测时保留最严重结论。
func severity(s Status) int {
	switch s {
	case StatusDigestMismatch:
		return 6
	case StatusSizeMismatch:
		return 5
	case StatusMalformed:
		return 4
	case StatusUnsupportedFormat:
		return 3
	case StatusMissing:
		return 2
	case StatusOK:
		return 1
	}
	return 0
}

// observe 合并一条对象观测。
func (a *acc) observe(d digest.Digest, st Status, declared, actual int64, ref, where, errText string) {
	a.claimed[d] = struct{}{}
	f := a.findings[d]
	if f == nil {
		f = &Finding{Digest: d, DeclaredSize: declared, ActualSize: actual}
		a.findings[d] = f
	}
	if severity(st) > severity(f.Status) {
		f.Status = st
		f.DeclaredSize = declared
		f.ActualSize = actual
		f.Error = errText
	}
	if ref != "" {
		f.ReferencedBy = addSorted(f.ReferencedBy, ref)
	}
	if f.Location == "" && where != "" {
		f.Location = where
	}
}

func addSorted(list []string, s string) []string {
	i := sort.SearchStrings(list, s)
	if i < len(list) && list[i] == s {
		return list
	}
	list = append(list, "")
	copy(list[i+1:], list[i:])
	list[i] = s
	return list
}

// Run 枚举存储中全部对象与全部引用，逐个给出完整性结论；被清单/索引
// 声明但不存在的对象记为 missing，无法解析/超版本的引用单列引用结论。
// 返回的 Report 永远包含全部结论；只有“无法枚举存储/引用目录”这类
// 环境错误才返回非空 error。
func (v *Verifier) Run(ctx context.Context) (*Report, error) {
	// 整轮快照：与 gc.Run 互斥、与提交区间共享读锁。期间被引用对象
	// 不会被删除；能看到的引用状态不是提交前就是提交后，两者闭包完整。
	v.registry.BeginSnapshot()
	defer v.registry.EndSnapshot()

	a := &acc{
		findings: map[digest.Digest]*Finding{},
		claimed:  map[digest.Digest]struct{}{},
		expanded: map[digest.Digest]struct{}{},
	}
	rep := &Report{}

	// 1) 逐条枚举引用：解析失败按引用类别记账，绝不静默跳过；
	//    解析成功则沿其闭包逐对象判定。
	refs, err := v.registry.AllRefs(ctx)
	if err != nil {
		return nil, fmt.Errorf("verify: list refs: %w", err)
	}
	sort.Strings(refs)
	for _, ref := range refs {
		desc, rerr := v.registry.Resolve(ctx, ref)
		if rerr != nil {
			rep.RefFindings = append(rep.RefFindings, RefFinding{
				Ref: ref, Status: classifyRef(rerr), Error: rerr.Error(),
			})
			continue
		}
		v.walk(ctx, a, "reference "+ref+" target", desc, ref)
	}

	// 2) 枚举磁盘全部对象：带声明大小的结论已在上面给出，这里补孤立
	//    对象的摘要口径校验，并展开磁盘上（含无引用的）清单/索引内部
	//    声明，使“被清单声明但缺失/长度不符”同样能被发现。
	present, err := v.store.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("verify: list objects: %w", err)
	}
	sort.Slice(present, func(i, j int) bool { return present[i] < present[j] })
	for _, d := range present {
		if _, done := a.claimed[d]; !done {
			size, ierr := v.store.Inspect(ctx, d, -1)
			a.observe(d, classifyCas(ierr), -1, actualSize(ierr, size), "", "", errText(ierr))
		}
		if _, expanded := a.expanded[d]; expanded {
			continue
		}
		data, gerr := v.store.GetBytes(ctx, d)
		if gerr != nil {
			continue // 对象自身损坏：结论已由上面的 Inspect 给出
		}
		mt, perr := manifest.ParseKind(data)
		if perr != nil {
			continue // 非清单/索引或不受支持格式的普通对象：不展开
		}
		// 用文档自描述（规范字节长度恒等于其描述符 size）展开内部声明。
		self := manifest.Descriptor{MediaType: mt, Digest: d, Size: int64(len(data))}
		v.walk(ctx, a, "document "+d.Short(), self, "")
	}

	for _, f := range a.findings {
		// 经引用/文档遍历但最终仍是零值状态（理论上不会发生）兜底为 OK。
		if f.Status == "" {
			f.Status = StatusOK
		}
		rep.Findings = append(rep.Findings, *f)
	}
	sort.Slice(rep.Findings, func(i, j int) bool { return rep.Findings[i].Digest < rep.Findings[j].Digest })
	sort.Slice(rep.RefFindings, func(i, j int) bool { return rep.RefFindings[i].Ref < rep.RefFindings[j].Ref })
	return rep, nil
}

// walk 沿“引用/文档 -> 描述符”逐对象判定，口径与 registry.walkClosure
// 一致，但不在第一个坏对象处中止：每个对象都留下独立结论，无法继续
// 展开的坏分支才停止下钻。
func (v *Verifier) walk(ctx context.Context, a *acc, where string, d manifest.Descriptor, ref string) {
	// 描述符自身非法：按结构问题记账（摘要非法时仍保留原始串以便定位）。
	if err := d.Validate(true); err != nil {
		st := StatusMalformed
		if errors.Is(err, manifest.ErrUnsupportedFormat) {
			st = StatusUnsupportedFormat
		}
		a.observe(d.Digest, st, d.Size, -1, ref, where, err.Error())
		return
	}

	// 经描述符到达：带声明大小走统一口径 Inspect。
	size, ierr := v.store.Inspect(ctx, d.Digest, d.Size)
	st := classifyCas(ierr)
	if ierr != nil {
		a.observe(d.Digest, st, d.Size, actualSize(ierr, size), ref, where, ierr.Error())
		return // 缺失/损坏对象无法继续按清单展开
	}
	a.observe(d.Digest, StatusOK, d.Size, size, ref, where, "")

	// config / 层是闭包叶子：只做存在性/大小/摘要校验。
	if d.MediaType == manifest.MediaTypeConfig || d.MediaType == manifest.MediaTypeLayer {
		return
	}
	if _, ok := a.expanded[d.Digest]; ok {
		return
	}
	a.expanded[d.Digest] = struct{}{}

	data, gerr := v.store.GetBytes(ctx, d.Digest)
	if gerr != nil {
		// 与 Inspect 之间不应变化（快照期内 gc 不可运行）；如实归类。
		a.observe(d.Digest, classifyCas(gerr), d.Size, actualSize(gerr, size), ref, where, gerr.Error())
		return
	}
	mt, perr := manifest.ParseKind(data)
	if perr != nil {
		st := StatusMalformed
		if errors.Is(perr, manifest.ErrUnsupportedFormat) {
			st = StatusUnsupportedFormat
		}
		a.observe(d.Digest, st, d.Size, size, ref, where, perr.Error())
		return
	}
	// 描述符标识与对象实际种类必须一致，不允许混用。
	if mt != d.MediaType {
		a.observe(d.Digest, StatusMalformed, d.Size, size, ref, where,
			fmt.Errorf("%w: %s declared mediaType %s but content is %s",
				manifest.ErrMalformed, where, d.MediaType, mt).Error())
		return
	}
	switch mt {
	case manifest.MediaTypeIndex:
		idx, perr := manifest.ParseIndex(data)
		if perr != nil {
			st := StatusMalformed
			if errors.Is(perr, manifest.ErrUnsupportedFormat) {
				st = StatusUnsupportedFormat
			}
			a.observe(d.Digest, st, d.Size, size, ref, where, perr.Error())
			return
		}
		for i, child := range idx.Manifests {
			v.walk(ctx, a, fmt.Sprintf("%s manifests[%d]", where, i), child, ref)
		}
	case manifest.MediaTypeManifest:
		m, perr := manifest.ParseManifest(data)
		if perr != nil {
			st := StatusMalformed
			if errors.Is(perr, manifest.ErrUnsupportedFormat) {
				st = StatusUnsupportedFormat
			}
			a.observe(d.Digest, st, d.Size, size, ref, where, perr.Error())
			return
		}
		v.walk(ctx, a, where+" config", m.Config, ref)
		for i, l := range m.Layers {
			v.walk(ctx, a, fmt.Sprintf("%s layers[%d]", where, i), l, ref)
		}
	}
}

// classifyRef 把引用解析错误映射到引用结论类别；未知错误也按损坏引用
// 处理（引用确实无法正常使用），绝不伪装成“没找到”而跳过。
func classifyRef(err error) RefStatus {
	switch {
	case errors.Is(err, registry.ErrUnsupportedReferenceVersion):
		return StatusUnsupportedRefVersion
	default:
		return StatusCorruptRef
	}
}

// classifyCas 把 cas.Inspect 的错误映射到三类非 OK 结论；nil 即 OK。
func classifyCas(err error) Status {
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
	if err != nil && errors.Is(err, cas.ErrNotFound) {
		return -1
	}
	return inspected
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
