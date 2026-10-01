// Package verify 对本地内容寻址存储做全库巡检：逐对象校验 blob 完整性，
// 并逐条校验命名引用（name:tag）自身格式与其指向的整棵闭包。
//
// 与 cas.Inspect / registry.VerifyClosure 共用同一套完整性三分类口径：
//   - StatusOK：对象存在、长度与声明一致、摘要通过；
//   - StatusMissing：被引用对象根本不存在（不存在于 blobs 目录）；
//   - StatusSizeMismatch：对象存在但长度与声明不符（疑似截断/追加）；
//   - StatusDigestMismatch：对象存在、长度一致，但内容摘要不符（疑似篡改）。
//
// 引用结论（RefStatus）与读取、回收、提交前检查的错误一一对应，绝不把
// 引用问题伪装成“引用不存在”而静默跳过：
//   - RefStatusOK：引用可解析且整棵闭包完整；
//   - RefStatusCorruptRef：引用文件读不出来（JSON 无法解析）或结构不合法
//     （目标描述符三要素非法），对应 registry.ErrCorruptRef；
//   - RefStatusUnsupportedVersion：formatVersion 超出当前支持范围，
//     对应 registry.ErrUnsupportedReferenceVersion；无版本字段的旧格式
//     引用按 1 解释，照常报 OK，两种格式绝不混着解释；
//   - RefStatusMissing / RefStatusSizeMismatch / RefStatusDigestMismatch：
//     引用闭包中某对象的三类完整性结论，与 blob 结论同口径；
//   - RefStatusMalformed：闭包中的清单/索引自身结构不合法或描述符标识
//     与内容种类不一致（manifest.ErrMalformed 等）。
//
// 巡检不会在第一个坏对象/坏引用处中止：每个对象、每条引用、闭包中的
// 每个位置都给出独立结论，便于整库导出排查。无任何清单/索引引用的孤立
// 对象没有声明大小，按摘要校验，损坏只能归类为 digest-mismatch。
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

// RefStatus 是一条命名引用（name:tag）的巡检结论。
type RefStatus string

const (
	// RefStatusOK 引用可解析（含无版本字段的旧格式）且整棵闭包完整。
	RefStatusOK RefStatus = "ok"
	// RefStatusCorruptRef 引用文件本身读不出来或结构不合法
	// （registry.ErrCorruptRef），不伪装成“引用不存在”。
	RefStatusCorruptRef RefStatus = "corrupt-ref"
	// RefStatusUnsupportedVersion 引用格式版本超出当前支持范围
	// （registry.ErrUnsupportedReferenceVersion），明确拒绝、绝不猜测。
	RefStatusUnsupportedVersion RefStatus = "unsupported-reference-version"
	// RefStatusMissing 闭包中某对象根本不存在（cas.ErrNotFound）。
	RefStatusMissing RefStatus = "missing"
	// RefStatusSizeMismatch 闭包中某对象长度与描述符声明不符
	// （cas.ErrSizeMismatch）。
	RefStatusSizeMismatch RefStatus = "size-mismatch"
	// RefStatusDigestMismatch 闭包中某对象长度对但内容被改
	// （cas.ErrDigestMismatch）。
	RefStatusDigestMismatch RefStatus = "digest-mismatch"
	// RefStatusMalformed 闭包中的清单/索引结构不合法或描述符标识
	// 与对象实际种类不一致（manifest.ErrMalformed /
	// manifest.ErrUnsupportedFormat）。
	RefStatusMalformed RefStatus = "malformed"
)

// Finding 记录一个 blob 对象的校验结论，供逐对象导出排查。
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

// RefFinding 记录一条命名引用的巡检结论。闭包中出问题的每个位置各一条，
// 因此同一条引用可能产生多条非 OK 结论。
type RefFinding struct {
	// Ref 出问题的引用名（name:tag）；引用文件本身损坏时仍可由路径得出。
	Ref string `json:"ref"`
	// Status 引用级结论类别（见 RefStatus 常量）。
	Status RefStatus `json:"status"`
	// Where 问题在引用闭包中的位置（如 "target manifests[0] layers[1]"）；
	// 引用文件自身问题时为 "reference"。
	Where string `json:"where,omitempty"`
	// Digest 出问题的对象摘要；引用文件自身无法解析时为空。
	Digest digest.Digest `json:"digest,omitempty"`
	// DeclaredSize 该位置描述符声明的大小；无声明/无法解析时为 -1。
	DeclaredSize int64 `json:"declaredSize,omitempty"`
	// Error 非 OK 结论附带的原始错误（OK 时为空）。
	Error string `json:"error,omitempty"`
}

// Report 全库巡检明细：blob 逐对象结论 + 引用逐条结论。
type Report struct {
	Findings    []Finding    `json:"findings"`
	RefFindings []RefFinding `json:"refFindings"`
}

// Counts 返回各 blob 结论数量。
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

// Healthy 当且仅当全部 blob 与全部引用结论均为 OK 时为 true。
func (r *Report) Healthy() bool {
	for _, f := range r.Findings {
		if f.Status != StatusOK {
			return false
		}
	}
	for _, f := range r.RefFindings {
		if f.Status != RefStatusOK {
			return false
		}
	}
	return true
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

// Run 枚举存储中全部对象并逐个给出完整性结论；同时逐条巡检全部命名引用：
// 引用文件本身、其格式版本、以及闭包中每个对象/每个位置都给出独立结论。
// 返回的 Report 永远包含全部结论，只有“无法枚举存储/引用”这类环境错误
// 才返回非空 error；引用损坏、版本超范围、闭包缺对象等一律体现在
// RefFindings 中，绝不静默跳过、绝不退化为全绿。
func (v *Verifier) Run(ctx context.Context) (*Report, error) {
	// 整轮巡检与 gc.Run 互斥（与提交区间共享读侧）：巡检期间不会出现
	// “引用已解析、闭包对象却被回收删掉”的穿插，坏引用不可能因此被
	// 漏判成绿色。纯读引用不串行化（不持 registry.mu）：原子 rename
	// 保证只读到旧引用或新引用，二者都指向完整闭包。
	if v.registry != nil {
		v.registry.BeginInspect()
		defer v.registry.EndInspect()
	}

	claims, err := v.collectClaims(ctx)
	if err != nil {
		return nil, err
	}

	rep := &Report{}

	// 先逐条巡检引用：遍历时会把闭包中每个描述符的声明并入 claims，
	// 随后 blob 级结论才能拿到“仅由引用声明”的大小，损坏引用也不会让
	// 其闭包对象被误当成无声明孤立对象。
	if v.registry != nil {
		refs, rerr := v.registry.AllRefs(ctx)
		if rerr != nil {
			return nil, fmt.Errorf("verify: list references: %w", rerr)
		}
		sort.Strings(refs)
		for _, ref := range refs {
			v.inspectReference(ctx, ref, claims, rep)
		}
	}

	present, err := v.store.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("verify: list objects: %w", err)
	}
	presentSet := make(map[digest.Digest]struct{}, len(present))
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

	// 被某清单/索引/引用声明但根本不在存储中的对象：missing。
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

// inspectReference 巡检单条引用，把引用自身与闭包各位置的结论追加进 rep。
// 任何一类问题都只追加结论、不向上中止：整库导出必须覆盖每一条引用。
func (v *Verifier) inspectReference(ctx context.Context, ref string,
	claims map[digest.Digest]manifest.Descriptor, rep *Report) {
	desc, err := v.registry.Resolve(ctx, ref)
	if err != nil {
		st := classifyRefError(err)
		if st == RefStatusOK {
			return
		}
		rep.RefFindings = append(rep.RefFindings, RefFinding{
			Ref:          ref,
			Status:       st,
			Where:        "reference",
			DeclaredSize: -1,
			Error:        errText(err),
		})
		return
	}
	ok := v.walkReferenceClosure(ctx, ref, "target", desc, map[digest.Digest]struct{}{}, claims, rep)
	if ok {
		rep.RefFindings = append(rep.RefFindings, RefFinding{
			Ref: ref, Status: RefStatusOK, Where: "target",
			Digest: desc.Digest, DeclaredSize: desc.Size,
		})
	}
}

// walkReferenceClosure 沿 索引->清单->config/层 遍历引用闭包，对每个对象
// 走与提交闸门/回收完全相同的判定（store.Inspect 带描述符声明大小）。
// 与 registry.walkClosure 的“遇错即停”不同，巡检在此把每一类问题逐位置
// 记录后继续（仅在无法继续解析子节点时剪枝），保证一条引用闭包里的多个
// 问题都能被导出。返回值表示“整段遍历未发现任何问题”。
//
// 遍历经过的对象声明同时并入 claims：这样引用根描述符、以及仅由损坏引用
// 声明的对象大小也能进入 blob 级结论，而不是被当成无声明孤立对象。
func (v *Verifier) walkReferenceClosure(ctx context.Context, ref, where string,
	d manifest.Descriptor, seen map[digest.Digest]struct{},
	claims map[digest.Digest]manifest.Descriptor, rep *Report) bool {
	ok := true
	add := func(st RefStatus, dg digest.Digest, declared int64, err error) {
		rep.RefFindings = append(rep.RefFindings, RefFinding{
			Ref: ref, Status: st, Where: where,
			Digest: dg, DeclaredSize: declared, Error: errText(err),
		})
		ok = false
	}

	if err := d.Validate(true); err != nil {
		add(RefStatusCorruptRef, d.Digest, d.Size, err)
		return false
	}
	if _, dup := seen[d.Digest]; dup {
		return true
	}
	seen[d.Digest] = struct{}{}
	// 描述符合法即并入声明：对象缺失时 blob 级结论据此得到 declared 大小，
	// 引用级 missing 结论由本函数负责追加。
	mergeClaim(claims, d)

	if _, ierr := v.store.Inspect(ctx, d.Digest, d.Size); ierr != nil {
		st := classifyBlobToRef(ierr)
		add(st, d.Digest, d.Size, fmt.Errorf("%s: %w", where, ierr))
		// 任一完整性问题都无法再安全解析子节点：GetManifest 会重新过
		// Inspect 并拒绝打开（等长篡改也一样），在此剪枝。本子树的问题
		// 已记录；兄弟子树由调用方继续遍历，不受影响。
		return false
	}

	// config / 层是闭包叶子：只做存在性/大小/摘要校验，不解析为清单。
	if d.MediaType == manifest.MediaTypeConfig || d.MediaType == manifest.MediaTypeLayer {
		return ok
	}

	mt, data, gerr := manifest.GetManifest(ctx, v.store, d.Digest)
	if gerr != nil {
		// 拿不到字节（除上面已归类的缺失外，多为 I/O 问题）：无法继续。
		if !errors.Is(gerr, cas.ErrNotFound) && !errors.Is(gerr, cas.ErrSizeMismatch) &&
			!errors.Is(gerr, cas.ErrDigestMismatch) {
			add(RefStatusMalformed, d.Digest, d.Size, fmt.Errorf("%s: %w", where, gerr))
		}
		return false
	}
	if mt != d.MediaType {
		add(RefStatusMalformed, d.Digest, d.Size, fmt.Errorf(
			"%w: %s declared mediaType %s but content is %s",
			manifest.ErrMalformed, where, d.MediaType, mt))
		// 仍按实际内容继续遍历，尽量暴露深层问题。
	}
	switch mt {
	case manifest.MediaTypeIndex:
		idx, perr := manifest.ParseIndex(data)
		if perr != nil {
			add(RefStatusMalformed, d.Digest, d.Size, fmt.Errorf("%s: %w", where, perr))
			return false
		}
		for i, child := range idx.Manifests {
			mergeClaim(claims, child)
			cwhere := fmt.Sprintf("%s manifests[%d]", where, i)
			if !v.walkReferenceClosure(ctx, ref, cwhere, child, seen, claims, rep) {
				ok = false
			}
		}
	case manifest.MediaTypeManifest:
		m, perr := manifest.ParseManifest(data)
		if perr != nil {
			add(RefStatusMalformed, d.Digest, d.Size, fmt.Errorf("%s: %w", where, perr))
			return false
		}
		if m.Config.MediaType != manifest.MediaTypeConfig {
			add(RefStatusMalformed, d.Digest, d.Size,
				fmt.Errorf("%w: %s config mediaType %s", manifest.ErrMalformed, where, m.Config.MediaType))
		}
		mergeClaim(claims, m.Config)
		if !v.walkReferenceClosure(ctx, ref, where+" config", m.Config, seen, claims, rep) {
			ok = false
		}
		for i, l := range m.Layers {
			mergeClaim(claims, l)
			if l.MediaType != manifest.MediaTypeLayer {
				add(RefStatusMalformed, l.Digest, l.Size,
					fmt.Errorf("%w: %s layers[%d] mediaType %s",
						manifest.ErrMalformed, where, i, l.MediaType))
			}
			lwhere := fmt.Sprintf("%s layers[%d]", where, i)
			if !v.walkReferenceClosure(ctx, ref, lwhere, l, seen, claims, rep) {
				ok = false
			}
		}
	default:
		add(RefStatusMalformed, d.Digest, d.Size, fmt.Errorf(
			"%w: %s unsupported mediaType %s", manifest.ErrUnsupportedFormat, where, mt))
	}
	return ok
}

// classifyRefError 把引用解析错误映射到明确的引用级结论类别。
func classifyRefError(err error) RefStatus {
	switch {
	case err == nil:
		return RefStatusOK
	case errors.Is(err, registry.ErrUnsupportedReferenceVersion):
		return RefStatusUnsupportedVersion
	case errors.Is(err, registry.ErrCorruptRef):
		return RefStatusCorruptRef
	case errors.Is(err, registry.ErrNoSuchRef):
		// AllRefs 刚列出的引用转瞬被删：本轮没有结论，下一轮即消失。
		return RefStatusOK
	default:
		// 其他 I/O 错误不伪装成已知引用类别：按引用损坏处理，保留原始信息。
		return RefStatusCorruptRef
	}
}

// classifyBlobToRef 把 cas.Inspect 的三类完整性错误映射为引用级闭包结论。
func classifyBlobToRef(err error) RefStatus {
	switch {
	case errors.Is(err, cas.ErrSizeMismatch):
		return RefStatusSizeMismatch
	case errors.Is(err, cas.ErrDigestMismatch):
		return RefStatusDigestMismatch
	case errors.Is(err, cas.ErrNotFound):
		return RefStatusMissing
	default:
		return RefStatusMalformed
	}
}

// collectClaims 汇总“存储中内容确实是合法清单/索引”的对象内部描述符
// 声明（digest+size）。引用闭包的声明在 inspectReference 的逐位置遍历
// 中并入：因此损坏引用、超版本引用即使无法解析，也绝不会被当成“没找到”
// 而跳过——它们各自在 RefFindings 中有明确结论。
func (v *Verifier) collectClaims(ctx context.Context) (map[digest.Digest]manifest.Descriptor, error) {
	claims := map[digest.Digest]manifest.Descriptor{}

	// 存储中每个 hash 完整的对象：若它是清单/索引，收录其内部描述符。
	// 这覆盖孤立的（未被任何引用指向的）清单/索引；被引用闭包中的声明
	// 由 walkReferenceClosure 负责，坏引用不影响此处收录其他对象。
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
