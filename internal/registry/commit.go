package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
)

// 提交动作的可区分失败类别：调用方用 errors.Is 判定“为什么这次提交没成”，
// 而不是只拿到一句笼统的失败。
//
// 无论返回哪一类错误，引用都保持提交前的状态：原来指向谁仍指向谁，原来
// 不存在就仍不存在；本次新组装但未发布的对象只是不可达 blob，下一轮
// 回收即清掉，不会越攒越多。
var (
	// ErrCommitIntegrity 提交前完整性检查未通过：闭包中有对象
	// 缺失(cas.ErrNotFound)/截断(cas.ErrSizeMismatch)/篡改(cas.ErrDigestMismatch)。
	// 错误链上同时保留具体 cas 哨兵，可进一步区分三类。
	ErrCommitIntegrity = errors.New("registry: commit rejected: target image not intact")
	// ErrCommitInvalid 提交内容本身不合法（描述符/平台非法、格式不受支持）。
	ErrCommitInvalid = errors.New("registry: commit rejected: invalid target")
	// ErrCommitState 引用当前状态不接受该提交（如向清单引用追加平台、
	// 平台重复、读取到损坏/超版本的旧引用）。
	ErrCommitState = errors.New("registry: commit rejected: reference state conflict")
	// ErrCommitConflict 乐观并发冲突：事务读取基线后、发布前，引用已被
	// 另一提交改到别处。本次提交未生效，调用方应重读-重组装-重试。
	ErrCommitConflict = errors.New("registry: commit conflict: reference changed concurrently")
)

// classifyCommitError 把内部各环节的错误归入三类提交失败原因之一。
//
//   - cas 缺失/截断/篡改、清单格式错误 -> ErrCommitIntegrity；
//   - 描述符/平台/序列化非法 -> ErrCommitInvalid；
//   - 引用状态冲突（错种类、重复平台、损坏/超版本引用、引用不存在语义等）
//     -> ErrCommitState；
//   - 引用名非法等参数错误保持原样（调用方用法错误，不是提交语义失败）。
func classifyCommitError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, cas.ErrNotFound),
		errors.Is(err, cas.ErrSizeMismatch),
		errors.Is(err, cas.ErrDigestMismatch),
		errors.Is(err, cas.ErrCorrupt),
		errors.Is(err, cas.ErrInvalidDigest),
		errors.Is(err, manifest.ErrMalformed),
		errors.Is(err, manifest.ErrUnsupportedFormat):
		return fmt.Errorf("%w: %w", ErrCommitIntegrity, err)
	case errors.Is(err, ErrWrongKind),
		errors.Is(err, ErrDuplicatePlatform),
		errors.Is(err, ErrCorruptRef),
		errors.Is(err, ErrUnsupportedReferenceVersion),
		errors.Is(err, ErrCommitConflict):
		return fmt.Errorf("%w: %w", ErrCommitState, err)
	default:
		// 描述符 Validate 等参数合法性问题统一归为 invalid。
		return fmt.Errorf("%w: %w", ErrCommitInvalid, err)
	}
}

// CommitImage 原子地把一个“从头到尾完整可用”的单镜像清单挂到引用上。
//
// 对外就是一次提交（事务）：
//
//   - 成功：引用指向 desc，且其 config 与每个层都存在、大小与摘要全部
//     正确，索引/清单本身也通过格式与位置校验；
//   - 失败：引用保持提交前状态（原本不存在则仍不存在），外部任何时刻
//     都读不到“做了一半”的中间状态；
//   - 失败原因可用 errors.Is 区分：ErrCommitIntegrity（再细分
//     cas.ErrNotFound/ErrSizeMismatch/ErrDigestMismatch）、
//     ErrCommitInvalid、ErrCommitState。
//
// 与并发回收/校验/其他组装的协调：完整性校验期间对整个闭包持有读 pin，
// 直到引用原子 rename 完成；rename 后由新引用提供可达性保护。
func (r *Registry) CommitImage(ctx context.Context, ref string, desc manifest.Descriptor) error {
	if err := desc.Validate(false); err != nil {
		// 单镜像提交的根必须是清单（不允许直接把索引当镜像提交）。
		return fmt.Errorf("%w: %w", ErrCommitInvalid, err)
	}
	if desc.MediaType != manifest.MediaTypeManifest {
		return fmt.Errorf("%w: root must be %s, got %s",
			ErrCommitInvalid, manifest.MediaTypeManifest, desc.MediaType)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	releaseGate := r.store.Lock().BeginCommit()
	defer releaseGate()
	ps, base, err := r.beginCommit(ctx, ref)
	if err != nil {
		return classifyCommitError(err)
	}
	defer ps.release()
	if err := r.publishIfBaseLocked(ctx, ref, desc, base); err != nil {
		return classifyCommitError(err)
	}
	return nil
}

// CommitPlatform 原子地把一个平台清单并入引用指向的多架构索引并发布。
//
//   - ref 不存在：创建只含该平台的索引；
//   - ref 指向索引：并入新平台，平台完全重复 -> ErrCommitState
//     （链 ErrDuplicatePlatform）；
//   - ref 指向清单：不允许，ErrCommitState（链 ErrWrongKind）；
//   - 旧格式（无 formatVersion）引用照常工作；超版本/损坏引用 ->
//     ErrCommitState（分别链 ErrUnsupportedReferenceVersion/ErrCorruptRef）。
//
// 发布前对“新索引的完整闭包”（既有平台 + 新平台）做校验：任一对象
// 缺失/截断/篡改则整体失败，旧索引引用原样保留。
func (r *Registry) CommitPlatform(ctx context.Context, ref string, m manifest.Descriptor, p manifest.Platform) error {
	if err := m.Validate(false); err != nil {
		return fmt.Errorf("%w: %w", ErrCommitInvalid, err)
	}
	if m.MediaType != manifest.MediaTypeManifest {
		return fmt.Errorf("%w: only manifests can be committed to an index, got %s",
			ErrCommitInvalid, m.MediaType)
	}
	if err := p.Valid(); err != nil {
		return fmt.Errorf("%w: %w", ErrCommitInvalid, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.attachLocked(ctx, ref, m, p); err != nil {
		return classifyCommitError(err)
	}
	return nil
}
