package registry

import (
	"context"
	"fmt"

	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
)

// pinSet 是事务内登记的读保护集合：每个 VerifyClosurePinned 返回的释放
// 函数登记一次，事务结束时统一释放。
type pinSet struct {
	releases []func()
}

func (ps *pinSet) add(rel func()) { ps.releases = append(ps.releases, rel) }

func (ps *pinSet) release() {
	for _, rel := range ps.releases {
		rel()
	}
	ps.releases = nil
}

// pinClosure 校验闭包并把其中每个对象登记读 pin（重复对象幂等计数）。
func (ps *pinSet) pinClosure(ctx context.Context, r *Registry, root digest.Digest, rootSize int64) error {
	_, release, err := r.VerifyClosurePinned(ctx, root, rootSize)
	if err != nil {
		return err
	}
	ps.add(release)
	return nil
}

// beginCommit 在持锁事务开始时：读取引用基线、pin 住其当前闭包。
// 引用尚不存在时基线 exists=false、pin 集合为空。
func (r *Registry) beginCommit(ctx context.Context, ref string) (*pinSet, refBase, error) {
	ps := &pinSet{}
	base, err := r.readBaseLocked(ref)
	if err != nil {
		return nil, refBase{}, err
	}
	if base.exists {
		if err := ps.pinClosure(ctx, r, base.root, base.size); err != nil {
			ps.release()
			return nil, refBase{}, err
		}
	}
	return ps, base, nil
}

// CommitImagePlan 以“组装计划”原子提交单镜像：在写任何 blob 之前先按最终
// 摘要 pin 住整个新镜像闭包，再写入并条件发布引用。这是并发回收下没有
// “写入 -> 发布”不可达窗口的单镜像提交入口。
func (r *Registry) CommitImagePlan(ctx context.Context, ref string, plan ImagePlan) (_ manifest.Descriptor, err error) {
	if plan.Platform != nil {
		return manifest.Descriptor{}, classifyCommitError(fmt.Errorf("%w: image plan must not carry platform", ErrCommitInvalid))
	}
	layers, cfg, mblob, desc, merr := materializePlan(plan)
	if merr != nil {
		return manifest.Descriptor{}, classifyCommitError(merr)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	releaseGate := r.store.Lock().BeginCommit()
	defer releaseGate()
	ps, base, err := r.beginCommit(ctx, ref)
	if err != nil {
		return manifest.Descriptor{}, classifyCommitError(err)
	}
	defer ps.release()

	blobs := make([]plannedBlob, 0, len(layers)+2)
	blobs = append(blobs, layers...)
	blobs = append(blobs, cfg, mblob)
	if err := r.pinBlobsBeforeWrite(ctx, ps, blobs...); err != nil {
		return manifest.Descriptor{}, classifyCommitError(err)
	}
	if err := r.publishIfBaseLocked(ctx, ref, desc, base); err != nil {
		return manifest.Descriptor{}, classifyCommitError(err)
	}
	return desc, nil
}

// CommitPlatformPlan 以“组装计划”原子地组装平台镜像并并入/创建索引：
// 写任何 blob 前先 pin 新镜像闭包，写新索引前先 pin 其摘要，最后条件发布。
//
// 若事务读取基线后、发布前引用被并发改到别处，返回 ErrCommitConflict
// （可 errors.Is 到 ErrCommitState），本次提交未生效；调用方应重读-重组装-
// 重试。整个动作因此对外只呈现“旧版本”或“新版本”，从不覆盖他人提交。
func (r *Registry) CommitPlatformPlan(ctx context.Context, ref string, plan ImagePlan) (_ manifest.Descriptor, err error) {
	if plan.Platform == nil {
		return manifest.Descriptor{}, classifyCommitError(fmt.Errorf("%w: platform plan requires platform", ErrCommitInvalid))
	}
	p := *plan.Platform
	if err := p.Valid(); err != nil {
		return manifest.Descriptor{}, classifyCommitError(err)
	}
	layers, cfg, mblob, mdesc, merr := materializePlan(plan)
	if merr != nil {
		return manifest.Descriptor{}, classifyCommitError(merr)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	releaseGate := r.store.Lock().BeginCommit()
	defer releaseGate()
	ps, base, err := r.beginCommit(ctx, ref)
	if err != nil {
		return manifest.Descriptor{}, classifyCommitError(err)
	}
	defer ps.release()

	// 1) 新镜像闭包：先 pin 再写。
	blobs := make([]plannedBlob, 0, len(layers)+2)
	blobs = append(blobs, layers...)
	blobs = append(blobs, cfg, mblob)
	if err := r.pinBlobsBeforeWrite(ctx, ps, blobs...); err != nil {
		return manifest.Descriptor{}, classifyCommitError(err)
	}

	// 2) 新索引：先 pin 摘要再落地。
	idx, berr := r.loadIndexForAppendLocked(ctx, ref, mdesc, p)
	if berr != nil {
		return manifest.Descriptor{}, classifyCommitError(berr)
	}
	canonical, cerr := manifest.EncodeIndex(idx)
	if cerr != nil {
		return manifest.Descriptor{}, classifyCommitError(cerr)
	}
	ps.add(r.store.Lock().Pin(digest.FromBytes(canonical)))
	idesc, perr := manifest.PutManifest(ctx, r.store, canonical)
	if perr != nil {
		return manifest.Descriptor{}, classifyCommitError(perr)
	}

	// 3) 条件发布：基线未变才原子替换引用。
	if err := r.publishIfBaseLocked(ctx, ref, idesc, base); err != nil {
		return manifest.Descriptor{}, classifyCommitError(err)
	}
	return idesc, nil
}
