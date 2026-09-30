// Package commit 把“组装一个镜像并把它挂到引用上”收敛为一次原子提交。
//
// 提交语义（对外像一次提交）：
//   - 成功：引用原子指向一棵从头到尾完整可用的对象树（索引选中的清单、
//     清单的 config 与每个层都存在、长度一致、摘要通过）；
//   - 失败：整体失败，引用保持提交前状态，存储中不会积累只有这次失败
//     提交才引用的垃圾（重试结果与历史失败无关；游离对象由下一轮 gc
//     回收，长期占用只随当前可达集变化）。
//
// 实现上，整段“组装对象 -> 完整性闸门校验 -> 原子发布引用”都在
// registry.BeginCommit 区间内：该区间与 gc.Run 整轮互斥，并串行化
// 引用变更。因此对象不会在“已写入但引用尚未发布”的窗口里被回收，
// 外部也不可能读到挂了一半的引用。失败原因可用 errors.Is 精确区分
// （cas.ErrNotFound / cas.ErrSizeMismatch / cas.ErrDigestMismatch、
// registry.ErrDuplicatePlatform 等）。
package commit

import (
	"context"
	"errors"
	"fmt"

	"github.com/h3yow/oci-image-pipeline-go/internal/assemble"
	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
)

// ErrCommitFailed 提交未能完成：引用没有被改动。具体原因由 errors.Is
// 进一步判定（cas.ErrNotFound/cas.ErrSizeMismatch/cas.ErrDigestMismatch、
// registry.ErrDuplicatePlatform 等）。
var ErrCommitFailed = errors.New("commit: commit failed")

// Request 一次提交的输入。
type Request struct {
	// Ref 目标引用 name:tag。
	Ref string
	// Image 待组装的单平台镜像。
	Image assemble.ImageSpec
	// Platform 平台标识（AsIndex 时必填 os/architecture）。
	Platform manifest.Platform
	// AsIndex 为 true 时把清单作为一个平台追加到 Ref 的多架构索引
	// （Ref 不存在则建索引）；为 false 时直接把清单指向 Ref。
	AsIndex bool
}

// Result 提交结果。
type Result struct {
	Manifest manifest.Descriptor `json:"manifest"`
	// Target 提交成功后引用所指的目标描述符（索引或清单）。
	Target manifest.Descriptor `json:"target"`
}

// Committer 在给定存储与仓库上执行原子提交。
type Committer struct {
	store    *cas.Store
	registry *registry.Registry
	asm      *assemble.Assembler
}

// New 创建提交器。
func New(store *cas.Store, reg *registry.Registry) *Committer {
	return &Committer{store: store, registry: reg, asm: assemble.New(store)}
}

// Commit 执行一次“组装并挂到引用”的原子提交。
//
// 组装（写层/config/清单）与发布处在同一个 BeginCommit 区间：
//   - 组装阶段失败：尚未触碰任何引用，直接返回，引用原状不变；
//   - 闸门失败（如某层缺失/截断/篡改）：发布前被 VerifyClosure 拦下，
//     引用仍是旧目标；
//   - 成功：引用可见时，整棵对象树必然完整，且对象不会被并发 gc 回收。
//
// 同一 Request 重试是幂等的：相同 spec 组装出相同摘要（CAS 去重），
// AsIndex=false 时再次指向同一清单；AsIndex=true 时重复平台得到
// registry.ErrDuplicatePlatform（同时链 ErrCommitFailed），引用不变。
func (c *Committer) Commit(ctx context.Context, req Request) (*Result, error) {
	if req.AsIndex {
		if err := req.Platform.Valid(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrCommitFailed, err)
		}
	}
	release := c.registry.BeginCommit()
	defer release()

	mdesc, _, err := c.asm.AssembleImage(ctx, req.Image)
	if err != nil {
		return nil, fmt.Errorf("%w: assemble for %s: %w", ErrCommitFailed, req.Ref, err)
	}
	return c.publishLocked(ctx, req.Ref, mdesc, req.Platform, req.AsIndex)
}

// Publish 在单个原子提交区间内，把“调用方已自行组装好”的清单挂到引用上。
// 与 Commit 相同：整闭包完整性闸门通过后才原子发布，失败引用不变。
func (c *Committer) Publish(ctx context.Context, ref string, m manifest.Descriptor, platform manifest.Platform, asIndex bool) (*Result, error) {
	if asIndex {
		if err := platform.Valid(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrCommitFailed, err)
		}
	}
	if err := m.Validate(true); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCommitFailed, err)
	}
	release := c.registry.BeginCommit()
	defer release()
	return c.publishLocked(ctx, ref, m, platform, asIndex)
}

// publishLocked 在已持有的提交区间内执行闸门校验与发布。
func (c *Committer) publishLocked(ctx context.Context, ref string, mdesc manifest.Descriptor, platform manifest.Platform, asIndex bool) (*Result, error) {
	if !asIndex {
		if err := c.registry.TagInCommit(ctx, ref, mdesc); err != nil {
			return nil, fmt.Errorf("%w: ref %s: %w", ErrCommitFailed, ref, err)
		}
		return &Result{Manifest: mdesc, Target: mdesc}, nil
	}
	if err := c.registry.AttachPlatformInCommit(ctx, ref, mdesc, platform); err != nil {
		return nil, fmt.Errorf("%w: ref %s: %w", ErrCommitFailed, ref, err)
	}
	target, err := c.registry.Resolve(ctx, ref)
	if err != nil {
		// 发布已成功：Resolve 失败不应发生（同一目标刚通过闸门），
		// 如实返回但不链 ErrCommitFailed，避免调用方误判为未提交。
		return nil, fmt.Errorf("commit %s: resolve after publish: %w", ref, err)
	}
	return &Result{Manifest: mdesc, Target: target}, nil
}
