// Package gc 基于本地仓库的标签记录与索引/清单引用图计算存活对象，
// 回收 CAS 中不再被任何标签（传递）引用的层、配置与清单。
//
// 标记口径（可达即存活）：
//
//	tag record ─▶ index ─▶ manifest(允许 v0) ─▶ config(v1) ─▶ layers
//
// 图中任何对象若在 CAS 中读取失败（缺失/截断/摘要不符），标记阶段直接
// 返回该错误并中止本次回收——绝不因为"读起来像没引用"而删除对象。
//
// 并发与一致性语义：
//   - 单进程内 Store/Registry 的互斥锁串行化变更；标签文件原子写入，
//     因此不会出现半写入记录被当作根；
//   - 清除采用保守策略：删除前再次确认对象不在"最新"引用闭包中，
//     标记之后新增引用的对象宁可保留也不错删；
//   - 仓库没有任何标签时拒绝整库清除（ErrNoTags），避免删掉正在构建、
//     尚未打标签的对象；
//   - 空间占用随当前引用闭包增长，而不随历史写入总量无界增长。
package gc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/h3yow/oci-image-pipeline-go/internal/descriptor"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
	"github.com/h3yow/oci-image-pipeline-go/internal/store"
)

// Collector 执行一次标记-清除。
type Collector struct {
	st  *store.Store
	reg *registry.Registry
}

// Report 为一次 GC 的结果统计。
type Report struct {
	// Reachable 为本次最终确认存活的摘要集合。
	Reachable map[digest.Digest]struct{}
	// Deleted 为实际删除的摘要（按摘要升序）。
	Deleted []digest.Digest
	// Kept 为存活对象数。
	Kept int
	// FreedBytes 为删除对象释放的字节总数（按删除前 Stat）。
	FreedBytes int64
	// Scanned 为存储中扫描到的对象总数。
	Scanned int
}

// ErrNoTags 仓库无任何标签时拒绝清除。
var ErrNoTags = errors.New("gc: no tags exist; refusing full sweep")

// New 创建回收器。
func New(st *store.Store, reg *registry.Registry) *Collector {
	return &Collector{st: st, reg: reg}
}

// Run 执行一次标记-清除。整个标记+清除期间持有 CAS 发布排他锁，
// 保证不会出现"新标签引用了刚被判为垃圾的层"的交错。
func (c *Collector) Run(ctx context.Context) (*Report, error) {
	c.st.GCLock()
	defer c.st.GCUnlock()

	reachable, err := c.Mark(ctx)
	if err != nil {
		return nil, err
	}

	all, err := c.st.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("gc: list blobs: %w", err)
	}

	rep := &Report{Reachable: reachable, Scanned: len(all)}

	// 清除阶段：逐个候选在"删除当下"复核最新引用闭包（保守回收）。
	for _, dgst := range all {
		if _, ok := reachable[dgst]; ok {
			rep.Kept++
			continue
		}
		if c.isReferencedNow(ctx, dgst) {
			// 标记之后被新标签/新索引引用：本次保留。
			rep.Kept++
			rep.Reachable[dgst] = struct{}{}
			continue
		}
		size, err := c.st.Stat(ctx, dgst)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				continue // 已被其它回收路径移除
			}
			return nil, fmt.Errorf("gc: stat %s: %w", dgst, err)
		}
		if err := c.st.Delete(ctx, dgst); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("gc: delete %s: %w", dgst, err)
		}
		rep.Deleted = append(rep.Deleted, dgst)
		rep.FreedBytes += size
	}
	sortDigests(rep.Deleted)
	return rep, nil
}

// Mark 返回从所有标签出发可达的摘要集合（传递闭包）。
func (c *Collector) Mark(ctx context.Context) (map[digest.Digest]struct{}, error) {
	roots, err := c.reg.AllRecords(ctx)
	if err != nil {
		return nil, fmt.Errorf("gc: enumerate tags: %w", err)
	}
	if len(roots) == 0 {
		return nil, ErrNoTags
	}

	reach := map[digest.Digest]struct{}{}
	for _, rec := range roots {
		if err := c.markDescriptor(ctx, rec.Descriptor, rec.Kind, reach); err != nil {
			return nil, err
		}
	}
	return reach, nil
}

// markDescriptor 按根记录类型进入图，并把可达对象全部加入 reach。
func (c *Collector) markDescriptor(ctx context.Context, desc descriptor.Descriptor, kind registry.RecordKind, reach map[digest.Digest]struct{}) error {
	if err := desc.Validate(); err != nil {
		return fmt.Errorf("gc: invalid root descriptor: %w", err)
	}
	switch kind {
	case registry.KindIndex:
		return c.markIndex(ctx, desc, reach)
	case registry.KindManifest:
		return c.markManifestByDesc(ctx, desc, reach)
	default:
		return fmt.Errorf("gc: tag record has unknown kind %q", kind)
	}
}

func (c *Collector) markSeen(dgst digest.Digest, reach map[digest.Digest]struct{}) bool {
	if _, ok := reach[dgst]; ok {
		return true
	}
	reach[dgst] = struct{}{}
	return false
}

// readBlob 读取并完整校验一个对象，返回其字节；任何损坏都作为错误上抛。
func (c *Collector) readBlob(ctx context.Context, desc descriptor.Descriptor) ([]byte, error) {
	rc, _, err := c.st.OpenExpected(ctx, desc.Digest, desc.Size)
	if err != nil {
		return nil, fmt.Errorf("gc: open %s: %w", desc.Digest, err)
	}
	defer rc.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, rc); err != nil {
		return nil, fmt.Errorf("gc: verify %s: %w", desc.Digest, err)
	}
	if got := manifest.DigestOf(buf.Bytes()); !got.Equal(desc.Digest) {
		return nil, fmt.Errorf("gc: %w: %s recomputes to %s",
			store.ErrDigestMismatch, desc.Digest, got)
	}
	return buf.Bytes(), nil
}

func (c *Collector) markIndex(ctx context.Context, desc descriptor.Descriptor, reach map[digest.Digest]struct{}) error {
	if seen := c.markSeen(desc.Digest, reach); seen {
		return nil
	}
	raw, err := c.readBlob(ctx, desc)
	if err != nil {
		return err
	}
	idx, _, err := manifest.DecodeIndex(raw)
	if err != nil {
		return fmt.Errorf("gc: decode index %s: %w", desc.Digest, err)
	}
	for i, e := range idx.Manifests {
		if err := c.markManifestByDesc(ctx, e.Descriptor, reach); err != nil {
			return fmt.Errorf("gc: index %s entry[%d]: %w", desc.Digest, i, err)
		}
	}
	return nil
}

func (c *Collector) markManifestByDesc(ctx context.Context, desc descriptor.Descriptor, reach map[digest.Digest]struct{}) error {
	if c.markSeen(desc.Digest, reach) {
		return nil
	}
	raw, err := c.readBlob(ctx, desc)
	if err != nil {
		return err
	}
	m, err := manifest.Decode(desc.MediaType, raw)
	if err != nil {
		return fmt.Errorf("gc: decode manifest %s: %w", desc.Digest, err)
	}
	for i, l := range m.Layers {
		if c.markSeen(l.Digest, reach) {
			continue
		}
		// 层对象同样必须可读且完整：损坏的被引用层不得被当作垃圾，
		// 也不允许静默忽略——交由 readBlob 暴露错误并中止 GC。
		if _, err := c.readBlob(ctx, l); err != nil {
			return fmt.Errorf("gc: layer[%d] %s: %w", i, l.Digest, err)
		}
	}
	if m.Config != nil {
		if c.markSeen(m.Config.Digest, reach) {
			return nil
		}
		if _, err := c.readBlob(ctx, *m.Config); err != nil {
			return fmt.Errorf("gc: config %s: %w", m.Config.Digest, err)
		}
	}
	return nil
}

// isReferencedNow 重新枚举标签并重建闭包，用于删除前的并发复核。
func (c *Collector) isReferencedNow(ctx context.Context, dgst digest.Digest) bool {
	reach, err := c.Mark(ctx)
	if err != nil {
		// 无法确认时保守视为存活。
		return true
	}
	_, ok := reach[dgst]
	return ok
}

// sortDigests 按规范字符串升序，保证报告与日志可复现。
func sortDigests(d []digest.Digest) {
	for i := 1; i < len(d); i++ {
		for j := i; j > 0 && d[j-1].String() > d[j].String(); j-- {
			d[j-1], d[j] = d[j], d[j-1]
		}
	}
}
