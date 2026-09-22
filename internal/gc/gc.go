// Package gc 基于本地仓库的标签记录与索引/清单引用图计算存活对象，
// 回收 CAS 中不再被任何标签（传递）引用的层、配置与清单。
//
// 并发与一致性语义：
//   - 单进程内通过 Store/Registry 的锁串行化 Put/Delete 与标签更新；
//   - GC 与写入并发时采用保守回收：标记阶段之后新增引用的对象在本次
//     GC 中"宁可保留不可错删"；
//   - 仍被任意标签（经索引/清单传递）引用的层绝不会被删除；
//   - 外部直接改文件不在保证范围内。
//
// 空间口径：未被引用的历史版本对象会被移除，目录占用随"当前引用闭包"
// 而非历史写入总量无界增长。
package gc

import (
	"context"
	"errors"

	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/descriptor"
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
	Reachable  map[digest.Digest]struct{}
	Deleted    []digest.Digest
	Kept       int
	FreedBytes int64
}

var (
	// ErrNoTags 仓库无任何标签时返回（默认不清除任何对象，避免误删
	// 正在构建、尚未打标签的对象）。
	ErrNoTags = errors.New("gc: no tags exist; refusing full sweep")
)

// New 创建回收器。
func New(st *store.Store, reg *registry.Registry) *Collector {
	return &Collector{st: st, reg: reg}
}

// Run 执行一次标记-清除。
func (c *Collector) Run(ctx context.Context) (*Report, error) {
	return nil, nil
}

// Mark 返回从所有标签出发可达的摘要集合（传递闭包：
// 索引→清单→配置/层）。
func (c *Collector) Mark(ctx context.Context) (map[digest.Digest]struct{}, error) {
	return nil, nil
}

// 占位引用，避免骨架阶段 manifest 包未被使用导致编译错误；
// 实现阶段标记逻辑会实际引用它。
var _ = descriptor.MediaTypeImageIndex
