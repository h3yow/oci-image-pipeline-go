// Package gc 实现内容寻址存储的垃圾回收：删除任何引用均不可达的对象。
//
// 语义：
//   - 回收前先快照：全部引用的可达闭包 + 当前进行中读/写（pin）的对象；
//   - 只删除“不可达且未被 pin”的对象；删除前对 pin 双检，杜绝与并发
//     写/读竞争导致的误删，正在写入的对象始终不会以半状态被看到；
//   - 写 pin 在单次 Put 期间保护对象；对象提交后由“已发布的引用”提供
//     长期保护。把“组装并挂引用”作为一次事务的入口
//     （registry.CommitImagePlan/CommitPlatformPlan）会在写任何对象前
//     先 pin 整个新闭包、持有到引用原子发布，因而与本回收器并发也没有
//     “写好了但还没引用”的删除窗口；
//   - 已写入但始终未被任何引用指向的对象（含失败提交的残留）被回收属于
//     正确行为，下一轮引用发布后其依赖自然稳定；
//   - 长期运行下存储占用只随“当前可达对象集”变化，不随历史写入总量
//     无界膨胀；
//   - 同时清理 tmp/ 中不属于任何进行中写入的临时文件。
package gc

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
)

// Collector 垃圾回收器。
type Collector struct {
	store    *cas.Store
	registry *registry.Registry
}

// New 创建回收器。
func New(store *cas.Store, reg *registry.Registry) *Collector {
	return &Collector{store: store, registry: reg}
}

// Result 回收判定明细（可解释、可复现）。
type Result struct {
	Visited  []string `json:"visited"`
	Kept     []string `json:"kept"`
	Deleted  []string `json:"deleted"`
	StaleTmp []string `json:"staleTmp"`
}

// Run 执行一轮回收。
//
// 判定依据对每一个对象均可解释：
//   - 在任一引用的可达闭包中 -> Kept(referenced)；
//   - 被进行中读/写 pin 保护 -> Kept(pinned)；
//   - 两者皆无 -> Deleted。
func (c *Collector) Run(ctx context.Context) (*Result, error) {
	res := &Result{}

	// 独占回收门：从“计算可达闭包”到“完成删除”期间不允许提交事务在跑。
	// 这样本轮到的可达集不会因为中途有引用发布而过期，杜绝“用旧闭包删除
	// 新引用对象”；提交彼此仍可并发，只在回收的这一阶段等待。
	releaseGate := c.store.Lock().BeginCollection()

	// 1) 引用可达闭包。
	refs, err := c.registry.AllRefs(ctx)
	if err != nil {
		releaseGate()
		return nil, fmt.Errorf("gc: list refs: %w", err)
	}
	reachable := make(map[digest.Digest]struct{})
	for _, ref := range refs {
		blobs, err := c.registry.ReferencedBlobs(ctx, ref)
		if err != nil {
			// 某引用损坏/指向缺失对象：中止本轮，绝不因此删除其（可能暂时
			// 不可解析的）依赖，避免误伤。错误原因交由调用方区分处理。
			releaseGate()
			return nil, fmt.Errorf("gc: resolve %s: %w", ref, err)
		}
		for d := range blobs {
			reachable[d] = struct{}{}
		}
	}

	// 2) 扫描全部对象。
	all, err := c.store.List(ctx)
	if err != nil {
		releaseGate()
		return nil, fmt.Errorf("gc: list objects: %w", err)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	// 3) 取进行中读/写的 pin 快照，并对每个对象删除前再双检一次。
	pinned := c.store.Lock().SnapshotPinned(ctx)
	for _, d := range all {
		res.Visited = append(res.Visited, string(d))
		if _, ok := reachable[d]; ok {
			res.Kept = append(res.Kept, string(d))
			continue
		}
		// 双检：快照之后可能刚开始的写/读，这里以当前 pin 状态复核。
		if _, snapPinned := pinned[d]; snapPinned || c.store.Lock().Pinned(d) {
			res.Kept = append(res.Kept, string(d))
			continue
		}
		if err := c.store.Delete(ctx, d); err != nil &&
			!errors.Is(err, cas.ErrNotFound) {
			// ErrNotFound=另一轮并发回收已删除，属自然消失；其他错误才中止。
			releaseGate()
			return nil, fmt.Errorf("gc: delete %s: %w", d.Short(), err)
		}
		res.Deleted = append(res.Deleted, string(d))
	}
	// 删除阶段结束即可放行提交；下面的临时文件清理只依赖 tmp pin 集合，
	// 与引用发布无关。
	releaseGate()

	// 4) 清理无主临时文件。
	stale, err := c.cleanStaleTmp(ctx)
	if err != nil {
		return nil, err
	}
	res.StaleTmp = stale

	sort.Strings(res.Kept)
	sort.Strings(res.Deleted)
	sort.Strings(res.StaleTmp)
	return res, nil
}

// cleanStaleTmp 删除 tmp/ 中不属于任何进行中写入的临时文件。
//
// 正确性依赖 cas 的不变量“先 pin 临时文件名，再创建文件”：
//  1. 先 ReadDir 得到候选集合（此时存在的文件，其 pin 必已登记）；
//  2. 再对 pin 集合快照；
//  3. 删除快照中无 pin 的候选。
//
// 因此：列表之后才创建的文件根本不在候选中；列表中的活跃文件其 pin
// 必然已可见。整个判定不依赖文件 mtime（部分环境下 mtime 可能与
// 实际创建时间不一致）。
func (c *Collector) cleanStaleTmp(_ context.Context) ([]string, error) {
	tmpDir := filepath.Join(c.store.Root(), "tmp")
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("gc: read tmp: %w", err)
	}
	// pin 快照必须在 ReadDir 之后取：见上述不变量。
	live := c.store.Lock().SnapshotTmp()

	var removed []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if _, active := live[e.Name()]; active {
			continue // 属于进行中写入，保留
		}
		p := filepath.Join(tmpDir, e.Name())
		if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("gc: remove tmp %s: %w", e.Name(), err)
		}
		removed = append(removed, e.Name())
	}
	sort.Strings(removed)
	return removed, nil
}
