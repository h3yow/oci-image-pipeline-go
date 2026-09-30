package cas

import (
	"context"
	"sync"

	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
)

// pinState 记录单个摘要当前被进行中的写入/读取保护的次数。
type pinState struct {
	writing int
	reading int
}

// Locker 是进程内的对象活动登记表（pin 表）。
//
// 它只协调同一进程内的写入/读取/回收；存储本身的原子性由文件系统
// 临时文件 + rename 保证。GC 只能删除既不可达、也未被 pin 的对象，
// 因此“正在写入/正在读取”的对象不会被回收。
type Locker struct {
	mu   sync.Mutex
	pins map[digest.Digest]*pinState
	// tmpPins 登记进行中写入使用的临时文件名，供 GC 识别不得清理。
	tmpPins map[string]int
	// commitGate 协调“提交事务”与“回收的扫描+删除阶段”：
	//
	//	提交事务持 RLock（彼此可并发）；一轮 GC 从计算可达闭包到完成删除
	//	持 Lock（独占）。于是 GC 删除某对象时，要么相关提交尚未发布（其
	//	RLock 令 GC 等待，对象同时还受写/读 pin 保护），要么提交已发布
	//	（对象已在最新可达闭包中）。这消除了“GC 用过期可达集删掉刚发布
	//	引用所需对象”的竞态。
	commitGate sync.RWMutex
}

// NewLocker 创建空锁表。
func NewLocker() *Locker {
	return &Locker{pins: make(map[digest.Digest]*pinState), tmpPins: make(map[string]int)}
}

func (l *Locker) state(d digest.Digest) *pinState {
	s := l.pins[d]
	if s == nil {
		s = &pinState{}
		l.pins[d] = s
	}
	return s
}

// BeginWrite 登记一次针对 d 的写入，回收期间该对象受保护。
func (l *Locker) BeginWrite(d digest.Digest) {
	l.mu.Lock()
	l.state(d).writing++
	l.mu.Unlock()
}

// EndWrite 结束一次写入登记。
func (l *Locker) EndWrite(d digest.Digest) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if s := l.pins[d]; s != nil && s.writing > 0 {
		s.writing--
	}
}

// BeginRead 登记一次针对 d 的读取，读取完成前回收不得删除该对象。
func (l *Locker) BeginRead(d digest.Digest) {
	l.mu.Lock()
	l.state(d).reading++
	l.mu.Unlock()
}

// EndRead 结束一次读取登记。
func (l *Locker) EndRead(d digest.Digest) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if s := l.pins[d]; s != nil && s.reading > 0 {
		s.reading--
	}
}

// PinTmp 预先登记一个即将使用的临时文件名（先于文件创建）。
// 返回 release：rename 提交完成或放弃后调用。这样 GC 的临时文件清理
// 在临时文件“整个生命周期”内都能观察到 pin，不存在创建前的竞争窗口。
func (l *Locker) PinTmp(name string) (release func()) {
	l.mu.Lock()
	l.tmpPins[name]++
	l.mu.Unlock()
	return func() { l.EndTmp(name) }
}

// BeginTmp 登记一个进行中写入的临时文件名。
func (l *Locker) BeginTmp(name string) {
	l.mu.Lock()
	l.tmpPins[name]++
	l.mu.Unlock()
}

// EndTmp 解除临时文件登记。
func (l *Locker) EndTmp(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.tmpPins[name] > 0 {
		l.tmpPins[name]--
		if l.tmpPins[name] == 0 {
			delete(l.tmpPins, name)
		}
	}
}

// Pinned 报告 d 当前是否存在进行中的写入或读取。
func (l *Locker) Pinned(d digest.Digest) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.pins[d]
	return s != nil && (s.writing > 0 || s.reading > 0)
}

// Pin 在不读取对象的前提下登记一个读保护（幂等：已登记则仅计数），返回
// 单次释放函数。用于提交事务在“对象尚未写入/刚写入但还不可达”时就预先
// 占位，消除“写入落地 -> 校验前”被并发回收删除的窗口。
func (l *Locker) Pin(d digest.Digest) (release func()) {
	l.mu.Lock()
	l.state(d).reading++
	l.mu.Unlock()
	return func() { l.EndRead(d) }
}

// SnapshotPinned 返回当前受保护摘要集合的快照，供一轮回收使用。
// 快照之后新开始的写入由其自身的原子落地语义保证：未发布对象本就不在
// 可达闭包中，且 GC 删除前会再次 Pinned 复核（双检）。
func (l *Locker) SnapshotPinned(_ context.Context) map[digest.Digest]struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[digest.Digest]struct{}, len(l.pins))
	for d, s := range l.pins {
		if s.writing > 0 || s.reading > 0 {
			out[d] = struct{}{}
		}
	}
	return out
}

// SnapshotTmp 返回进行中写入临时文件名集合的快照。
func (l *Locker) SnapshotTmp() map[string]struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]struct{}, len(l.tmpPins))
	for name := range l.tmpPins {
		out[name] = struct{}{}
	}
	return out
}

// BeginCommit 在一次原子提交事务开始时取读门，返回释放函数。多个提交可
// 同时持有；一轮回收在 BeginCollection/EndCollection 期间独占，从而保证
// 回收不会基于过期可达集删除刚提交引用所需的对象。
func (l *Locker) BeginCommit() (release func()) {
	l.commitGate.RLock()
	return func() { l.commitGate.RUnlock() }
}

// BeginCollection 在一轮回收“计算可达闭包 -> 完成删除”期间取独占门，
// 返回释放函数。期间不会有提交事务在运行（新的提交会阻塞等待）。
func (l *Locker) BeginCollection() (release func()) {
	l.commitGate.Lock()
	return func() { l.commitGate.Unlock() }
}
